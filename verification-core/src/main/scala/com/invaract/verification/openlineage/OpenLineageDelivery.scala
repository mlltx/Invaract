// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.verification.openlineage

import com.invaract.verification.notification.{JsonText, NotificationEvent}
import com.invaract.verification.notification.JsonText.Obj

import org.slf4j.LoggerFactory

import java.nio.charset.StandardCharsets
import java.util.concurrent.{ArrayBlockingQueue, CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import scala.util.control.NonFatal

/** Counts of what became of events, for logs and tests. */
final case class DeliveryStats(accepted: Long, delivered: Long, droppedQueueFull: Long, failed: Long, skippedCircuitOpen: Long, shed: Long, tooLarge: Long)

/** Delivers OpenLineage events off the job's thread (docs/OPENLINEAGE.md, sections 5 and 6).
  *
  * `submit` is all the job's thread does: one `offer` to a bounded queue, never blocking. One daemon thread takes each
  * Invaract event from the queue, maps it (`OpenLineageMapper`), renders it, and hands it to the `transport`, retrying a
  * failure another try could fix with capped exponential backoff. After `circuitFailures` consecutive events that could
  * not be delivered, delivery stops being attempted for `circuitCooldownMs` (the circuit is open), so a down endpoint costs
  * neither the job nor the worker a timeout per event; then one event is tried again. Whatever is given up on goes to the
  * dead letter, when there is one. An event over `maxEventBytes` sheds its column lineage, then its schemas, before it is
  * dropped. A full queue drops the newest event. Every drop is counted, and said at WARN at most once a minute.
  *
  * `close` (also run by a JVM shutdown hook) ends every open run with its terminal event and delivers what is queued,
  * waiting at most the given time.
  */
private[openlineage] final class OpenLineageDelivery(
    settings: OpenLineageSettings,
    transport: Transport,
    mapper: OpenLineageMapper,
    deadLetter: Option[Transport],
    sleep: Long => Unit = null,
    nanoTime: () => Long = () => System.nanoTime()
) {
  import OpenLineageDelivery._

  private val logger = LoggerFactory.getLogger(classOf[OpenLineageDelivery])
  private val queue = new ArrayBlockingQueue[Item](settings.queueCapacity)
  private val closed = new AtomicBoolean(false)
  /** Released by `close`: a backoff waits on it, so a job that is ending is not held for a retry's wait. */
  private val closing = new CountDownLatch(1)
  private val pause: Long => Unit =
    if (sleep != null) sleep else ms => try closing.await(ms, TimeUnit.MILLISECONDS) catch { case _: InterruptedException => Thread.currentThread().interrupt() }

  private val accepted = new AtomicLong
  private val delivered = new AtomicLong
  private val droppedQueueFull = new AtomicLong
  private val failed = new AtomicLong
  private val skippedCircuitOpen = new AtomicLong
  private val shed = new AtomicLong
  private val tooLarge = new AtomicLong

  // touched by the worker only
  private var consecutiveFailures = 0
  private var openUntilNanos = 0L
  private var lastWarnNanos = Long.MinValue / 2
  private var warnedCounts = DeliveryStats(0, 0, 0, 0, 0, 0, 0)

  private val worker = new Thread(() => run(), "invaract-openlineage")
  worker.setDaemon(true)
  worker.start()

  /** Hands `event` to the worker; `false` when it was dropped (the queue is full, or delivery has closed). */
  def submit(event: NotificationEvent): Boolean =
    if (closed.get()) false
    else if (queue.offer(Publish(event))) { accepted.incrementAndGet(); true }
    else { droppedQueueFull.incrementAndGet(); false }

  /** Waits, at most `timeoutMs`, until everything submitted before it has been delivered or given up on. */
  def flush(timeoutMs: Long): Boolean = await(new Flush(new CountDownLatch(1)), timeoutMs)

  /** Ends every open run, delivers what is queued, and stops the worker; at most `timeoutMs`. Idempotent. */
  def close(timeoutMs: Long): Boolean =
    if (!closed.compareAndSet(false, true)) true
    else {
      closing.countDown()
      val done = await(new Close(new CountDownLatch(1)), timeoutMs)
      if (!done) logger.warn(s"Invaract OpenLineage: ${queue.size} event(s) were still queued for ${transport.describe} when the job ended; they are not published")
      logSummary(force = true)
      done
    }

  def stats: DeliveryStats =
    DeliveryStats(accepted.get, delivered.get, droppedQueueFull.get, failed.get, skippedCircuitOpen.get, shed.get, tooLarge.get)

  private def await(marker: Marker, timeoutMs: Long): Boolean = {
    val deadline = nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs)
    // a marker is never dropped: if the queue is full, wait for room within the time allowed
    val queued = try queue.offer(marker, timeoutMs, TimeUnit.MILLISECONDS) catch { case _: InterruptedException => Thread.currentThread().interrupt(); false }
    queued && {
      val left = math.max(0L, deadline - nanoTime())
      try marker.latch.await(left, TimeUnit.NANOSECONDS) catch { case _: InterruptedException => Thread.currentThread().interrupt(); false }
    }
  }

  // --- the worker -------------------------------------------------------------------------------------------------

  private def run(): Unit = {
    var running = true
    while (running) {
      try {
        queue.take() match {
          case Publish(event) => mapper.map(event).foreach(deliver)
          case f: Flush       => f.latch.countDown()
          case c: Close =>
            mapper.closeAll().foreach(deliver)
            var next = queue.poll()
            while (next != null) {
              next match {
                case Publish(event) => mapper.map(event).foreach(deliver)
                case m: Marker      => m.latch.countDown()
              }
              next = queue.poll()
            }
            c.latch.countDown()
            running = false
        }
      } catch {
        case _: InterruptedException => running = false
        case NonFatal(e) => logger.warn(s"Invaract OpenLineage: an event could not be published ($e); continuing", e)
      }
      logSummary(force = false)
    }
  }

  private def deliver(event: Obj): Unit =
    render(event).foreach { json =>
      if (nanoTime() < openUntilNanos) {
        skippedCircuitOpen.incrementAndGet()
        toDeadLetter(json)
      } else {
        attempt(json, 1) match {
          case Outcome.Delivered =>
            delivered.incrementAndGet()
            consecutiveFailures = 0
          case Outcome.Rejected(message) =>
            failed.incrementAndGet()
            consecutiveFailures = 0 // the endpoint answers; this event is the problem
            warnNow(s"Invaract OpenLineage: $message; this event is not published")
            toDeadLetter(json)
          case Outcome.Retryable(message) =>
            failed.incrementAndGet()
            consecutiveFailures += 1
            if (consecutiveFailures >= settings.circuitFailures) {
              openUntilNanos = nanoTime() + TimeUnit.MILLISECONDS.toNanos(settings.circuitCooldownMs)
              consecutiveFailures = 0
              warnNow(s"Invaract OpenLineage: $message, ${settings.circuitFailures} event(s) in a row; publishing pauses for ${settings.circuitCooldownMs} ms")
            }
            toDeadLetter(json)
        }
      }
    }

  private def attempt(json: String, n: Int): Outcome =
    transport.send(json) match {
      // once closing, a failure is not retried: the job is ending, and what is not delivered goes to the dead letter
      case _: Outcome.Retryable if n < settings.retryMaxAttempts && !closed.get() =>
        pause(backoffMs(n))
        attempt(json, n + 1)
      case outcome => outcome
    }

  private[openlineage] def backoffMs(attempt: Int): Long = {
    val exponential = settings.retryInitialBackoffMs * math.pow(2, (attempt - 1).toDouble)
    math.min(settings.retryMaxBackoffMs.toDouble, exponential).toLong
  }

  /** The event's JSON, within `maxEventBytes`: shedding column lineage, then schemas, if it is too large; `None` if even
    * that is too large. */
  private def render(event: Obj): Option[String] = {
    val full = JsonText.render(event)
    if (fits(full)) Some(full)
    else {
      val withoutLineage = JsonText.render(without(event, Set("columnLineage")))
      if (fits(withoutLineage)) { shed.incrementAndGet(); Some(withoutLineage) }
      else {
        val bare = JsonText.render(without(event, Set("columnLineage", "schema")))
        if (fits(bare)) { shed.incrementAndGet(); Some(bare) }
        else {
          tooLarge.incrementAndGet()
          warnNow(s"Invaract OpenLineage: an event of ${bare.getBytes(StandardCharsets.UTF_8).length} bytes is over maxEventBytes (${settings.maxEventBytes}) even without its column lineage and schemas; it is not published")
          None
        }
      }
    }
  }

  private def fits(json: String): Boolean =
    // a UTF-16 char is at most 3 bytes of UTF-8 (a surrogate pair, two chars, is 4): short text needs no encoding to tell
    json.length <= settings.maxEventBytes / 3 || json.getBytes(StandardCharsets.UTF_8).length <= settings.maxEventBytes

  private def toDeadLetter(json: String): Unit =
    deadLetter.foreach { d =>
      d.send(json) match {
        case Outcome.Delivered => ()
        case other             => warnNow(s"Invaract OpenLineage: the dead letter ${d.describe} could not keep an event: $other")
      }
    }

  /** A WARN said now, whatever the rate (something an operator must see once). */
  private def warnNow(message: String): Unit = logger.warn(message)

  /** What was dropped or given up on since the last summary, said at WARN at most once a minute (or now, when forced). */
  private def logSummary(force: Boolean): Unit = {
    val now = nanoTime()
    if (force || now - lastWarnNanos >= SummaryIntervalNanos) {
      val s = stats
      val newDrops = s.droppedQueueFull - warnedCounts.droppedQueueFull
      val newFailures = s.failed - warnedCounts.failed
      val newSkips = s.skippedCircuitOpen - warnedCounts.skippedCircuitOpen
      if (newDrops + newFailures + newSkips > 0) {
        logger.warn(
          s"Invaract OpenLineage (${transport.describe}): $newDrops event(s) dropped because the queue (${settings.queueCapacity}) was full, " +
            s"$newFailures could not be delivered, $newSkips skipped while publishing was paused" +
            (if (deadLetter.isDefined) "; those not delivered are in the dead letter" else "")
        )
        warnedCounts = s
        lastWarnNanos = now
      }
    }
  }
}

private[openlineage] object OpenLineageDelivery {
  val SummaryIntervalNanos: Long = TimeUnit.MINUTES.toNanos(1)

  sealed trait Item
  final case class Publish(event: NotificationEvent) extends Item
  sealed abstract class Marker(val latch: CountDownLatch) extends Item
  final class Flush(latch: CountDownLatch) extends Marker(latch)
  final class Close(latch: CountDownLatch) extends Marker(latch)

  /** `event` with the dataset facets named in `facets` removed from every input and output. */
  def without(event: Obj, facets: Set[String]): Obj = Obj(event.entries.map {
    case (k @ ("inputs" | "outputs"), datasets: List[_]) =>
      k -> datasets.map {
        case d: Obj => Obj(d.entries.map {
          case ("facets", f: Obj) => "facets" -> Obj(f.entries.filterNot { case (name, _) => facets.contains(name) })
          case other              => other
        })
        case other => other
      }
    case other => other
  })
}
