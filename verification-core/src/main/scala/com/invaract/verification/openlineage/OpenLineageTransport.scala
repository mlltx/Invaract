// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.verification.openlineage

import org.slf4j.LoggerFactory

import java.io.ByteArrayOutputStream
import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.nio.channels.FileChannel
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths, StandardOpenOption}
import java.time.Duration
import java.util.zip.GZIPOutputStream

/** What became of one attempt to deliver an event. */
private[openlineage] sealed trait Outcome
private[openlineage] object Outcome {
  case object Delivered extends Outcome
  /** Another try could succeed: a connection failure, a timeout, `408`, `429`, a `5xx`. */
  final case class Retryable(message: String) extends Outcome
  /** The receiver refused this event (`400`, `401`, `404`, ...): trying again would be refused again. */
  final case class Rejected(message: String) extends Outcome
}

/** Delivers one rendered event. Called from one thread only (the sink's worker); never throws. */
private[openlineage] trait Transport {
  def send(json: String): Outcome
  def describe: String
}

private[openlineage] object Transport {
  def apply(settings: TransportSettings): Transport = settings match {
    case h: TransportSettings.Http => new HttpTransport(h)
    case TransportSettings.File(path) => new FileTransport(path)
    case TransportSettings.Console => ConsoleTransport
  }

  def isRetryableStatus(status: Int): Boolean = status == 408 || status == 429 || status / 100 == 5
}

/** POSTs each event to `url/endpoint`, as OpenLineage's HTTP transport does, with the API key as a bearer token and, if
  * asked, the body gzipped. Synchronous: the sink's worker thread waits, the job never does. */
private[openlineage] final class HttpTransport(settings: TransportSettings.Http) extends Transport {
  private val target = URI.create(s"${settings.url}/${settings.endpoint}")
  private val timeout = Duration.ofMillis(settings.timeoutMs)
  private def client = HttpTransport.client
  private val headers: Seq[(String, String)] =
    Seq("Content-Type" -> "application/json") ++
      (if (settings.gzip) Seq("Content-Encoding" -> "gzip") else Nil) ++
      settings.apiKey.map(key => "Authorization" -> s"Bearer $key").toSeq ++
      settings.headers.toSeq

  val describe: String = OpenLineageSettings.redact(target.toString)

  def send(json: String): Outcome = {
    val body = json.getBytes(StandardCharsets.UTF_8)
    val request = headers
      .foldLeft(HttpRequest.newBuilder(target).timeout(timeout)) { case (b, (k, v)) => b.header(k, v) }
      .POST(HttpRequest.BodyPublishers.ofByteArray(if (settings.gzip) HttpTransport.gzip(body) else body))
      .build()
    try {
      val status = client.send(request, HttpResponse.BodyHandlers.discarding()).statusCode()
      if (status / 100 == 2) Outcome.Delivered
      else if (Transport.isRetryableStatus(status)) Outcome.Retryable(s"$describe answered $status")
      else Outcome.Rejected(s"$describe refused the event with $status")
    } catch {
      case e: InterruptedException =>
        Thread.currentThread().interrupt()
        Outcome.Retryable(s"$describe: interrupted")
      case e: Exception => Outcome.Retryable(s"$describe could not be reached: ${e.getClass.getSimpleName}${Option(e.getMessage).fold("")(m => s": $m")}")
    }
  }
}

private[openlineage] object HttpTransport {

  /** One client for every HTTP transport in the process: a client keeps threads of its own, and a process that builds sinks
    * for several settings (a test suite, a driver building many sessions) should not keep a set per sink. Redirects are never
    * followed (an event is not re-sent somewhere a response names); each request carries its own `timeoutMs`, which bounds
    * connecting too. */
  lazy val client: HttpClient = HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NEVER).build()

  def gzip(bytes: Array[Byte]): Array[Byte] = {
    val out = new ByteArrayOutputStream(bytes.length / 4 + 64)
    val zip = new GZIPOutputStream(out)
    try zip.write(bytes) finally zip.close()
    out.toByteArray
  }
}

/** Appends one event per line. Each line is written whole, under an exclusive lock on the file, so several processes
  * publishing to one file (a job and the processes it starts) interleave whole lines, never parts of them. */
private[openlineage] final class FileTransport(path: String) extends Transport {
  private val file = Paths.get(path)
  val describe: String = s"file $path"

  def send(json: String): Outcome =
    try {
      Option(file.toAbsolutePath.getParent).foreach(p => Files.createDirectories(p))
      // A file lock is the process's: two sinks of one JVM on one file (a Spark job with several sessions) take turns
      // here first, or the second would fail with OverlappingFileLockException.
      FileTransport.monitorFor(file.toAbsolutePath.normalize.toString).synchronized {
      val channel = FileChannel.open(file, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND)
      try {
        val lock = channel.lock()
        try {
          val bytes = java.nio.ByteBuffer.wrap((json + "\n").getBytes(StandardCharsets.UTF_8))
          while (bytes.hasRemaining) channel.write(bytes)
        } finally lock.release()
      } finally channel.close()
      }
      Outcome.Delivered
    } catch {
      case e: Exception => Outcome.Retryable(s"$describe could not be written: $e")
    }
}

private[openlineage] object FileTransport {
  private val monitors = new java.util.concurrent.ConcurrentHashMap[String, Object]()
  def monitorFor(path: String): Object = monitors.computeIfAbsent(path, _ => new Object)
}

/** Logs each event at INFO, one line each: for a platform that already ships its logs. */
private[openlineage] object ConsoleTransport extends Transport {
  private val logger = LoggerFactory.getLogger("com.invaract.openlineage")
  val describe = "the log"
  def send(json: String): Outcome = { logger.info(json); Outcome.Delivered }
}
