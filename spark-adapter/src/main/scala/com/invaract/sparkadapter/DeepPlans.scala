// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

/** Running Invaract's work on a plan so that a very deep plan never crashes the job.
  *
  * The check rule translates every plan Spark analyzes, not just writes (it has to, to remember what a later
  * `.checkpoint()` was made from). Translation recurses once per plan node, with larger frames than Spark's own
  * analyzer, so a plan Spark handles - a loop of a thousand `withColumn` calls before a `count()` - could exhaust the
  * thread's stack inside Invaract and fail a query that writes nothing. `attempt` reports that instead of throwing it;
  * `onLargeStack` runs the same work again on a thread whose stack is large enough for any plan Spark itself can
  * analyze. `ContractEnforcementRule` uses the two in that order and decides what an overflow on both means.
  */
private[sparkadapter] object DeepPlans {

  /** The stack of the retry thread. Reserved address space, not memory: the JVM commits only what is used. */
  val LargeStackBytes: Long = 512L * 1024 * 1024

  /** `body` on the calling thread; `Left` if it ran out of stack. Anything else it throws is thrown. */
  def attempt[A](body: => A): Either[StackOverflowError, A] =
    try Right(body)
    catch { case e: StackOverflowError => Left(e) }

  /** `body` on a new thread with a `LargeStackBytes` stack, waited for; `Left` if even that ran out. Anything else
    * `body` throws (a `ContractViolationException` above all) is rethrown on the calling thread as itself, unwrapped.
    *
    * The new thread inherits the caller's active `SparkSession` (Spark keeps it in an inheritable thread-local), so
    * `SparkSession.active` and `SQLConf.get` mean the same there as on the analyzer thread that called this.
    */
  def onLargeStack[A](body: => A): Either[StackOverflowError, A] = {
    @volatile var outcome: Option[Either[StackOverflowError, A]] = None
    @volatile var thrown: Option[Throwable] = None
    val worker = new Thread(
      null,
      () =>
        try outcome = Some(attempt(body))
        catch { case e: Throwable => thrown = Some(e) },
      "invaract-deep-plan",
      LargeStackBytes
    )
    worker.start()
    worker.join()
    thrown.foreach(e => throw e)
    outcome.get
  }
}
