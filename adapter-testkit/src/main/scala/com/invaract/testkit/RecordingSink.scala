// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.testkit

import com.invaract.verification.notification.{ContractValidationEvent, NotificationEvent, NotificationSink}

import scala.collection.mutable

/** A `NotificationSink` that remembers the validation statuses it is given - what an adapter hands
  * to its engine's enforcement so `ScenarioOutcome.statuses` can be filled in.
  */
final class RecordingSink extends NotificationSink {
  private val seen = mutable.ListBuffer.empty[NotificationEvent]

  override def publish(event: NotificationEvent): Unit = synchronized(seen += event)

  def statuses: List[String] = synchronized(seen.toList).collect { case e: ContractValidationEvent => e.status }

  /** The output columns the last validation's fingerprint reports non-deterministic. */
  def nonDeterministicOutputs: Set[String] =
    synchronized(seen.toList).collect { case e: ContractValidationEvent => e }.lastOption
      .flatMap(_.fingerprints)
      .map(_.outputs.collect { case (name, fp) if fp.nonDeterministic.contains(true) => name }.toSet)
      .getOrElse(Set.empty)
}
