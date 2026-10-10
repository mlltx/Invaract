// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.testkit

import com.invaract.verification.notification.{ContractValidationEvent, NotificationEvent, NotificationSink}

import scala.collection.mutable

/** A `NotificationSink` that remembers the validation events it is given - what an adapter hands to its
  * engine's enforcement so the `ScenarioOutcome` can be filled in from what enforcement actually published.
  * An adapter finishes a run with `passed` or `rejected(...)`, and never builds an outcome by hand.
  */
final class RecordingSink extends NotificationSink {
  private val seen = mutable.ListBuffer.empty[NotificationEvent]

  override def publish(event: NotificationEvent): Unit = synchronized(seen += event)

  private def validations: List[ContractValidationEvent] = synchronized(seen.toList).collect { case e: ContractValidationEvent => e }

  def statuses: List[String] = validations.map(_.status)

  /** The output columns the last validation's fingerprint reports non-deterministic. */
  def nonDeterministicOutputs: Set[String] =
    validations.lastOption
      .flatMap(_.fingerprints)
      .map(_.outputs.collect { case (name, fp) if fp.nonDeterministic.contains(true) => name }.toSet)
      .getOrElse(Set.empty)

  /** `(field, verdict)` for each static data-quality result of the last validation. */
  def dataQuality: Set[(String, String)] =
    validations.lastOption.map(_.dataQuality.map(r => r.field -> r.verdict.toString).toSet).getOrElse(Set.empty)

  /** Input name -> role-consistency verdict of the last validation. */
  def roles: Map[String, String] =
    validations.lastOption.map(_.roleConformance.map(r => r.dataset -> r.verdict.toString).toMap).getOrElse(Map.empty)

  /** The declared inputs the last validation could not confirm were read. */
  def unverifiableInputs: Set[String] =
    validations.lastOption.map(_.unverifiableInputs.map(_.inputName).toSet).getOrElse(Set.empty)

  /** The engine each validation event named in `job.engine`, in order (`None` for an event that named none). */
  def eventEngines: List[Option[String]] = validations.map(_.job.flatMap(_.engine))

  /** The outcome of a write the engine allowed. */
  def passed: ScenarioOutcome.Passed =
    ScenarioOutcome.Passed(statuses, nonDeterministicOutputs, dataQuality, roles, unverifiableInputs, eventEngines)

  /** The outcome of a write the engine blocked with these violation types. */
  def rejected(violationTypes: Set[String]): ScenarioOutcome.Rejected =
    ScenarioOutcome.Rejected(violationTypes, statuses, nonDeterministicOutputs, dataQuality, roles, unverifiableInputs, eventEngines)
}
