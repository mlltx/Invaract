// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.testkit

import com.invaract.contract.{Contract, LogicalSchema, LogicalType}
import com.invaract.verification.{Capability, VerificationOptions}

/** A dataset a scenario's job reads. Only its location and schema matter - an adapter materializes
  * it as an empty dataset of that schema, because every check the engine makes is about shapes
  * (locations, columns, types, which plan nodes exist), never about values.
  */
final case class ScenarioInput(location: String, schema: LogicalSchema)

/** Where a scenario's job writes. `format` and `saveMode` use the engine-neutral spellings the
  * contract format uses (`parquet`, `csv`, `json`; `append`, `overwrite`, `ignore`, `error`).
  * With `registeredAs` set the data is also registered in the engine's catalog under that table
  * name (still stored at `location`), which is what a contract's `catalog:` requirement asks about.
  */
final case class ScenarioOutput(location: String, format: String = "parquet", saveMode: String = "overwrite", registeredAs: Option[String] = None)

/** What one output column is made of. Deliberately tiny: just enough to build every shape the
  * scenarios need on any engine - a pass-through, a cast, and a value that can never be null.
  */
sealed trait ColumnSource

object ColumnSource {

  /** Column `column` of input number `input` (0-based), unchanged - nullable if the input's is. */
  final case class FromInput(input: Int, column: String) extends ColumnSource

  /** The same column cast to `to`. */
  final case class CastInput(input: Int, column: String, to: LogicalType) extends ColumnSource

  /** A value the engine generates afresh for every row (a random unique id) - typed `string`, never null. A
    * conforming adapter writes it with its own engine's function (`uuid()`, `GENERATE_UUID()`); what the
    * scenarios check is that the engine's name for it is recognized as non-deterministic.
    */
  case object UniqueId extends ColumnSource

  /** A constant integer, typed `long` and never null (the way to get a guaranteed non-null column). */
  final case class NonNullLong(value: Long) extends ColumnSource
}

final case class OutColumn(name: String, source: ColumnSource)

/** Join the two inputs on equality of one column from each. */
final case class JoinOn(leftColumn: String, rightColumn: String)

/** A row-level operation on the output dataset in place - not a read-transform-write. Only the
  * shapes the scenarios need: a delete that touches every row, and one that touches the rows
  * matching a predicate (`id > 0`).
  */
sealed trait RowChange

object RowChange {

  /** `DELETE FROM output` with no `WHERE`. */
  case object UnconditionalDelete extends RowChange

  /** `DELETE FROM output WHERE id > 0`. */
  case object FilteredDelete extends RowChange
}

/** A job described in no engine's terms: read one or two inputs (two are inner-joined), optionally
  * keep the rows where a column of the first input is greater than zero, project `columns`, write
  * `output`. Each adapter turns this into its own engine's real job and runs it under enforcement.
  *
  * With `untranslatableWrite` set the job is not that read-transform-write at all: it is a
  * data-changing operation on `output` that looks like a write but that the adapter has no
  * translation for, and an adapter must refuse it (`UNVERIFIABLE_WRITE`) rather than let it through
  * unchecked. The adapter picks its own engine's representative (Spark: `TRUNCATE TABLE`); the
  * inputs and columns are not used.
  *
  * With `rowChange` set the job changes the rows of `output` in place instead, and `columns` describe
  * that dataset's own columns: the adapter first creates it (in `output.format`, with no contract
  * active), then runs the change under enforcement. The change reads no declared input, so a
  * scenario using it declares none.
  *
  * With `boundary` set the work so far (the read, the join, the filter) is materialized through the
  * engine's own lineage-erasing point - a checkpoint or a cache - before the projection and the write.
  * What the contract and its rules say about the job must come out the same as without it: an adapter
  * that cannot see through the boundary has lost the reads and the filter that sit behind it.
  *
  * With `streaming` set the inputs are read as streams and the output is written as a stream (run to
  * completion over the inputs that exist, then stopped), one input only. What the contract says about the job
  * must come out the same as for the batch job: the stream is the declared input, the stream's sink the output.
  */
final case class ScenarioJob(
    inputs: List[ScenarioInput],
    columns: List[OutColumn],
    output: ScenarioOutput,
    join: Option[JoinOn] = None,
    filterColumn: Option[String] = None,
    untranslatableWrite: Boolean = false,
    rowChange: Option[RowChange] = None,
    boundary: Boolean = false,
    streaming: Boolean = false
) {
  require(inputs.nonEmpty && inputs.size <= 2, "a scenario job reads one or two inputs")
  require(inputs.size == 1 || join.isDefined, "a two-input job must say how its inputs are joined")
  require(inputs.size == 2 || join.isEmpty, "a join needs two inputs")
  require(!streaming || (inputs.size == 1 && rowChange.isEmpty && !untranslatableWrite && !boundary), "a streaming job is a plain read-transform-write of one input")
}

/** What running a scenario through an adapter produced. `statuses` are the `ContractValidationEvent`
  * statuses the adapter published to the sink it was given, in order; the rest is read from the last of
  * those events, so an adapter fills it in by handing `RecordingSink` to its enforcement and asking the
  * sink (`RecordingSink.passed` / `rejected`).
  */
sealed trait ScenarioOutcome {
  def statuses: List[String]

  /** The output columns the adapter's fingerprint reported non-deterministic (empty when it computed none). */
  def nonDeterministicColumns: Set[String]

  /** `(output field, verdict)` for every static data-quality check reported (`DataQualityVerdict` names). */
  def dataQuality: Set[(String, String)]

  /** Input name -> role-consistency verdict (`RoleConformanceVerdict` names) for every input checked. */
  def roles: Map[String, String]

  /** The declared inputs the adapter could not confirm were read, because a lineage boundary hid them. */
  def unverifiableInputs: Set[String]
}

object ScenarioOutcome {

  /** The write was allowed to proceed. */
  final case class Passed(
      statuses: List[String],
      nonDeterministicColumns: Set[String] = Set.empty,
      dataQuality: Set[(String, String)] = Set.empty,
      roles: Map[String, String] = Map.empty,
      unverifiableInputs: Set[String] = Set.empty
  ) extends ScenarioOutcome

  /** The write was blocked; `violationTypes` are the distinct `ViolationType`s reported. */
  final case class Rejected(
      violationTypes: Set[String],
      statuses: List[String],
      nonDeterministicColumns: Set[String] = Set.empty,
      dataQuality: Set[(String, String)] = Set.empty,
      roles: Map[String, String] = Map.empty,
      unverifiableInputs: Set[String] = Set.empty
  ) extends ScenarioOutcome
}

/** What a conforming adapter must do with a scenario. */
sealed trait Expectation

object Expectation {
  case object Pass extends Expectation

  /** Rejected, with exactly this set of violation types. */
  final case class Reject(violationTypes: Set[String]) extends Expectation
}

/** What a scenario expects an adapter to report beyond allow or block. Each part is checked only when set;
  * `None` means the scenario says nothing about it.
  *
  * @param dataQuality exactly these `(field, verdict)` static data-quality results (the scenario turns
  *   `staticDataQuality` on).
  * @param roles exactly these input -> role-consistency verdicts (the scenario turns `roleConsistency` on).
  * @param unverifiableInputs exactly these declared inputs reported as hidden behind a boundary.
  * @param lineage for each output column, the `(input number, column)` pairs it derives from, read from
  *   the adapter's own translation of the job (`ConformanceAdapter.translation`).
  * @param sensitivity for each output column, the sensitivity tags that reach it from the contract's tagged
  *   input fields, propagated over the adapter's own translation.
  */
final case class Analysis(
    dataQuality: Option[Set[(String, String)]] = None,
    roles: Option[Map[String, String]] = None,
    unverifiableInputs: Option[Set[String]] = None,
    lineage: Option[Map[String, Set[(Int, String)]]] = None,
    sensitivity: Option[Map[String, Set[String]]] = None
) {

  /** True when checking this needs the adapter's translated plan, not just what a run reported. */
  def needsTranslation: Boolean = lineage.isDefined || sensitivity.isDefined
}

/** One engine-neutral behaviour every adapter must show.
  *
  * @param focus the capabilities this scenario is the evidence for: an adapter that declares one of
  *   these `supported` or `partial` is claiming this scenario passes on it.
  * @param expectNonDeterministic when set, exactly these output columns must be reported non-deterministic by the
  *   adapter's fingerprint (the scenario turns `computeFingerprint` on) - the check that an engine's own
  *   function names reach the shared catalog.
  * @param analysis what the scenario expects the adapter to report beyond allow or block (see `Analysis`).
  * @param operations what the job itself needs the engine to do (read, write) - an adapter that
  *   cannot do those cannot run the scenario at all.
  */
final case class Scenario(
    id: String,
    description: String,
    focus: Set[Capability],
    contract: Contract,
    job: ScenarioJob,
    options: VerificationOptions,
    expect: Expectation,
    expectNonDeterministic: Option[Set[String]] = None,
    analysis: Analysis = Analysis(),
    operations: Set[Capability] = Set(Capability.ReadBatch, Capability.WriteBatch)
)
