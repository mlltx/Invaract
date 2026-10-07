// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.verification

import com.invaract.contract.{Contract, Dataset, LogicalSchema}
import com.invaract.fingerprint.TransformationFingerprint
import com.invaract.ir.{CatalogIdentity, Plan, Read, UnknownPlan, Write}


/** Checks a transformation plan's actual inputs and output against a
  * `Contract`'s declarations. This is ROADMAP.md Phase 4: the first
  * *useful* verifier, checking exactly the "Structural" class of property
  * from MISSION.md §8 — existence, location, schema, and (for outputs)
  * format, for both inputs and outputs — not yet dependency,
  * transformation, or governance checks.
  *
  * Two kinds of information feed a check, and they come from different
  * places:
  *
  *   - **Existence and location** are read directly off the `Plan`
  *     (`Read`/`Write` nodes' `DatasetRef.location`) — no Spark-specific
  *     data needed, since `ir.Plan` already carries this.
  *   - **Schema** (field presence, type, nullability) needs the actual
  *     `LogicalSchema` (the engine-neutral `com.invaract.contract` model, see
  *     `LogicalType`) for each dataset, because the IR deliberately
  *     carries no schema of its own (see `ir.Read`'s doc) — only which
  *     columns were *referenced*, not the dataset's full column set. The
  *     caller supplies these, already mapped from its engine's own types (for
  *     Spark, via `SparkSchemas`). Every resolved Catalyst `LogicalPlan` exposes
  *     its own `.schema` derived from resolved attributes, so a caller can
  *     get these directly from the *analyzed* plan — before anything
  *     executes — rather than needing a materialized `DataFrame`; see
  *     `ContractEnforcementRule`, which does exactly this to verify a write
  *     before Spark runs it.
  *
  * ## Determinism
  *
  * Given the same `contract`, `plan`, `inputSchemas`, and `outputSchema`,
  * `verify` always returns the same violations in the same order — no
  * hash-based `Set`/`Map` iteration in the result-building path (`Set`s are
  * used only for membership tests, never iterated to produce output). This
  * matters beyond reproducible tests: ROADMAP.md Phase 5 gates a real
  * Spark write on this result, and a nondeterministic verdict — or even a
  * deterministic verdict with nondeterministically-ordered violations —
  * would make a failure impossible to reliably reproduce or explain.
  *
  * ## Location matching
  *
  * A contract declares portable, relative locations
  * (`"demo/input/sample.csv"`); Spark reports absolute `file:` URIs at
  * runtime (`"file:/home/user/.../demo/input/sample.csv"`) — confirmed
  * empirically, see docs/SPARK_ADAPTER.md. Comparing these with `==` would
  * fail every declared input/output on every real run for a reason that
  * has nothing to do with contract compliance. Locations are matched by
  * normalized suffix instead: strip a `file:` scheme, then a declared
  * location matches if it equals the actual location or is a path-boundary
  * suffix of it.
  *
  * ## Multi-output contracts
  *
  * A `Contract` can declare multiple outputs (`contract.outputs: List`),
  * but one verification run only ever observes one `Write` — each write a
  * job performs triggers its own, independent check (see
  * `ContractEnforcementRule`'s "Fires on every analyzed plan" doc), so there
  * is never more than one actual output to reconcile against a contract's
  * several declared ones in a single `verify` call.
  *
  * The plan's actual write location is matched against every declared
  * output's `location` (via the same `locationsMatch` normalized-suffix
  * rule used everywhere else in this method), not just `contract.outputs.head`:
  *
  *   - A contract with exactly one declared output keeps its original
  *     behavior unchanged: format/saveMode/catalog/schema are all checked
  *     against that one output regardless of whether its location matches
  *     the actual write (a location mismatch is reported *in addition to*,
  *     not instead of, those other checks): with one declared output there is
  *     no ambiguity about which output the author meant, so every finding is
  *     reported in one run.
  *   - A contract with more than one declared output has no such single
  *     default to fall back to. Exactly one declared output matching the
  *     write's location → every other check (format, saveMode, catalog,
  *     schema) runs against *that* output specifically. No declared output
  *     matching → `OUTPUT_LOCATION_MISMATCH`, naming every declared output
  *     location as a candidate, and no format/saveMode/catalog/schema check
  *     runs at all — there is no non-ambiguous output left to check them
  *     against.
  *   - The plan produces no write at all → `MISSING_OUTPUT` once per
  *     declared output (for a single-output contract, exactly the original
  *     one violation).
  *
  * A contract with two declared outputs sharing the same `location` is
  * flagged by `ContractValidator` as a Warning (ambiguous, not rejected —
  * see its own doc); `verify` picks whichever matches first when that
  * happens, and doesn't special-case it further.
  *
  * ## Which inputs a write is checked against
  *
  * A contract's `inputs` and `outputs` are two flat lists, so a contract
  * governing several writes needs some way to say which inputs each
  * output is built from — a job can read three datasets and use two for
  * one output and one for another, and requiring every write to read every
  * input would reject exactly that. `Dataset.derivedFrom` on an output
  * (see its own doc) is that mapping, and `verify` scopes every per-input
  * check to `Contract.inputsFor(<the output this write lands on>)`:
  *
  *   - `MISSING_INPUT` (and the unverifiable-input classification below)
  *     is only considered for the scoped inputs, so an input an output
  *     doesn't derive from is never required of a write to it.
  *   - Under `rejectUndeclaredInputs`, a read of a declared input that is
  *     *not* in the scoped set is an `UNDECLARED_INPUT` (worded as "declared
  *     by this contract but not a source of output X") — the mapping is a
  *     real claim about the output's sources, and a plan's `Read` nodes are
  *     a structural fact about which datasets were composed into the write,
  *     so this is enforceable rather than advisory.
  *   - An output without `derivedFrom` keeps the original behavior exactly:
  *     every declared input is expected. So does a plan with no `Write`, and
  *     a multi-output write matching no declared output (which already
  *     reports `OUTPUT_LOCATION_MISMATCH` on its own).
  *
  * Scoping also narrows what the checkpoint-lineage evidence below can
  * excuse: only the write's own scoped inputs are ever candidates for
  * `UnverifiableInput`, so a checkpoint elsewhere in the session can no
  * longer mask a missing input the *other* output was never meant to read.
  *
  * ## Inputs hidden behind a lineage boundary
  *
  * A declared input with no matching `Read` node anywhere in `plan` is
  * usually genuinely missing — `MISSING_INPUT`. But a `.checkpoint()` call
  * sitting between the real read and the checked write erases Spark's own
  * analyzed-plan lineage back to it (`LogicalRDD` is a leaf that retains no
  * `LogicalPlan`). `SparkPlanAdapter` handles this *before* `verify` ever
  * runs: given a `CheckpointRegistry`, it resolves each `LogicalRDD` back to
  * the pre-checkpoint plan the check rule saw when that Dataset was
  * created, and splices it in — so the plan `verify` receives already
  * contains the real `Read` nodes, and every check below (missing,
  * undeclared, schema, catalog, lineage, fingerprint) simply works.
  *
  * What reaches `verify` unresolved is what the registry could not resolve
  * (an origin the rule never saw, an evicted one, or several plans reading
  * *different* sources sharing the boundary's output columns) and any
  * `InMemoryRelation`. For those, "no matching `Read`" is not the same claim
  * as "never read", so a declared input that is unread *and* has an
  * unresolved boundary somewhere in the plan is reported as an
  * `UnverifiableInput` instead of `MISSING_INPUT` — report-only, never a
  * `Violation`, naming the boundary's `ir.UnknownPlan.sourceType`
  * (`unknownNodeTypes`). This is deliberately the narrower, non-blocking
  * `RoleConsistencyVerifier`-style precedent (`Conforms`/`Contradicts`/
  * `CannotDetermine`), not `UNVERIFIABLE_WRITE`'s fail-closed one: the
  * uncertainty is about one input's visibility, not the whole write's
  * meaning, and failing closed would only turn a job that reads its input
  * just fine into a newly-blocked one. With no unresolved boundary, an
  * unread input is `MISSING_INPUT`, blocking, exactly as before any of this
  * existed — even if the job checkpoints something unrelated (that
  * checkpoint resolves, and its reads are visible).
  *
  * ## Visibility
  *
  * `private[invaract]`: nothing outside this module calls `verify`
  * directly (confirmed by grep before narrowing it —
  * `ContractEnforcementRule` is the only real caller). A real Invaract
  * user gets verification automatically via the installed extension
  * (`ContractEnforcementRule.forContract`) and never needs to call this
  * raw function themselves; `VerificationResult`/`Violation` (the payload
  * of `ContractViolationException.result`) remain public since a user
  * does need to inspect those. As with `SparkPlanAdapter`, this is a
  * Scala-compiler-enforced restriction, not a JVM one — the compiled
  * class stays `public` in bytecode, so MiMa doesn't (and structurally
  * can't) enforce this particular boundary; it only stops real Scala code
  * from depending on this by accident.
  */
private[invaract] object StructuralVerifier {

  /** `caseSensitive` is the session's `spark.sql.caseSensitive` (see
    * `SchemaChecker`'s class doc): it decides how declared field names are
    * matched against the plan's actual columns. It defaults to Spark's own
    * default, `false`; `ContractEnforcementRule` always passes the real setting.
    *
    * `lineageBoundaryTypes`: the `ir.UnknownPlan.sourceType`s this engine's
    * adapter treats as an unresolved lineage boundary (see "Inputs hidden
    * behind a lineage boundary" above). Spark passes
    * `CheckpointRegistry.BoundarySourceTypes`; the empty default means "this
    * engine has no such boundary", so an unread input is always `MISSING_INPUT`.
    */
  def verify(
    contract: Contract,
    plan: Plan,
    inputSchemas: List[(String, LogicalSchema)],
    outputSchema: LogicalSchema,
    options: VerificationOptions = VerificationOptions(),
    caseSensitive: Boolean = false,
    lineageBoundaryTypes: Set[String] = Set.empty
  ): VerificationResult =
    verify(contract, PlanFacts.of(plan), inputSchemas, outputSchema, options, caseSensitive, lineageBoundaryTypes)

  /** The same check over a plan whose shape `ContractEnforcementRule` has
    * already gathered once for every verifier it runs (see `PlanFacts`).
    */
  def verify(
    contract: Contract,
    facts: PlanFacts,
    inputSchemas: List[(String, LogicalSchema)],
    outputSchema: LogicalSchema,
    options: VerificationOptions,
    caseSensitive: Boolean,
    lineageBoundaryTypes: Set[String]
  ): VerificationResult = {
    // Which declared output this write lands on (a single-output contract's only
    // output whatever the location, else the one whose location matches), which
    // scopes the input checks too; the same `OutputChecker.expectedOutputFor`
    // the output checks use, so both halves always agree on which output a write
    // is "for". A plan with no Write (or a multi-output write matching no
    // declared output) has no output to scope by and keeps every input.
    val scopedOutput: Option[Dataset] = facts.plan match {
      case Write(dataset, _, _, _, _) => OutputChecker.expectedOutputFor(contract.outputs, dataset.location)
      case _                          => None
    }

    val inputs = InputChecker.check(contract, facts, scopedOutput, inputSchemas, options, caseSensitive, lineageBoundaryTypes)
    val outputs = OutputChecker.check(contract, facts.plan, outputSchema, options, caseSensitive)

    VerificationResult.of(s"${contract.id}@${contract.version}", inputs.violations ++ outputs, unverifiableInputs = inputs.unverifiable)
  }

  /** For state-changing, non-write operations that still result in a
    * committed schema change at a location - currently just Iceberg's
    * `rollback_to_snapshot` (see `StateChangingCallSupport`) - checked
    * against a contract's declared output. Deliberately narrower than
    * `verify` above: no `ir.Plan` to walk (there's no Spark query being
    * written, so no reads to collect and no input-side checking applies).
    *
    * Location is a *scoping* gate here, not a violation the way it is for
    * `verify`'s `Write` case above: a real write happening under an active
    * contract, to the wrong place, is worth flagging (something WAS
    * written, just not where expected). A state-changing operation on a
    * table this contract doesn't declare at all isn't a wrong-place write -
    * it's simply not this contract's concern, since one contract's
    * presence shouldn't gate every operation on every table in a job that
    * happens to run under it. So a location that doesn't match returns a
    * clean pass (no violations, schema not even checked) rather than an
    * `OutputLocationMismatch` violation - confirmed by a real test
    * (`IcebergConnectorSpec`'s "rollback_to_snapshot on a table the active
    * contract doesn't govern") that first caught this getting it backwards.
    * Schema checking only happens once location scoping says this
    * operation IS the contract's concern - reuses `SchemaChecker.check` directly
    * rather than duplicating it, same rules/violation types/remediation
    * wording as every other output check in this file.
    *
    * Multi-output contracts: the same `OutputChecker.matchOutput` lookup `verify` uses
    * above decides which declared output (if any) this location belongs
    * to - a contract declaring several outputs is scoped exactly the same
    * way a single-output one already was, just checked against whichever
    * one actually matches instead of always `contract.outputs.head`.
    */
  private[invaract] def verifyStateChange(
    contract: Contract,
    location: String,
    resultingSchema: LogicalSchema,
    options: VerificationOptions = VerificationOptions(),
    caseSensitive: Boolean = false
  ): VerificationResult =
    OutputChecker.matchOutput(contract.outputs, location) match {
      case None => VerificationResult.of(s"${contract.id}@${contract.version}", Nil)
      case Some(expectedOutput) =>
        val schemaViolations =
          SchemaChecker.check(expectedOutput.schema.fields, resultingSchema, SchemaChecker.Side.Output, location, options.rejectUndeclaredFields, caseSensitive)
        VerificationResult.of(s"${contract.id}@${contract.version}", schemaViolations)
    }

  /** `private[invaract]`: reused by `StaticDataQualityVerifier` to
    * discover a plan's real `Read` scopes before matching them against
    * `contract.inputs` — the same reason `locationsMatch` below is widened.
    */
  private[invaract] def collectReads(plan: Plan): List[Read] = PlanFacts.of(plan).reads

  /** Every `ir.UnknownPlan` node found anywhere in `plan` — feeds
    * `InputChecker.unverifiableEvidenceFor`.
    *
    * `private[invaract]`, not `private`: `ContractEnforcementRule`
    * reuses this directly to decide whether a computed
    * `TransformationFingerprint` crossed a lineage boundary worth
    * disclosing (see that call site's own doc) - the same "reuse, don't
    * re-derive" reasoning `collectReads`'s own widened visibility above
    * already documents.
    */
  private[invaract] def collectUnknownPlans(plan: Plan): List[UnknownPlan] = PlanFacts.of(plan).unknownPlans

  // private[invaract], not private: reused by SensitivityLineage to
  // match a traced ColumnRef's qualifier (a Read's actual reported
  // location) against a contract input's declared, portable location -
  // the same normalized-suffix rule this method already documents, not a
  // second copy of it (mirrors why normalizeLocation below already
  // has this same widened visibility, for ContractInference's reuse).
  //
  // A contract's declared location can come from anywhere (a config file
  // authored on Windows, e.g.), while Spark always reports actual plan
  // locations with forward slashes regardless of OS; the rule (both sides
  // normalized, then equal-or-suffix) lives in `LocationMatching`, shared with
  // the `LocationIndex` the bulk checks in `verify` use.
  private[invaract] def locationsMatch(declared: String, actual: String): Boolean =
    LocationMatching.matches(declared, actual)

  /** `location` against every member of `qualifiers` via `locationsMatch` —
    * factored out since `RoleConsistencyVerifier`/`ContractInference` both
    * need exactly this "does this declared/inferred location match any
    * observed qualifier" predicate, not two independent copies of it.
    */
  private[invaract] def matchesAny(location: String, qualifiers: Set[String]): Boolean =
    qualifiers.exists(q => locationsMatch(location, q))

  /** The bare, OS-agnostic form a contract's `declared` location is
    * expected to already be in, derived from a location as Spark itself
    * reports it (always forward-slash, often `file:`-scheme-prefixed for a
    * local path) - factored out of `locationsMatch` above so
    * `ContractInference` (dry-run mode) can apply the exact same
    * normalization when inferring a location a user will *declare* in a
    * contract, not just when comparing one against it. The two must use
    * one shared definition: an inferred contract that skipped this
    * normalization would carry a `"file:..."`-prefixed declared location
    * that `locationsMatch` never strips from the *declared* side, wrongly
    * rejecting the exact write it was inferred from - confirmed the hard
    * way by a real test failing this way before `ContractInference` was
    * fixed to call this instead of its own separate copy.
    */
  private[invaract] def normalizeLocation(actual: String): String =
    LocationMatching.normalizeActual(actual)
}
