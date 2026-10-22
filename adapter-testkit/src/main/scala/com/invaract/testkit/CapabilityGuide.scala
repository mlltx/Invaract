// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.testkit

import com.invaract.verification.{Capability, CapabilityCategory, EnforcementPoint}

/** Every capability in plain words, for the people who use Invaract rather than build it: what it does for them, what
  * they lose on an engine that does not do it, and the guide that shows it. The "What Each Engine Checks" page
  * (`EngineGuidePage`) is written from this and from each adapter's declaration; `Capability.description` stays the
  * precise, contributor-facing wording the capability matrix shows.
  *
  * A new `Capability` needs an entry here: `CapabilityGuideTest` fails until it has one, so a check never reaches the
  * user page without saying what it means.
  *
  * @param title a short name a user would recognise ("Columns match the contract"), unique across capabilities.
  * @param what what Invaract does for the user when an engine supports it, in a sentence or two.
  * @param withoutIt what the user loses on an engine that does not, for a capability that does not guard the contract
  *   (one that does always says the same: the job is refused - see `EngineGuidePage`).
  * @param guide the docs-site page that shows it, as a root-relative link.
  */
final case class CapabilityExplained(title: String, what: String, withoutIt: String, guide: Option[String])

object CapabilityGuide {

  private def e(title: String, what: String, withoutIt: String = "", guide: String = "") =
    CapabilityExplained(title, what, withoutIt, Option(guide).filter(_.nonEmpty))

  /** The plain heading for each category of capabilities. */
  val categoryTitles: Map[CapabilityCategory, String] = Map(
    CapabilityCategory.Operations -> "Which reads and writes it sees",
    CapabilityCategory.StructuralChecks -> "What it checks about each write",
    CapabilityCategory.Rules -> "Rules a contract can declare",
    CapabilityCategory.Analysis -> "Deeper analysis",
    CapabilityCategory.Governance -> "Organization-wide policy",
    CapabilityCategory.Attachment -> "Setting it up",
    CapabilityCategory.Reporting -> "Reporting",
    CapabilityCategory.FailClosed -> "Safety net"
  )

  /** How an adapter at each enforcement point stops a write, in plain words. */
  val enforcement: Map[EnforcementPoint, String] = Map(
    EnforcementPoint.InEngineBlocking ->
      ("Checks each write inside the running job, just before it happens. A write that breaks the contract is stopped and " +
        "the job fails; writes the job made before it have already happened."),
    EnforcementPoint.PreSubmitGate ->
      "Checks the whole pipeline before it is handed to a runner. If anything breaks the contract, nothing runs at all.",
    EnforcementPoint.ObserveOnly -> "Watches the job and reports what breaks the contract, but cannot stop it."
  )

  val entries: Map[Capability, CapabilityExplained] = {
    import Capability._
    Map(
      ReadBatch -> e(
        "Batch reads",
        "Sees which datasets a batch job reads, so they can be checked against the contract's inputs.",
        "A read it cannot see cannot be matched to the contract's inputs: a declared input it may be is reported as unverifiable.",
        "/reference/connector-support/"
      ),
      ReadStreaming -> e(
        "Streaming reads",
        "Sees which datasets a streaming job reads, as it does for a batch job.",
        "A streaming read it cannot place is reported as unverifiable rather than checked.",
        "/reference/connector-support/"
      ),
      WriteBatch -> e(
        "Batch writes",
        "Checks every batch write against the contract before the data is written.",
        "A batch write it does not recognize is refused (see the safety net below), not passed unchecked.",
        "/reference/connector-support/"
      ),
      WriteStreaming -> e(
        "Streaming writes",
        "Checks a streaming write against the contract when the stream starts.",
        "A streaming write it does not recognize is refused, not passed unchecked.",
        "/reference/connector-support/"
      ),
      WriteRowLevelDml -> e(
        "MERGE, UPDATE and DELETE",
        "Treats a row-level change to a table as a write, and checks it like one.",
        "Row-level changes are not checked as writes.",
        "/guides/enforcing-dml-rules/"
      ),
      WriteStateChange -> e(
        "Table rollbacks and similar changes",
        "Checks an operation that changes what a table holds without writing new rows, such as rolling it back to an earlier snapshot.",
        "Such an operation is not checked against the contract."
      ),
      CheckInputExistence -> e(
        "Required inputs are read",
        "Stops a write that does not read an input the contract requires (`MISSING_INPUT`). It can also stop a write that reads a dataset the contract does not list (`UNDECLARED_INPUT`).",
        guide = "/guides/writing-a-contract/"
      ),
      CheckLocation -> e(
        "Writes land where the contract says",
        "Stops a write to any location other than the one the contract declares (`OUTPUT_LOCATION_MISMATCH`).",
        guide = "/guides/writing-a-contract/"
      ),
      CheckOutputsWritten -> e(
        "Every promised output is written",
        "Catches a job that ends without writing an output its contract declares (`MISSING_OUTPUT`).",
        "A job that skips one of its outputs is not caught.",
        "/guides/installing-the-enforcement-rule/#every-declared-output-must-be-written"
      ),
      CheckSchema -> e(
        "Columns match the contract",
        "Stops a write whose columns are missing, of the wrong type, or nullable where the contract says they must not be.",
        guide = "/guides/writing-a-contract/"
      ),
      CheckNestedTypes -> e(
        "Nested columns are compared in full",
        "Compares arrays, maps and structs element by element, not only by their outer type.",
        guide = "/reference/contract-format/"
      ),
      CheckFormat -> e(
        "The file format matches",
        "Stops a write in a different format from the one the contract declares, such as CSV where it says Parquet (`OUTPUT_FORMAT_MISMATCH`).",
        guide = "/reference/contract-format/"
      ),
      CheckSaveMode -> e(
        "The save mode matches",
        "Stops a write that appends where the contract says overwrite, or the other way round (`OUTPUT_SAVE_MODE_MISMATCH`).",
        guide = "/reference/contract-format/"
      ),
      CheckCatalogRegistration -> e(
        "Datasets are registered in a catalog",
        "Checks that a dataset the contract says must be catalog-registered is, in the catalog it names.",
        guide = "/guides/requiring-catalog-registration/"
      ),
      RulesDml -> e(
        "Rules for MERGE, UPDATE and DELETE",
        "Enforces rules about row-level changes: the columns a MERGE must match on, no DELETE without a condition, the columns an UPDATE may change.",
        guide = "/guides/enforcing-dml-rules/"
      ),
      RulesPlanShape -> e(
        "Rules about the transformation's shape",
        "Enforces rules about how the job computes its output: the columns it must group by, join on or filter on, and no join without a condition.",
        guide = "/guides/enforcing-transformation-rules/"
      ),
      RulesCustom -> e(
        "Your own rule types",
        "Runs rule types your organization writes itself, named in the contract's `customRuleTypes`.",
        guide = "/guides/enforcing-dml-rules/#write-your-own-rule-type"
      ),
      AnalysisStaticDataQuality -> e(
        "Proves value constraints",
        "Proves from the job's own logic that a column always meets its declared constraint (never null, one of a set of values, in a range), before any row is written. Turned on with the `staticDataQuality` setting.",
        guide = "/guides/verifying-static-data-quality/"
      ),
      AnalysisRoleConsistency -> e(
        "Datasets are used as their declared type says",
        "Checks that each dataset is used the way its declared type (`SOURCE`, `CONTROL`, `DATA_ASSET`) says. Turned on with the `roleConsistency` setting.",
        guide = "/guides/declaring-input-and-output-types/"
      ),
      AnalysisFingerprint -> e(
        "Fingerprints the transformation's logic",
        "Adds a fingerprint of what the job computes to each check, so a change to its logic can be spotted even when the columns stay the same. Turned on with the `computeFingerprint` setting.",
        "Turning `computeFingerprint` on does not stop the job; no fingerprint is published, and a warning in the log says why.",
        "/guides/fingerprinting-transformations/"
      ),
      AnalysisFunctionCatalog -> e(
        "Recognizes built-in functions",
        "Knows which of the engine's built-in functions give a different answer on each run (random numbers, the current time), so fingerprints say so and compare across engines.",
        "Matters only for fingerprints, which this engine does not produce.",
        "/guides/fingerprinting-transformations/"
      ),
      AnalysisSensitivityPropagation -> e(
        "Tracks sensitive columns",
        "Reports output columns built from inputs tagged as sensitive (personal data, say) as sensitive too.",
        "No sensitivity report is produced; tags on the contract's fields are kept but not followed through the job.",
        "/reference/contract-format/"
      ),
      LineageColumnLevel -> e(
        "Traces each output column to its inputs",
        "Works out which input columns each output column is built from. The deeper checks above rely on it.",
        "Checks that rely on it, such as proving value constraints, are not available."
      ),
      LineageBoundaryResolution -> e(
        "Sees through checkpoints and caches",
        "Still traces a job back to what it read when it checkpoints or caches data between reading and writing.",
        "A checkpoint or cache hides what is behind it: an input read only behind one is reported as unverifiable."
      ),
      PolicyOrganizational -> e(
        "Organization-wide policy",
        "Checks every contract a job uses against a policy your platform team sets, such as \"every output must be catalog-registered\".",
        "A policy is not applied.",
        "/guides/enforcing-organizational-policy/"
      ),
      ConfigZeroCodeInstall -> e(
        "Turn it on without changing the job",
        "A platform team can attach Invaract to a job it does not own, through the engine's own launch settings, with no change to the job's code.",
        "The job's own code has to install Invaract.",
        "/reference/configuration/"
      ),
      ConfigLocationRefs -> e(
        "Locations resolved when the job starts",
        "A contract can name a location as `ref://<id>`, resolved from a mapping file when the job starts, so one contract works in every environment.",
        "Contracts must name real locations.",
        "/guides/resolving-dataset-locations/"
      ),
      ConfigContractRegistry -> e(
        "Contracts from a registry",
        "A job can be pointed at a contract kept in a contract registry (`registry://<id>@<version>`) instead of a file.",
        "Contracts must be files the job can read.",
        "/guides/using-a-contract-registry/"
      ),
      ReportingNotifications -> e(
        "Sends events to your systems",
        "Publishes an event for every check, pass or fail, to a file, an HTTP endpoint, Kafka or a sink you write.",
        "No events are published.",
        "/guides/notification-sinks/"
      ),
      ReportingDryRun -> e(
        "Writes a starting contract for you",
        "In dry-run mode, blocks nothing and writes out the contract each write would need, as a draft to review.",
        "There is no dry-run mode: contracts are written by hand.",
        "/guides/dry-run-mode/"
      ),
      FailClosedUnverifiableWrites -> e(
        "Refuses writes it cannot understand",
        "Refuses a step that writes data in a way Invaract cannot check (`UNVERIFIABLE_WRITE`), rather than letting it through unchecked.",
        "A write it cannot understand may go through unchecked.",
        "/concepts/fail-closed/"
      )
    )
  }

  def of(capability: Capability): CapabilityExplained = entries(capability)

  /** What a user loses on an engine that does not support `capability`. One that guards the contract has the job refused,
    * so nothing is ever silently half-checked; the others say what goes missing. */
  def withoutIt(capability: Capability): String =
    if (capability.enforcesContract)
      "If your contract relies on it, the job is refused before it writes anything (`UNSUPPORTED_CONTRACT_FEATURE`), rather than run with that part unchecked."
    else of(capability).withoutIt
}
