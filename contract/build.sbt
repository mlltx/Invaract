name := "invaract-contract"
// 0.2.0 -> 0.3.0: Field gained a sensitivityTags constructor parameter
// (see docs/CONTRACT_MODEL.md / docs-site's "Sensitivity tags" section),
// changing Field.apply/copy/this's arity - a real binary break, confirmed
// by a real `sbt mimaReportBinaryIssues` run against the 0.2.0 baseline
// below, not assumed. Pre-1.0, docs/VERSIONING.md's FAQ calls for bumping
// the MINOR digit (not MAJOR, pinned at 0 until 1.0.0) to signal a
// deliberate break - the same convention the 0.1.0 -> 0.2.0 rebrand
// itself used.
version := "0.3.0"
scalaVersion := "2.12.18"
organization := "com.invaract"

libraryDependencies ++= Seq(
  "org.yaml" % "snakeyaml" % "2.2",
  "org.scalatest" %% "scalatest" % "3.2.18" % "test",
  // Validates contract/schema/invaract-contract.schema.json against real
  // fixtures (ContractSchemaSpec) - test-scoped only. The schema is a
  // static artifact for external tooling to bind to; nothing in the
  // contract module's own runtime parses YAML against it (ContractParser/
  // ContractValidator remain the authoritative implementation).
  "com.networknt" % "json-schema-validator" % "1.4.1" % "test"
)

scalacOptions ++= Seq(
  "-target:jvm-1.8",
  "-deprecation",
  "-feature"
)

assembly / assemblyJarName := "invaract-contract-0.3.0.jar"
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

// API compatibility (MiMa): fails `sbt mimaReportBinaryIssues` if this
// module's public API (Contract/Dataset/Schema/Field/ContractVersion/
// ContractRule and ContractParser/ContractValidator/ContractCompatibility's
// signatures) changed in a way that breaks binary compatibility with the
// version below. There is no Maven Central release yet to compare
// against, so CI's `api-compatibility` job (.github/workflows/test.yml)
// publishes the PR's base-ref build to the local Ivy cache under this same
// coordinate first, then runs this task against the PR's head - i.e. "did
// this PR break compatibility with its own base branch," the same
// rolling-comparison approach the incremental mutation-testing check uses
// (see docs/CONTRACT_MODEL.md's "API compatibility" section). A real
// accepted break (a deliberate MAJOR-version API change) needs an explicit,
// documented entry in `mimaBinaryIssueFilters` below, not a version bump
// alone - see CLAUDE.md's "API Compatibility Requirement".
//
// This module was renamed invariant-contract -> invaract-contract by the
// rebrand PR, which has now landed on the base branch (main): base-ref's
// own build.sbt already publishes under organization "com.invaract",
// version "0.2.0" (CI's api-compatibility job runs `sbt publishLocal`
// against base-ref's own build.sbt settings, not this file's), so this
// must match that, not the pre-rebrand "com.example"/"0.1.0" coordinate -
// see this file's own prior revision for the transitional state and the
// "FOLLOW-UP" comment that called for this exact flip once the rebrand PR
// reached the base branch (confirmed via `git merge-base` against
// origin/main: it has).
mimaPreviousArtifacts := Set("com.invaract" %% "invaract-contract" % "0.2.0")

// Deliberate break against the 0.2.0 baseline above: Field gained a
// sensitivityTags: Set[String] constructor parameter (see the version
// comment above), changing Field.apply/copy/this's arity from 5 params to
// 6 - the exact filter lines below are copied verbatim from a real `sbt
// mimaReportBinaryIssues` run's own suggested output, not guessed.
import com.typesafe.tools.mima.core._
mimaBinaryIssueFilters ++= Seq(
  ProblemFilters.exclude[DirectMissingMethodProblem]("com.invaract.contract.Field.apply"),
  ProblemFilters.exclude[DirectMissingMethodProblem]("com.invaract.contract.Field.copy"),
  ProblemFilters.exclude[DirectMissingMethodProblem]("com.invaract.contract.Field.this"),
  ProblemFilters.exclude[MissingTypesProblem]("com.invaract.contract.Field$")
)
