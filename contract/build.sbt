name := "invaract-contract"
version := "0.2.0"
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

assembly / assemblyJarName := "invaract-contract-0.2.0.jar"
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
// rebrand PR, which left this coordinate pointing at the transitional
// "invariant-contract" name with a comment saying it needed flipping once
// that PR reached the base branch - it now has, so base-ref's own build.sbt
// (like this one) publishes under "invaract-contract", and this must match.
mimaPreviousArtifacts := Set("com.example" %% "invaract-contract" % "0.1.0")

// com.example -> com.invaract namespace rebrand (this PR): the package and
// `organization` moved wholesale, so every symbol in this module now has a
// different fully-qualified name than the artifact published above - MiMa
// correctly reports every one of them as "removed." This is the real,
// intended shape of a deliberate break (CLAUDE.md's "API Compatibility
// Requirement" option 2), not something to silence per-symbol: the version
// bump to 0.2.0 above is the MAJOR-equivalent bump docs/VERSIONING.md's
// pre-1.0 policy calls for, and this filter is the honest, broad
// acknowledgment that the entire com.example.contract.* surface was
// intentionally moved to com.invaract.contract.*, not accidentally deleted.
//
// FOLLOW-UP (once this PR lands on the base branch): flip
// `mimaPreviousArtifacts` above to
// `Set("com.invaract" %% "invaract-contract" % "0.2.0")` and remove this
// filter, so future PRs compare against the new namespace's own baseline
// instead of comparing against the pre-rename one forever. Do not make that
// flip in this PR - base-ref won't have the rename yet, so CI's publish of
// base-ref would fail to produce anything at the new coordinate.
import com.typesafe.tools.mima.core._
mimaBinaryIssueFilters ++= Seq(
  ProblemFilters.exclude[Problem]("com.example.contract.*")
)
