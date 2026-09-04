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
// rebrand PR, which landed on the base branch (main) some time ago -
// base-ref's own build.sbt publishes under organization "com.invaract"
// (CI's api-compatibility job runs `sbt publishLocal` against base-ref's
// own build.sbt settings, not this file's), so this must always match
// base-ref's own current `version` above, not some fixed historical one.
//
// The 0.2.0 -> 0.3.0 bump (this file's version comment above) landed on
// the base branch in its own PR, which left this pointing at the
// now-superseded 0.2.0 baseline with a "FOLLOW-UP: flip this once that PR
// lands" comment - the same pattern the rebrand itself used (see git
// history for both prior revisions). That PR has now landed (base-ref
// itself publishes 0.3.0, not 0.2.0, confirmed the hard way: CI's
// api-compatibility job failed with a real "Not found" resolving 0.2.0,
// since base-ref never publishes that coordinate once its own `version`
// moved past it), so this is that follow-up flip.
mimaPreviousArtifacts := Set("com.invaract" %% "invaract-contract" % "0.3.0")

// FOLLOW-UP (once a future PR bumps `version` above again): flip this to
// that new version and add filters for whatever real break motivated the
// bump, mirroring this section's own history - do not make that flip in
// the PR doing the bump itself (base-ref won't have it yet).
//
// No filters needed right now: mimaPreviousArtifacts above already equals
// this module's own current version, so there is nothing between them to
// filter - the sensitivityTags-on-Field break that motivated the 0.2.0 ->
// 0.3.0 bump is now baked into both sides of the comparison. The filters
// that documented it against the old 0.2.0 baseline were removed here
// rather than left as dead entries with nothing left to match.
