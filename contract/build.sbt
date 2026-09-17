name := "invaract-contract"
// 0.2.0 -> 0.3.0: Field gained a sensitivityTags constructor parameter
// (see docs/CONTRACT_MODEL.md / docs-site's "Sensitivity tags" section),
// changing Field.apply/copy/this's arity - a real binary break, confirmed
// by a real `sbt mimaReportBinaryIssues` run against the 0.2.0 baseline
// below, not assumed. Pre-1.0, docs/VERSIONING.md's FAQ calls for bumping
// the MINOR digit (not MAJOR, pinned at 0 until 1.0.0) to signal a
// deliberate break - the same convention the 0.1.0 -> 0.2.0 rebrand
// itself used.
// 0.5.0 -> 0.6.0: OrgPolicy gained a customPolicyTypes constructor
// parameter (see docs/CONTRACT_MODEL.md's "Custom policy types" section),
// changing OrgPolicy.apply/copy/this's arity - the same shape of break as
// 0.2.0 -> 0.3.0 above, confirmed by a real `sbt mimaReportBinaryIssues`
// run against the 0.5.0 baseline below before this bump, not assumed.
// 0.6.0 -> 0.7.0: Contract gained a customRuleTypes constructor parameter
// (see docs/SPARK_ADAPTER.md's "Custom rule types" section), the same
// shape of break again - confirmed by a real `sbt mimaReportBinaryIssues`
// run against the 0.6.0 baseline below before this bump, not assumed.
// ThisBuild-scoped, not a bare `version :=` - sbt-sonatype's
// sonatypePublishToBundle (and other cross-cutting plugin settings) reads
// ThisBuild/version specifically, which otherwise silently stays at sbt's
// own "0.1.0-SNAPSHOT" default even though every in-file reference to
// "this module's version" (assembly jar name, mimaPreviousArtifacts,
// etc.) correctly saw "0.3.0" - confirmed the gap directly with
// `sbt "show version" "show ThisBuild/version"` before fixing it, not
// assumed.
ThisBuild / version := "0.7.0"
scalaVersion := "2.12.18"
organization := "com.invaract"

// --- Maven Central publishing (Central Portal) ---
// This module is one of the three published to Maven Central (see
// CLAUDE.md's "What's the product": contract/ir/spark-adapter). No release
// has actually been published yet - the `organization` above is the
// intended groupId, contingent on completing Sonatype's namespace
// verification for it (a DNS TXT record proving control of the invaract.com
// domain; see docs/RELEASING.md). Everything below is the POM metadata and
// Central Portal settings Sonatype requires before it will accept any
// release at all, regardless of namespace.
publishMavenStyle := true
Test / publishArtifact := false
pomIncludeRepository := { _ => false }

homepage := Some(url("https://github.com/mlltx/Invaract"))
licenses := List("Apache-2.0" -> url("https://www.apache.org/licenses/LICENSE-2.0.txt"))
scmInfo := Some(
  ScmInfo(
    url("https://github.com/mlltx/Invaract"),
    "scm:git@github.com:mlltx/Invaract.git"
  )
)
developers := List(
  // id/url are the real GitHub org this repo lives under; email is
  // GitHub's own noreply-alias convention, to avoid publishing a personal
  // address in a public POM. Update `name` with a real maintainer name
  // before the first real release.
  Developer(
    id = "mlltx",
    name = "mlltx",
    email = "mlltx@users.noreply.github.com",
    url = url("https://github.com/mlltx")
  )
)

// Pre-1.0 (docs/VERSIONING.md): a 0.x -> 0.(x+1) bump may be
// binary-breaking, so "early-semver" is the accurate scheme - the same
// convention this file's own mimaPreviousArtifacts comment already bumps
// MINOR (not PATCH) for a deliberate break under.
versionScheme := Some("early-semver")

// The OSSRH/Nexus staging host sbt-sonatype originally targeted is
// retired; Central Portal (central.sonatype.com) is the only route onto
// Maven Central now. See docs/RELEASING.md for credentials/CI wiring and
// the actual release commands (publishSigned, then sonatypeCentralRelease).
import xerial.sbt.Sonatype.sonatypeCentralHost
sonatypeCredentialHost := sonatypeCentralHost
publishTo := sonatypePublishToBundle.value

// Non-interactive PGP passphrase for CI (crazy-max/ghaction-import-gpg
// imports the key + configures gpg-agent; this just supplies the
// passphrase sbt-pgp needs when it shells out to gpg). Unset locally -
// sbt-pgp falls back to an interactive prompt, which is fine for a
// maintainer cutting a release by hand.
pgpPassphrase := sys.env.get("PGP_PASSPHRASE").map(_.toCharArray)

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

assembly / assemblyJarName := "invaract-contract-0.7.0.jar"
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
// mimaPreviousArtifacts stays at 0.6.0 here as the LOCAL-DEV fallback only
// (a plain `sbt mimaReportBinaryIssues` outside CI, comparing against the
// last baseline a human bothered to write down). It is deliberately NOT
// what CI's api-compatibility job actually checks against.
//
// ROOT CAUSE this env-var override fixes: this hardcoded literal used to
// be the *only* source of truth, and it had to be manually "flipped" to
// match base-ref's own `version` in a separate FOLLOW-UP PR every time
// this file's `version` (above) bumped - CI publishes base-ref's build
// under base-ref's own current version, so the moment that version moves
// past whatever's hardcoded here, every OTHER PR's api-compatibility job
// fails resolving a now-nonexistent coordinate (e.g. "Error downloading
// com.invaract:invaract-contract_2.12:0.5.0 - Not found") until someone
// notices and lands the flip - a real, repeated failure mode, not a
// hypothetical one: the 0.2.0->0.3.0 through 0.5.0->0.6.0 bumps here (and
// spark-adapter's 0.4.0->0.5.0) each cost a separate reactive fix-CI PR,
// and this exact gap broke this repository's own PR #65 while that
// follow-up sat unmade - see #65's own fix (which introduced this
// mechanism) for the full history.
//
// The fix: CI's api-compatibility job (.github/workflows/test.yml) now
// reads base-ref's OWN `ThisBuild / version` directly off its checked-out
// build.sbt and passes it as INVARACT_MIMA_BASELINE_VERSION, so the
// baseline PR-head is compared against always matches what CI actually
// just published - by construction, not by a human remembering a
// follow-up PR. The hardcoded literal below only matters for a local
// `sbt mimaReportBinaryIssues` run with no env var set; bump it whenever
// convenient, but a stale value here can no longer break CI for anyone.
mimaPreviousArtifacts := Set(
  "com.invaract" %% "invaract-contract" % sys.env.getOrElse("INVARACT_MIMA_BASELINE_VERSION", "0.6.0")
)

import com.typesafe.tools.mima.core._

// The OrgPolicy.customPolicyTypes break (0.5.0 -> 0.6.0, landed via #62)
// is baked into both sides of any comparison against a 0.6.0-or-later
// baseline by construction now (see the comment above), so its four
// filters are removed here rather than kept as dead entries with nothing
// left to match.
//
// The real, deliberate break motivating the 0.6.0 -> 0.7.0 bump above:
// Contract gained a sixth constructor parameter, customRuleTypes (see
// docs/SPARK_ADAPTER.md's "Custom rule types" section) - confirmed by a
// real `sbt mimaReportBinaryIssues` run against the 0.6.0 baseline before
// this bump, not assumed. Unlike the old hardcoded-literal mechanism,
// these filters become inert on their own (matching nothing) once
// base-ref's own version reaches 0.7.0 or later - no future PR needs to
// remember to remove them to keep CI green, though it's fine to clean
// them up whenever this file is next touched.
mimaBinaryIssueFilters ++= Seq(
  ProblemFilters.exclude[DirectMissingMethodProblem]("com.invaract.contract.Contract.apply"),
  ProblemFilters.exclude[DirectMissingMethodProblem]("com.invaract.contract.Contract.copy"),
  ProblemFilters.exclude[DirectMissingMethodProblem]("com.invaract.contract.Contract.this"),
  ProblemFilters.exclude[MissingTypesProblem]("com.invaract.contract.Contract$")
)
