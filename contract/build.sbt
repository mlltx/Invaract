name := "invaract-contract"
// 0.2.0 -> 0.3.0: Field gained a sensitivityTags constructor parameter
// (see docs/CONTRACT_MODEL.md / docs-site's "Sensitivity tags" section),
// changing Field.apply/copy/this's arity - a real binary break, confirmed
// by a real `sbt mimaReportBinaryIssues` run against the 0.2.0 baseline
// below, not assumed. Pre-1.0, docs/VERSIONING.md's FAQ calls for bumping
// the MINOR digit (not MAJOR, pinned at 0 until 1.0.0) to signal a
// deliberate break - the same convention the 0.1.0 -> 0.2.0 rebrand
// itself used.
// ThisBuild-scoped, not a bare `version :=` - sbt-sonatype's
// sonatypePublishToBundle (and other cross-cutting plugin settings) reads
// ThisBuild/version specifically, which otherwise silently stays at sbt's
// own "0.1.0-SNAPSHOT" default even though every in-file reference to
// "this module's version" (assembly jar name, mimaPreviousArtifacts,
// etc.) correctly saw "0.3.0" - confirmed the gap directly with
// `sbt "show version" "show ThisBuild/version"` before fixing it, not
// assumed.
ThisBuild / version := "0.3.0"
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
