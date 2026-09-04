name := "invaract-ir"
// 0.2.0 -> 0.3.0: the expression-algebra rework (FunctionCall split into
// Cast/Arithmetic/Comparison/BooleanExpr/Conditional/Function/UDF/Alias;
// Unsupported/UnsupportedExpr renamed to UnknownPlan/UnknownExpression;
// ColumnRef gained an `id` field) and the lineage rework (ColumnLineage's
// `aggregated: Boolean` replaced with `derivation`/`aggregations`) are
// both real binary breaks against the 0.2.0 baseline, confirmed by a real
// `sbt mimaReportBinaryIssues` run, not assumed. Pre-1.0,
// docs/VERSIONING.md's FAQ calls for bumping the MINOR digit (not MAJOR,
// pinned at 0 until 1.0.0) to signal a deliberate break - the same
// convention the 0.1.0 -> 0.2.0 rebrand itself used.
// ThisBuild-scoped, not a bare `version :=` - see contract/build.sbt's
// matching comment for why (sonatypePublishToBundle reads ThisBuild/version
// specifically; confirmed the gap directly with
// `sbt "show version" "show ThisBuild/version"` before fixing it there).
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
  "org.scalatest" %% "scalatest" % "3.2.18" % "test"
)

scalacOptions ++= Seq(
  "-target:jvm-1.8",
  "-deprecation",
  "-feature"
)

assembly / assemblyJarName := "invaract-ir-0.3.0.jar"

// Mutation testing (Stryker4s) config: see stryker4s.conf for reporters.
// `mutate`/`thresholds` are set here rather than in stryker4s.conf, whose
// equivalent keys were observed not to take effect via the config file in
// this sbt/plugin version combination - these sbt settings do work.
// Whole-module scope (widened from just Lineage.scala once the initial
// narrow pass's score was reviewed). `break` is what makes CI's
// mutation-testing job fail when the score regresses below it.
strykerMutate := Seq("src/main/scala/**/*.scala")
strykerThresholdsHigh := 80
strykerThresholdsLow := 60
strykerThresholdsBreak := 50

// API compatibility (MiMa) - see contract/build.sbt's comment for the full
// rationale (no Maven Central release yet, so CI's `api-compatibility` job
// compares against the PR's own base branch instead) and
// docs/TRANSFORMATION_IR.md's "API compatibility" section.
//
// Points at the base branch's own current published coordinate, which
// must always track base-ref's own live `version` above (CI's
// api-compatibility job runs `sbt publishLocal` against base-ref's own
// build.sbt, then resolves exactly this coordinate against it - see
// contract/build.sbt's matching comment for the general invariant this
// has to satisfy).
//
// The 0.2.0 -> 0.3.0 bump (this file's version comment above) landed on
// the base branch in its own PR, which left this pointing at the
// now-superseded 0.2.0 baseline with a "FOLLOW-UP: flip this once that PR
// lands" comment. That PR has now landed (base-ref itself publishes
// 0.3.0, not 0.2.0, confirmed the hard way: CI's api-compatibility job
// failed with a real "Not found" resolving 0.2.0), so this is that
// follow-up flip.
mimaPreviousArtifacts := Set("com.invaract" %% "invaract-ir" % "0.3.0")

// FOLLOW-UP (once a future PR bumps `version` above again): flip this to
// that new version and add filters for whatever real break motivated the
// bump, mirroring this section's own history - do not make that flip in
// the PR doing the bump itself (base-ref won't have it yet).
//
// No filters needed right now: mimaPreviousArtifacts above already equals
// this module's own current version, so there is nothing between them to
// filter - both breaks that motivated the 0.2.0 -> 0.3.0 bump (the
// expression-algebra rework splitting FunctionCall/renaming Unsupported*/
// adding ColumnRef.id, and the lineage rework replacing ColumnLineage's
// aggregated: Boolean with derivation/aggregations) are now baked into
// both sides of the comparison. The ~20 filter lines that documented them
// against the old 0.2.0 baseline were removed here rather than left as
// dead entries with nothing left to match.
