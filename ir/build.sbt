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
version := "0.3.0"
scalaVersion := "2.12.18"
organization := "com.invaract"

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
