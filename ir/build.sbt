import com.typesafe.tools.mima.core._

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
// Points at the base branch's own current published coordinate
// (com.invaract/0.2.0, confirmed via a real `sbt publishLocal` + `sbt
// mimaReportBinaryIssues` run against it, not assumed) - the rebrand that
// produced that coordinate has already landed on the base branch, so this
// is no longer the transitional "com.example/0.1.0" state a prior
// revision of this file pointed at.
mimaPreviousArtifacts := Set("com.invaract" %% "invaract-ir" % "0.2.0")

// Two independent deliberate breaks against the 0.2.0 baseline above, both
// documented rather than silently filtered - the exact filter lines below
// are copied verbatim from a real `sbt mimaReportBinaryIssues` run's own
// suggested output:
//
// 1. The "Spark Logical Plan -> Invaract IR" expression-level rework:
//    split the single `FunctionCall` node into `Cast`/`Arithmetic`/
//    `Comparison`/`BooleanExpr`/`Conditional`/`Function`/`UDF`/`Alias`,
//    renamed `Unsupported`/`UnsupportedExpr` to `UnknownPlan`/
//    `UnknownExpression` (adding a `sourceType` field to each), and added
//    an optional `id` field to `ColumnRef` for translator-assigned column
//    identity - see docs/TRANSFORMATION_IR.md's "Critical principle"
//    section for the full rationale. (`UnknownPlan`/`UnknownExpression`
//    themselves are new symbols, not removals, so they need no filter of
//    their own - only the old `Unsupported`/`UnsupportedExpr`/
//    `FunctionCall` classes they replaced show up below.)
// 2. `ColumnLineage`'s single `aggregated: Boolean` field replaced with
//    `derivation: DerivationKind` (a new sealed trait: Direct/Constant/
//    Computed/Opaque) and `aggregations: Set[AggregationDetail]` (which
//    aggregate function(s), not just whether one was involved) - see
//    docs/TRANSFORMATION_IR.md's "Lineage tracing" section. `Lineage`'s
//    private `Provenance` case class changed the same way (MiMa still
//    reports it: `private`, not `private[this]`, doesn't fully hide a
//    case class's synthesized methods at the bytecode level).
mimaBinaryIssueFilters ++= Seq(
  ProblemFilters.exclude[MissingClassProblem]("com.invaract.ir.FunctionCall"),
  ProblemFilters.exclude[MissingClassProblem]("com.invaract.ir.FunctionCall$"),
  ProblemFilters.exclude[MissingClassProblem]("com.invaract.ir.Unsupported"),
  ProblemFilters.exclude[MissingClassProblem]("com.invaract.ir.Unsupported$"),
  ProblemFilters.exclude[MissingClassProblem]("com.invaract.ir.UnsupportedExpr"),
  ProblemFilters.exclude[MissingClassProblem]("com.invaract.ir.UnsupportedExpr$"),
  ProblemFilters.exclude[DirectMissingMethodProblem]("com.invaract.ir.ColumnRef.apply"),
  ProblemFilters.exclude[DirectMissingMethodProblem]("com.invaract.ir.ColumnRef.copy"),
  ProblemFilters.exclude[DirectMissingMethodProblem]("com.invaract.ir.ColumnRef.this"),
  ProblemFilters.exclude[MissingTypesProblem]("com.invaract.ir.ColumnRef$"),
  ProblemFilters.exclude[DirectMissingMethodProblem]("com.invaract.ir.ColumnLineage.apply"),
  ProblemFilters.exclude[DirectMissingMethodProblem]("com.invaract.ir.ColumnLineage.copy"),
  ProblemFilters.exclude[DirectMissingMethodProblem]("com.invaract.ir.ColumnLineage.this"),
  ProblemFilters.exclude[IncompatibleResultTypeProblem]("com.invaract.ir.ColumnLineage.copy$default$3"),
  ProblemFilters.exclude[MissingTypesProblem]("com.invaract.ir.ColumnLineage$"),
  ProblemFilters.exclude[DirectMissingMethodProblem]("com.invaract.ir.Lineage#Provenance.aggregated"),
  ProblemFilters.exclude[DirectMissingMethodProblem]("com.invaract.ir.Lineage#Provenance.apply"),
  ProblemFilters.exclude[DirectMissingMethodProblem]("com.invaract.ir.Lineage#Provenance.copy"),
  ProblemFilters.exclude[DirectMissingMethodProblem]("com.invaract.ir.Lineage#Provenance.this"),
  ProblemFilters.exclude[IncompatibleResultTypeProblem]("com.invaract.ir.Lineage#Provenance.copy$default$2"),
  ProblemFilters.exclude[MissingTypesProblem]("com.invaract.ir.Lineage$Provenance$")
)
