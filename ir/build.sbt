import com.typesafe.tools.mima.core._

name := "invaract-ir"
version := "0.1.0"
scalaVersion := "2.12.18"
organization := "com.example"

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.2.18" % "test"
)

scalacOptions ++= Seq(
  "-target:jvm-1.8",
  "-deprecation",
  "-feature"
)

assembly / assemblyJarName := "invaract-ir-0.1.0.jar"

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
// Renamed invariant-ir -> invaract-ir by the rebrand PR, which is now on the
// base branch - see contract/build.sbt's comment for why this coordinate
// must match base-ref's own published name.
mimaPreviousArtifacts := Set("com.example" %% "invaract-ir" % "0.1.0")

// A deliberate, documented MAJOR-version API break (see CLAUDE.md's "API
// Compatibility Requirement"): the "Spark Logical Plan -> Invariant IR"
// expression-level rework split the single `FunctionCall` node into
// `Cast`/`Arithmetic`/`Comparison`/`BooleanExpr`/`Conditional`/`Function`/
// `UDF`/`Alias`, renamed `Unsupported`/`UnsupportedExpr` to `UnknownPlan`/
// `UnknownExpression` (adding a `sourceType` field to each), and added an
// optional `id` field to `ColumnRef` for translator-assigned column
// identity - see docs/TRANSFORMATION_IR.md's "Expression algebra, v2" for
// the full rationale. Every line below is exactly what
// `sbt mimaReportBinaryIssues` reported once this change compiled, run
// against a local `publishLocal` of the module as it stood on `main` before
// this change - not guessed.
mimaBinaryIssueFilters ++= Seq(
  ProblemFilters.exclude[DirectMissingMethodProblem]("com.example.ir.ColumnRef.apply"),
  ProblemFilters.exclude[DirectMissingMethodProblem]("com.example.ir.ColumnRef.copy"),
  ProblemFilters.exclude[DirectMissingMethodProblem]("com.example.ir.ColumnRef.this"),
  ProblemFilters.exclude[MissingTypesProblem]("com.example.ir.ColumnRef$"),
  ProblemFilters.exclude[MissingClassProblem]("com.example.ir.FunctionCall"),
  ProblemFilters.exclude[MissingClassProblem]("com.example.ir.FunctionCall$"),
  ProblemFilters.exclude[MissingClassProblem]("com.example.ir.Unsupported"),
  ProblemFilters.exclude[MissingClassProblem]("com.example.ir.Unsupported$"),
  ProblemFilters.exclude[MissingClassProblem]("com.example.ir.UnsupportedExpr"),
  ProblemFilters.exclude[MissingClassProblem]("com.example.ir.UnsupportedExpr$")
)
