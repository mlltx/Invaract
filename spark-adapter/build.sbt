name := "invariant-spark-adapter"
version := "0.1.0"
scalaVersion := "2.12.18"
organization := "com.example"

val sparkVersion = "3.5.1"

// Test-scope only, not provided: empirical investigation (see
// docs/SPARK_ADAPTER.md's "Delta Lake support" section) found that Delta
// writes go through Spark's own generic SaveIntoDataSourceCommand +
// DataSourceRegister - both plain, public spark-sql classes already on
// the `provided` Spark dependency above. Translating them needs no
// Delta-specific type at all, so delta-spark is only needed here to spin
// up a real Delta-enabled session to test against (the same role
// com.h2database plays for the JDBC precedent below), never to compile
// or run the main translation code.
// 3.2.0, not the latest 3.x release: a confirmed real bug in 3.2.1 affects
// exactly this combination (Scala 2.12 + Spark 3.5.1) - see
// docs/SPARK_ADAPTER.md's Delta section for the citation.
val deltaVersion = "3.2.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "io.delta" %% "delta-spark" % deltaVersion % "test",
  "org.scalatest" %% "scalatest" % "3.2.18" % "test",
  "org.scalatestplus" %% "scalacheck-1-17" % "3.2.18.0" % "test",
  "org.apache.spark" %% "spark-core" % sparkVersion % "test" classifier "tests",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "test" classifier "tests",
  "com.h2database" % "h2" % "2.2.224" % "test"
)

unmanagedJars in Compile += file("../ir/target/scala-2.12/invariant-ir-0.1.0.jar")
unmanagedJars in Compile += file("../contract/target/scala-2.12/invariant-contract-0.1.0.jar")

assembly / assemblyJarName := "invariant-spark-adapter-0.1.0.jar"
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

Test / parallelExecution := false

// Spark reflectively accesses JDK-internal classes (e.g.
// sun.nio.ch.DirectBuffer in org.apache.spark.storage.StorageUtils) that
// JDK 17+'s module system closes off by default. spark-submit's own launch
// scripts inject the necessary --add-opens flags automatically for JDK 17+,
// which is why `./dev/test`'s real spark-submit run needs no changes here;
// a plain `sbt test` JVM gets none of that, so it's reproduced explicitly
// for the forked test JVM below. This is Spark's own documented flag set
// for JDK 17+ compatibility (see spark-defaults.conf.template).
Test / fork := true
Test / javaOptions ++= Seq(
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
  "--add-opens=java.base/java.io=ALL-UNNAMED",
  "--add-opens=java.base/java.net=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
  "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
  "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
)

scalacOptions ++= Seq(
  "-target:jvm-1.8",
  "-deprecation",
  "-feature"
)

// Mutation testing (Stryker4s) config: see stryker4s.conf for reporters.
// `mutate`/`thresholds` are set here rather than in stryker4s.conf, whose
// equivalent keys were observed not to take effect via the config file in
// this sbt/plugin version combination - these sbt settings do work.
// Whole-module scope (widened from just StructuralVerifier.scala once the
// initial narrow pass's score was reviewed). `break` is what makes CI's
// mutation-testing job fail when the score regresses below it.
strykerMutate := Seq("src/main/scala/**/*.scala")

// After genuine test uplift closed every real (non-StringLiteral) gap
// this module's coverage tooling can reach, the only mutants still
// surviving are ~80 StringLiteral mutants on human-readable
// message/remediation/type-name text (see docs/SPARK_ADAPTER.md's
// "Mutation testing" section) - the category CLAUDE.md's "Mutation
// Testing Requirement" already names as a documented, acceptable
// exclusion, since a test asserting an exact error-message string is
// brittle and doesn't verify behavior. Excluding it here makes that
// exclusion explicit and repo-wide instead of an ad hoc per-PR judgment
// call, and lets the break threshold reflect the module's real behavioral
// mutation coverage rather than being capped by unrelated prose.
strykerExcludedMutations := Seq("StringLiteral")

// Real measured score after the exclusion above is 91.53% (of total) /
// 93.1% (of covered code) - 54/59 mutants killed, the same 5 documented,
// left-on-purpose survivors as before (JDBCRelation near-equivalence,
// unwrapWriteWrapper's Spark-3.5.1-unreachable branch, and the
// Hive-relation fallback with no metastore available to exercise it
// here). Thresholds below match the incremental PR check's values
// (.github/workflows/test.yml) rather than hugging 91.53% exactly, so a
// small, explainable regression doesn't fail CI outright.
strykerThresholdsHigh := 90
strykerThresholdsLow := 80
strykerThresholdsBreak := 70

// API compatibility (MiMa) - see contract/build.sbt's comment for the full
// rationale (no Maven Central release yet, so CI's `api-compatibility` job
// compares against the PR's own base branch instead) and
// docs/SPARK_ADAPTER.md's "API compatibility" section.
mimaPreviousArtifacts := Set("com.example" %% "invariant-spark-adapter" % "0.1.0")
