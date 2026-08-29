name := "invaract-spark-adapter"
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

// Same test-scope-only reasoning as Delta above - the shaded "runtime" jar
// for exactly this Spark/Scala combination (3.5_2.12), needed only to spin
// up a real Iceberg-enabled session to test against. Checked the
// connector's own issue tracker before pinning, per Phase 0's "any known
// compatibility issues" step: 1.10.0 had a confirmed real bug on this exact
// combination (Avro 1.12 API used against Spark 3.5's bundled Avro 1.11,
// NoSuchMethodError on org.apache.avro.LogicalTypes.timestampNanos -
// apache/iceberg#14232), fixed via #14292 and folded into the Avro-1.12.1
// upgrade that landed before this version - see docs/SPARK_ADAPTER.md's
// Iceberg section for the citation.
val icebergVersion = "1.11.0"

// Hive support, unlike Delta/Iceberg above, is not an external connector
// library - it's Spark's own first-party integration module, split out of
// spark-sql into a separate artifact (`spark-hive`) precisely so a job
// that never touches Hive doesn't need Hive's metastore-client dependency
// footprint on its classpath. Same test-scope-only reasoning applies for
// the same reason: `enableHiveSupport()` needs this to spin up a real
// Hive-enabled session (an embedded Derby metastore, no external Hive
// install needed for local/test use - the same "no external service
// needed" property Iceberg's Hadoop-catalog test setup has), but the
// actual write-command classes this module recognizes
// (`InsertIntoHiveTable`/`CreateHiveTableAsSelectCommand`, both in
// `org.apache.spark.sql.hive.execution`) are matched by reflection/class-
// name string, the same convention `WriteCommandSupport.deltaRowLevelDml`
// uses for Delta's internal command classes - so no compile-time or
// runtime dependency on spark-hive is needed for a job that never enables
// Hive support. Pinned to the exact same sparkVersion as spark-core/
// spark-sql above (not a separately-versioned artifact) - Spark ships
// spark-hive per-Spark-release, not on its own version line the way
// Delta/Iceberg are.
val sparkHiveVersion = sparkVersion

// Avro support, unlike Parquet/CSV (Spark's own bundled FileFormat
// implementations), is a separate first-party artifact Spark splits out
// of spark-sql - a job that never touches Avro doesn't need Avro's own
// (org.apache.avro) dependency footprint on its classpath. Same
// test-scope-only reasoning as spark-hive above: needed only to spin up
// a real Avro-enabled read/write session to test against; the actual
// write-command shape it produces is the exact same generic
// InsertIntoHadoopFsRelationCommand/CreateDataSourceTableAsSelectCommand/
// WriteToStream family already recognized for Parquet/CSV, requiring no
// Avro-specific type in WriteCommandSupport.scala at all. Pinned to the
// exact same sparkVersion as spark-core/spark-sql/spark-hive above -
// spark-avro ships per-Spark-release, not on its own version line the
// way Delta/Iceberg are.
val sparkAvroVersion = sparkVersion

// ClickHouse support, unlike every prior connector, needs a real ClickHouse
// *server* to test against - not just a session extension/embedded
// metastore. Test-scope-only for the same reason as Delta/Iceberg above:
// spinning up a real clickhouse-spark-runtime-backed catalog session to
// test against, never compiled or run by the main translation code.
// Pinned to 0.10.0 - confirmed the latest release on Maven Central for
// exactly this Spark/Scala combination (clickhouse-spark-runtime-3.5_2.12)
// at onboarding time, per Phase 0's "any known compatibility issues" step
// (no blocking issue found against this exact combination). The real
// ClickHouse *server* itself (not this library) is provisioned by
// `ClickHouseTestServer` (test sources) as a standalone binary subprocess,
// not Docker/testcontainers - see that file's own doc comment for why.
val clickhouseVersion = "0.10.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "io.delta" %% "delta-spark" % deltaVersion % "test",
  "org.apache.spark" %% "spark-hive" % sparkHiveVersion % "test",
  "org.apache.spark" %% "spark-avro" % sparkAvroVersion % "test",
  "org.scalatest" %% "scalatest" % "3.2.18" % "test",
  "org.scalatestplus" %% "scalacheck-1-17" % "3.2.18.0" % "test",
  "org.apache.spark" %% "spark-core" % sparkVersion % "test" classifier "tests",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "test" classifier "tests",
  "com.h2database" % "h2" % "2.2.224" % "test"
)

// Not a plain unconditional entry, unlike every other test dependency
// above - see the "iceberg-spark-runtime-3.5_2.12:1.11.0's own classes"
// comment further down for why. Adding these to libraryDependencies at
// all is enough to break JDK 11: Spark's DataSource lookup uses
// ServiceLoader to scan *every* registered DataSourceRegister provider
// on the classpath (to find whichever one matches the requested format),
// which means simply having iceberg-spark-runtime resolvable is enough
// to make *any* .load()/.csv()/format-based read in *any* test suite -
// not just Iceberg-specific ones - try to load org.apache.iceberg.spark.
// IcebergSource and blow up on JDK 11, confirmed via a real CI failure
// (SparkPlanAdapterSpec's plain CSV-fixture read aborted this way).
// Excluding the dependency itself, not just IcebergConnectorSpec's own
// test run, is what actually fixes that - a per-test-class skip alone
// doesn't remove the jar from the classpath the ServiceLoader scans.
libraryDependencies ++= {
  if (scala.util.Properties.isJavaAtLeast("17"))
    Seq(
      "org.apache.iceberg" % "iceberg-spark-runtime-3.5_2.12" % icebergVersion % "test",
      // Confirmed empirically, not assumed: iceberg-spark-runtime-3.5_2.12's
      // SQL extensions parser (IcebergSparkSqlExtensionsParser.isIcebergProcedure,
      // exercised specifically by `CALL <catalog>.system.<proc>(...)` syntax -
      // Iceberg's maintenance-operation mechanism, e.g. rewrite_data_files/
      // expire_snapshots/rollback_to_snapshot) references scala.jdk.CollectionConverters,
      // a class the runtime jar needs but its own published POM doesn't declare
      // as a dependency - a real gap in Iceberg's own artifact for this
      // Spark/Scala combination, not a bug in this module. Needed here only so
      // this module's own test suite can exercise CALL-based Iceberg
      // maintenance ops against a real session; a real Invaract user running
      // Iceberg CALL procedures in their own job would need this on their
      // runtime classpath too, independent of anything spark-adapter does.
      "org.scala-lang.modules" %% "scala-collection-compat" % "2.13.0" % "test"
    )
  else Seq.empty
}

// Same "exclude the dependency itself, not just the test class" reasoning
// as Iceberg's block above, for a different underlying constraint: this
// module's own ClickHouseTestServer provisions a real ClickHouse *server*
// binary with no supported native Windows build (see that file's doc
// comment), not a JDK-version issue. Excluding clickhouse-spark-runtime
// itself on Windows, not just ClickHouseConnectorSpec.scala's own run,
// avoids the same Iceberg-taught risk: Spark's ServiceLoader-based
// DataSourceRegister lookup scans every provider resolvable on the
// classpath for *any* format-based read, so simply having this jar
// resolvable could affect unrelated tests if it behaves at all
// differently on Windows - not observed, but not worth risking given the
// precedent.
libraryDependencies ++= {
  if (scala.util.Properties.isWin) Seq.empty
  else Seq("com.clickhouse.spark" %% "clickhouse-spark-runtime-3.5" % clickhouseVersion % "test")
}

// The ClickHouse connector's .writeTo(...) path serializes batches via
// Arrow (its own bulk-load mechanism, not a generic Spark one - Delta/
// Iceberg's own .writeTo() tests never hit this). Spark 3.5.1 bundles
// arrow-vector/arrow-memory-* 12.0.1 (confirmed via the resolved test
// classpath), which predates a real, external JDK 21 incompatibility:
// JDK 21 changed DirectByteBuffer's private constructor signature from
// (long, int) to (long, long), and Arrow's MemoryUtil.directBuffer()
// reflectively depends on the old one - confirmed via a real
// UnsupportedOperationException on this exact combination, not assumed
// (apache/arrow#35053, fixed in Arrow 13.0.0). Not fixable via
// --add-opens (both java.base/java.nio and jdk.unsupported/sun.misc are
// already open below; the failure is a missing constructor overload, not
// a reflective-access denial). Overridden to 14.0.1 for the *test*
// classpath only - confirmed compatible with Spark 3.5.1's own Arrow use
// elsewhere in this suite (every other spec exercising Arrow-adjacent
// code paths still passes). This does not affect the shipped
// spark-adapter jar's runtime behavior for real users: Arrow itself is
// never a compile/runtime dependency of this module, only pulled in
// transitively by Spark's `provided`/test dependencies.
dependencyOverrides ++= Seq(
  "org.apache.arrow" % "arrow-vector" % "14.0.1",
  "org.apache.arrow" % "arrow-memory-core" % "14.0.1",
  "org.apache.arrow" % "arrow-memory-netty" % "14.0.1"
)

unmanagedJars in Compile += file("../ir/target/scala-2.12/invaract-ir-0.1.0.jar")
unmanagedJars in Compile += file("../contract/target/scala-2.12/invaract-contract-0.1.0.jar")

assembly / assemblyJarName := "invaract-spark-adapter-0.1.0.jar"
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

Test / parallelExecution := false

// iceberg-spark-runtime-3.5_2.12:1.11.0's own classes are compiled to
// class file version 61 (Java 17) - confirmed via CI, not assumed:
// UnsupportedClassVersionError on org.apache.iceberg.spark.SparkCatalog/
// IcebergSource under this repo's JDK-11 test matrix leg
// (.github/workflows/test.yml). A genuine, external constraint of the
// library itself, not something fixable here - unlike Delta 3.2.0 above,
// which loads fine under JDK 11. The dependency itself is excluded under
// JDK <17 above (see that comment for why a per-test-class skip alone
// isn't enough - Spark's ServiceLoader-based DataSourceRegister lookup
// touches every provider on the classpath for *any* format-based read).
// With the dependency gone, IcebergConnectorSpec.scala's own
// org.apache.iceberg/org.apache.spark.sql.connector.iceberg imports
// would fail to *compile* under JDK <17 - so its source file is excluded
// from that build too. Every other spark-adapter source file is
// dependency-free of Iceberg (confirmed by grepping src/ - only this
// file and FailClosedCommands.scala reference it at all, and that one
// only via string literals, never a real import - see its own header
// comment), so nothing else needs excluding. The module's own compiled
// bytecode target (-target:jvm-1.8 below) is unaffected; this is purely
// a test-only dependency's own runtime floor, not a product
// compatibility change.
Test / unmanagedSources / excludeFilter := {
  val icebergExcluded =
    if (scala.util.Properties.isJavaAtLeast("17")) (Test / unmanagedSources / excludeFilter).value
    else (Test / unmanagedSources / excludeFilter).value || "IcebergConnectorSpec.scala"
  // ClickHouse has no supported native Windows server build (a hard
  // platform constraint, unlike Iceberg's JDK-version one above) -
  // ClickHouseTestServer/ClickHouseConnectorSpec.scala are excluded on
  // Windows only. Every other test file is dependency-free of ClickHouse
  // (only these two reference it), so nothing else needs excluding.
  if (scala.util.Properties.isWin)
    icebergExcluded || "ClickHouseTestServer.scala" || "ClickHouseConnectorSpec.scala" || "ClickHouseConnectorProbeSpec.scala"
  else icebergExcluded
}

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
  "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
  // ClickHouse's Spark connector serializes writes via Apache Arrow
  // (ClickHouseArrowStreamWriter), whose MemoryUtil needs reflective
  // access to sun.misc.Unsafe/DirectByteBuffer's package-private
  // constructor - confirmed empirically (a real
  // UnsupportedOperationException on this forked JDK 21 test JVM, not
  // assumed): sun.misc.Unsafe lives in the jdk.unsupported module, which
  // none of the java.base opens above reach.
  "--add-opens=jdk.unsupported/sun.misc=ALL-UNNAMED"
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
//
// Transitional: renamed invariant-spark-adapter -> invaract-spark-adapter in
// the same commit that changed `name :=` above - see contract/build.sbt's
// comment on why this has to keep saying "invariant-spark-adapter" until this
// PR is on the base branch.
mimaPreviousArtifacts := Set("com.example" %% "invariant-spark-adapter" % "0.1.0")
