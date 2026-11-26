// lineage-crosscheck: holds Invaract's Spark lineage up against two independent tools, on real Spark jobs.
//
// Each job in the corpus (src/test/scala/.../Corpus.scala) runs once, in a fresh SparkSession that has all three
// capturing lineage at the same time:
//   - Invaract (spark-adapter, attached by configuration, publishing OpenLineage to a file);
//   - openlineage-spark, OpenLineage's own Spark integration (the reference for dataset naming and for the
//     transformation types of the OpenLineage column lineage facet);
//   - Spline's Spark agent (an independent model of the analyzed plan: every written attribute traced back to the
//     attributes of the reads it comes from).
// Their output is reduced to one edge model and compared; a difference is either fixed in Invaract or recorded,
// with its reason, in crosscheck-baseline.txt. docs-site's "Spark Column Lineage Coverage" page is generated from
// the result. See docs/OPENLINEAGE.md, "Cross-checking Spark lineage".
//
// Not published, not part of any runtime jar: the two other agents are test dependencies of this module only, so
// they never reach spark-adapter's classpath (or its mutation runs). Built by its own CI job, against the engine
// modules published locally (like adapter-testkit's dependents).
name := "invaract-lineage-crosscheck"
version := "0.1.0"
scalaVersion := "2.12.18"
organization := "com.invaract"

resolvers += Resolver.mavenLocal

// The versions spark-adapter is built and tested against, from the one file that names them.
def supported(key: String): String = {
  val props = new java.util.Properties()
  val in = new java.io.FileInputStream(file("../spark-adapter/src/main/resources/supported-versions.properties"))
  try props.load(in) finally in.close()
  props.getProperty(key)
}
val sparkVersion = supported("spark.primary")
val deltaVersion = supported("delta.primary")
val icebergVersion = supported("iceberg.primary")

// openlineage-spark 1.55.0 is the release whose spec (2-0-2) verification-core's schema tests vendor; Spline's
// agent bundle 2.4.0 is the newest release for Spark 3.5.
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion % "test",
  "org.apache.spark" %% "spark-hive" % sparkVersion % "test",
  "io.delta" %% "delta-spark" % deltaVersion % "test",
  "org.apache.iceberg" % "iceberg-spark-runtime-3.5_2.12" % icebergVersion % "test",
  "com.h2database" % "h2" % "2.2.224" % "test",
  "com.invaract" %% "invaract-spark-adapter" % "0.11.0" % "test",
  "io.openlineage" %% "openlineage-spark" % "1.55.0" % "test",
  "za.co.absa.spline.agent.spark" %% "spark-3.5-spline-agent-bundle" % "2.4.0" % "test",
  "org.scalatest" %% "scalatest" % "3.2.18" % "test"
)

// One JVM per run, the corpus in order: each job builds and stops its own SparkSession.
Test / fork := true
Test / parallelExecution := false
Test / javaOptions ++= Seq(
  "-Xmx2g",
  "-XX:+UseSerialGC",
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
  // Spline's agent closes the jar it reads its defaults from through this package
  "--add-opens=java.base/sun.net.www.protocol.jar=ALL-UNNAMED"
)
Test / envVars ++= sys.env.get("CROSSCHECK_UPDATE").map(v => Map("CROSSCHECK_UPDATE" -> v)).getOrElse(Map.empty)

scalacOptions ++= Seq("-deprecation", "-feature")
