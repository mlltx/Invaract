name := "invaract-spark-runner"
version := "0.1.0"
scalaVersion := "2.12.18"
organization := "com.example"

val sparkVersion = "3.5.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
)

// CVE remediation (see docs/CVE_REMEDIATION.md). Unlike plugin/spark-adapter,
// spark-core/spark-sql are compile-scope here (unscoped, not `provided`) -
// this is the one module where DemoJobHarness's own `assembly` jar
// (invaract-spark-runner.jar) actually bundles Spark's full transitive
// tree, so these overrides change what a real `spark-submit` of that jar
// puts on the classpath, not just what this module's tests run against.
//
// avro 1.11.2 -> 1.11.4: CVE-2024-47561 (GHSA-r7pg-v2c8-mfg3, CVSS 9.3,
// arbitrary code execution when parsing an untrusted Avro schema), fixed
// in 1.11.4/1.12.0; 1.11.4 chosen to stay in Spark 3.5.1's own resolved
// minor line (org.apache.avro:avro:1.11.2, confirmed via
// `sbt Compile/dependencyTree`).
// zookeeper 3.6.3 -> 3.9.2: CVE-2023-44981 (authorization bypass when
// SASL Quorum Peer auth is enabled), fixed in 3.7.2/3.8.3/3.9.1+, with
// 3.9.2 one of the advisory's own named recommended patches.
dependencyOverrides ++= Seq(
  "org.apache.avro" % "avro" % "1.11.4",
  "org.apache.zookeeper" % "zookeeper" % "3.9.2"
)

unmanagedJars in Compile += file("../plugin/target/scala-2.12/invaract-spark-plugin-0.1.0.jar")
unmanagedJars in Compile += file("../ir/target/scala-2.12/invaract-ir-0.1.0.jar")
unmanagedJars in Compile += file("../spark-adapter/target/scala-2.12/invaract-spark-adapter-0.1.0.jar")
unmanagedJars in Compile += file("../contract/target/scala-2.12/invaract-contract-0.1.0.jar")

assembly / assemblyJarName := "invaract-spark-runner.jar"
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

scalacOptions ++= Seq(
  "-target:jvm-1.8",
  "-deprecation",
  "-feature"
)
