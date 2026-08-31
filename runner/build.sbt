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
  "org.apache.zookeeper" % "zookeeper" % "3.9.2",
  // Netty pinned to a single consistent version across every io.netty
  // artifact Spark's own tree resolves here (confirmed via
  // `sbt Compile/dependencyTree`) - same coordinate set and reasoning as
  // spark-adapter/build.sbt's override (see its comment for the full
  // detail): 4.1.96.Final is vulnerable to CVE-2025-24970 (SslHandler
  // packet validation, fixed 4.1.118.Final) and CVE-2026-33871 (HTTP/2
  // CONTINUATION-frame flood DoS, fixed 4.1.132.Final); 4.1.132.Final
  // covers both. This is the one module (see the note above) where the
  // fix actually changes what ships in invaract-spark-runner.jar, not
  // just this module's own test classpath.
  "io.netty" % "netty-all" % "4.1.132.Final",
  "io.netty" % "netty-buffer" % "4.1.132.Final",
  "io.netty" % "netty-codec" % "4.1.132.Final",
  "io.netty" % "netty-codec-http" % "4.1.132.Final",
  "io.netty" % "netty-codec-http2" % "4.1.132.Final",
  "io.netty" % "netty-codec-socks" % "4.1.132.Final",
  "io.netty" % "netty-common" % "4.1.132.Final",
  "io.netty" % "netty-handler" % "4.1.132.Final",
  "io.netty" % "netty-handler-proxy" % "4.1.132.Final",
  "io.netty" % "netty-resolver" % "4.1.132.Final",
  "io.netty" % "netty-transport" % "4.1.132.Final",
  "io.netty" % "netty-transport-classes-epoll" % "4.1.132.Final",
  "io.netty" % "netty-transport-classes-kqueue" % "4.1.132.Final",
  "io.netty" % "netty-transport-native-epoll" % "4.1.132.Final",
  "io.netty" % "netty-transport-native-kqueue" % "4.1.132.Final",
  "io.netty" % "netty-transport-native-unix-common" % "4.1.132.Final",
  // CVE-2022-46751 (GHSA-hedq-r4mx-jhh8, XXE in Ivy's XML parsing), fixed
  // in 2.5.2. Confirmed via `sbt Compile/dependencyTree` that 2.5.1 is
  // this module's actual resolved winner.
  "org.apache.ivy" % "ivy" % "2.5.2"
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
