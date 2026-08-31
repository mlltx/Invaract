name := "invaract-spark-plugin"
version := "0.1.0"
scalaVersion := "2.12.18"
organization := "com.example"

val sparkVersion = "3.5.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.scalatest" %% "scalatest" % "3.2.18" % "test",
  "org.apache.spark" %% "spark-core" % sparkVersion % "test" classifier "tests",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "test" classifier "tests"
)

// CVE remediation (see docs/CVE_REMEDIATION.md) for two transitive jars
// Spark 3.5.1's own dependency tree resolves (org.apache.avro:avro:1.11.2,
// org.apache.zookeeper:zookeeper:3.6.3 - confirmed via
// `sbt Test/dependencyTree`, since spark-core/spark-sql are `provided`
// here and don't show under Compile/dependencyTree). This module never
// itself calls Avro or ZooKeeper - both come along for the ride as part
// of Spark's own dependency footprint - so this changes only what version
// lands on the test classpath, not this module's own compiled code.
//
// avro 1.11.2 -> 1.11.4: CVE-2024-47561 (GHSA-r7pg-v2c8-mfg3, CVSS 9.3,
// arbitrary code execution when parsing an untrusted Avro schema), fixed
// in 1.11.4/1.12.0; 1.11.4 chosen to stay in Spark 3.5.1's own resolved
// minor line.
// zookeeper 3.6.3 -> 3.9.2: CVE-2023-44981 (authorization bypass when
// SASL Quorum Peer auth is enabled), fixed in 3.7.2/3.8.3/3.9.1+, with
// 3.9.2 one of the advisory's own named recommended patches.
dependencyOverrides ++= Seq(
  "org.apache.avro" % "avro" % "1.11.4",
  "org.apache.zookeeper" % "zookeeper" % "3.9.2",
  // Netty pinned to a single consistent version across every io.netty
  // artifact Spark's own tree resolves here (confirmed via
  // `sbt Test/dependencyTree`) - same coordinate set and reasoning as
  // spark-adapter/build.sbt's override (see its comment for the full
  // detail): 4.1.96.Final is vulnerable to CVE-2025-24970 (SslHandler
  // packet validation, fixed 4.1.118.Final) and CVE-2026-33871 (HTTP/2
  // CONTINUATION-frame flood DoS, fixed 4.1.132.Final); 4.1.132.Final
  // covers both. This module has no Arrow dependency, so none of
  // spark-adapter's PoolArena fragility applies - still pinned as one
  // consistent set rather than per-artifact, to avoid a split-version
  // classpath on principle.
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
  // in 2.5.2. Confirmed via `sbt Test/dependencyTree` that 2.5.1 is this
  // module's actual resolved winner.
  "org.apache.ivy" % "ivy" % "2.5.2"
)

assembly / assemblyJarName := "invaract-spark-plugin-0.1.0.jar"
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

Test / parallelExecution := false

// Spark reflectively accesses JDK-internal classes (e.g.
// sun.nio.ch.DirectBuffer in org.apache.spark.storage.StorageUtils) that
// JDK 17+'s module system closes off by default. spark-submit's own launch
// scripts inject the necessary --add-opens flags automatically for JDK 17+,
// which is why `./dev/test`'s spark-submit-based run needs no changes; a
// plain `sbt test` JVM gets none of that, so it's reproduced explicitly for
// the forked test JVM below. This is Spark's own documented flag set for
// JDK 17+ compatibility (see spark-defaults.conf.template).
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
