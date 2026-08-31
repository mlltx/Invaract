name := "invaract-spark-runner"
version := "0.1.0"
scalaVersion := "2.12.18"
organization := "com.example"

// 3.5.1 -> 3.5.7: CVE-2025-54920 (Spark History Server Code Execution,
// a Direct dependency, not transitive - no dependencyOverrides
// workaround for a bug in Spark's own code) - see spark-adapter/build.sbt's
// comment for the full detail, including confirming Spark 3.5.7's own
// POM still declares the same jackson-module-scala:2.15.2 as 3.5.1 does,
// so this doesn't reopen that module's Netty->Arrow->Jackson conflict
// class. This is the one module where the fix actually changes what
// ships in invaract-spark-runner.jar (compile-scope spark-core/spark-sql
// here, unlike plugin/spark-adapter's provided scope).
val sparkVersion = "3.5.7"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  // org.lz4:lz4-java is unmaintained (upstream archived) and vulnerable
  // to CVE-2025-12183 and CVE-2025-66566 - see spark-adapter/build.sbt's
  // comment for the full detail (Maven Central's own relocation POM for
  // org.lz4:lz4-java:1.8.1 points at this fork; added directly at 1.11.1
  // rather than relying on Ivy to follow the relocation). Same
  // net.jpountz.lz4 package namespace, so Spark's shuffle-compression
  // code needs no changes; org.lz4 excluded below. Unscoped (not `test`),
  // matching spark-core/spark-sql above - this module's compile-scope
  // deps are what invaract-spark-runner.jar's assembly actually bundles.
  "at.yawk.lz4" % "lz4-java" % "1.11.1"
)
excludeDependencies += ExclusionRule("org.lz4", "lz4-java")

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
  // 3.6.3 -> 3.9.5 (not just 3.9.2): CVE-2023-44981 (SASL Quorum Peer auth
  // bypass, fixed 3.9.1+) plus two more found in a later alert batch,
  // CVE-2026-24308 (ZKConfig logs configuration values including
  // potential credentials at INFO level, fixed 3.9.5) and CVE-2024-51504
  // (Admin Server's IPAuthenticationProvider trusts a spoofable
  // X-Forwarded-For header, fixed 3.9.3). 3.9.5 covers all three.
  "org.apache.zookeeper" % "zookeeper" % "3.9.5",
  // Netty pinned to a single consistent version across every io.netty
  // artifact Spark's own tree resolves here (confirmed via
  // `sbt Compile/dependencyTree`) - same coordinate set and reasoning as
  // spark-adapter/build.sbt's override (see its comment for the full
  // detail, including a later alert batch that found four more CVEs
  // fixed at or below 4.1.136.Final): 4.1.96.Final was vulnerable to
  // CVE-2025-24970, CVE-2026-33871, CVE-2025-55163, CVE-2026-44249, and
  // two ByteBuf-leak/infinite-loop bugs in SpdyHttpDecoder/Bzip2Decoder;
  // 4.1.136.Final is the highest of all six fix floors. This is the one
  // module (see the note above) where the fix actually changes what
  // ships in invaract-spark-runner.jar, not just this module's own test
  // classpath.
  "io.netty" % "netty-all" % "4.1.136.Final",
  "io.netty" % "netty-buffer" % "4.1.136.Final",
  "io.netty" % "netty-codec" % "4.1.136.Final",
  "io.netty" % "netty-codec-http" % "4.1.136.Final",
  "io.netty" % "netty-codec-http2" % "4.1.136.Final",
  "io.netty" % "netty-codec-socks" % "4.1.136.Final",
  "io.netty" % "netty-common" % "4.1.136.Final",
  "io.netty" % "netty-handler" % "4.1.136.Final",
  "io.netty" % "netty-handler-proxy" % "4.1.136.Final",
  "io.netty" % "netty-resolver" % "4.1.136.Final",
  "io.netty" % "netty-transport" % "4.1.136.Final",
  "io.netty" % "netty-transport-classes-epoll" % "4.1.136.Final",
  "io.netty" % "netty-transport-classes-kqueue" % "4.1.136.Final",
  "io.netty" % "netty-transport-native-epoll" % "4.1.136.Final",
  "io.netty" % "netty-transport-native-kqueue" % "4.1.136.Final",
  "io.netty" % "netty-transport-native-unix-common" % "4.1.136.Final",
  // CVE-2022-46751 (GHSA-hedq-r4mx-jhh8, XXE in Ivy's XML parsing), fixed
  // in 2.5.2. Confirmed via `sbt Compile/dependencyTree` that 2.5.1 is
  // this module's actual resolved winner.
  "org.apache.ivy" % "ivy" % "2.5.2",
  // 0.25 -> 2.0.3: CVE-2024-36114 (Unsafe-based OOB access) plus
  // CVE-2025-67721 (reused-output-buffer leak in Snappy/LZ4, still
  // present at 0.27) - see spark-adapter/build.sbt's comment for the
  // full detail, including the jar-level check that ruled out a
  // Derby/Thrift-style repackaging break across this version jump.
  "io.airlift" % "aircompressor" % "2.0.3",
  // 3.12.0 -> 3.18.0: CVE-2025-48924 (GHSA-j288-q9x7-2f5v) -
  // ClassUtils.getClass(...) recurses without a depth limit, StackOverflowError
  // on a long enough class-name input.
  "org.apache.commons" % "commons-lang3" % "3.18.0",
  // 1.1.10.3 -> 1.1.10.4: CVE-2023-43642 (GHSA-55g7-9cwv-5qfv) -
  // SnappyInputStream has no upper bound on the declared chunk length,
  // so a crafted input can force an oversized heap allocation.
  "org.xerial.snappy" % "snappy-java" % "1.1.10.4",
  // 2.15.2 -> 2.18.8 (jackson-core/databind/annotations and
  // jackson-module-scala, moved together - see spark-adapter/build.sbt's
  // comment for the full detail, including why these four have to move
  // as one unit): CVE-2026-54512 and CVE-2026-54513 (two
  // PolymorphicTypeValidator bypasses in jackson-databind) plus
  // GHSA-r7wm-3cxj-wff9 (an incomplete-fix follow-up in jackson-core's
  // async parser). This is the one module where the fix actually changes
  // what ships in invaract-spark-runner.jar, not just this module's own
  // test classpath (same note as the other overrides above).
  "com.fasterxml.jackson.core" % "jackson-core" % "2.18.8",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.18.8",
  "com.fasterxml.jackson.core" % "jackson-annotations" % "2.18.8",
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.18.8"
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
