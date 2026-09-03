name := "invaract-spark-plugin"
version := "0.2.0"
scalaVersion := "2.12.18"
organization := "com.invaract"

// 3.5.1 -> 3.5.7: CVE-2025-54920 (Spark History Server Code Execution,
// a Direct dependency, not transitive - no dependencyOverrides
// workaround for a bug in Spark's own code) - see spark-adapter/build.sbt's
// comment for the full detail, including confirming Spark 3.5.7's own
// POM still declares the same jackson-module-scala:2.15.2 as 3.5.1 does,
// so this doesn't reopen that module's Netty->Arrow->Jackson conflict
// class.
val sparkVersion = "3.5.7"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.scalatest" %% "scalatest" % "3.2.18" % "test",
  "org.apache.spark" %% "spark-core" % sparkVersion % "test" classifier "tests",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "test" classifier "tests",
  // org.lz4:lz4-java is unmaintained (upstream archived) and vulnerable
  // to CVE-2025-12183 and CVE-2025-66566 - see spark-adapter/build.sbt's
  // comment for the full detail (Maven Central's own relocation POM for
  // org.lz4:lz4-java:1.8.1 points at this fork; added directly at 1.11.1
  // rather than relying on Ivy to follow the relocation). Same
  // net.jpountz.lz4 package namespace, so Spark's shuffle-compression
  // code needs no changes; org.lz4 excluded below.
  "at.yawk.lz4" % "lz4-java" % "1.11.1" % "test"
)
excludeDependencies += ExclusionRule("org.lz4", "lz4-java")

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
  // 3.6.3 -> 3.9.5 (not just 3.9.2): CVE-2023-44981 (SASL Quorum Peer auth
  // bypass, fixed 3.9.1+) plus two more found in a later alert batch,
  // CVE-2026-24308 (ZKConfig logs configuration values including
  // potential credentials at INFO level, fixed 3.9.5) and CVE-2024-51504
  // (Admin Server's IPAuthenticationProvider trusts a spoofable
  // X-Forwarded-For header, fixed 3.9.3). 3.9.5 covers all three.
  "org.apache.zookeeper" % "zookeeper" % "3.9.5",
  // Netty pinned to a single consistent version across every io.netty
  // artifact Spark's own tree resolves here (confirmed via
  // `sbt Test/dependencyTree`) - same coordinate set and reasoning as
  // spark-adapter/build.sbt's override (see its comment for the full
  // detail, including two later alert batches that found five more
  // CVEs fixed at or below 4.1.137.Final): 4.1.96.Final was vulnerable
  // to CVE-2025-24970, CVE-2026-33871, CVE-2025-55163, CVE-2026-44249,
  // two ByteBuf-leak/infinite-loop bugs in SpdyHttpDecoder/Bzip2Decoder,
  // and CVE-2026-59903 (netty-codec-http's CorsHandler silently
  // overwrites an application's own Vary header with Vary: Origin,
  // enabling cache poisoning/cross-user response disclosure); 4.1.137.Final
  // is the highest of all seven fix floors. This module has no Arrow
  // dependency, so none of spark-adapter's PoolArena fragility applies -
  // still pinned as one consistent set rather than per-artifact, to
  // avoid a split-version classpath on principle.
  "io.netty" % "netty-all" % "4.1.137.Final",
  "io.netty" % "netty-buffer" % "4.1.137.Final",
  "io.netty" % "netty-codec" % "4.1.137.Final",
  "io.netty" % "netty-codec-http" % "4.1.137.Final",
  "io.netty" % "netty-codec-http2" % "4.1.137.Final",
  "io.netty" % "netty-codec-socks" % "4.1.137.Final",
  "io.netty" % "netty-common" % "4.1.137.Final",
  "io.netty" % "netty-handler" % "4.1.137.Final",
  "io.netty" % "netty-handler-proxy" % "4.1.137.Final",
  "io.netty" % "netty-resolver" % "4.1.137.Final",
  "io.netty" % "netty-transport" % "4.1.137.Final",
  "io.netty" % "netty-transport-classes-epoll" % "4.1.137.Final",
  "io.netty" % "netty-transport-classes-kqueue" % "4.1.137.Final",
  "io.netty" % "netty-transport-native-epoll" % "4.1.137.Final",
  "io.netty" % "netty-transport-native-kqueue" % "4.1.137.Final",
  "io.netty" % "netty-transport-native-unix-common" % "4.1.137.Final",
  // CVE-2022-46751 (GHSA-hedq-r4mx-jhh8, XXE in Ivy's XML parsing), fixed
  // in 2.5.2. Confirmed via `sbt Test/dependencyTree` that 2.5.1 is this
  // module's actual resolved winner.
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
  // async parser). This module has no Arrow dependency and never hit
  // spark-adapter's Netty->Arrow->Jackson regression chain, so there was
  // no pre-existing override here to update - this adds one directly at
  // the fixed version. Confirmed via dependencyTree that this module
  // resolves the same vulnerable jackson-databind:2.15.2 spark-adapter
  // did before its own fix, even though this specific alert batch only
  // named spark-adapter/build.sbt.
  "com.fasterxml.jackson.core" % "jackson-core" % "2.18.9",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.18.9",
  "com.fasterxml.jackson.core" % "jackson-annotations" % "2.18.9",
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.18.9",
  // log4j-core/log4j-api/log4j-1.2-api/log4j-slf4j2-impl, 2.20.0 -> 2.25.5
  // - see spark-adapter/build.sbt's comment for the full detail on all
  // four CVEs fixed (CVE-2025-68161, CVE-2026-34477, CVE-2026-34480/
  // 34479, CVE-2026-49844) and why all four artifacts move together.
  // Confirmed via dependencyTree that this module resolves the same
  // vulnerable 2.20.0 spark-adapter did, even though this alert batch
  // only named spark-adapter/build.sbt.
  "org.apache.logging.log4j" % "log4j-core" % "2.25.5",
  "org.apache.logging.log4j" % "log4j-api" % "2.25.5",
  "org.apache.logging.log4j" % "log4j-1.2-api" % "2.25.5",
  "org.apache.logging.log4j" % "log4j-slf4j2-impl" % "2.25.5"
)

// NOT overridden - com.google.guava:guava:16.0.1, same two CVEs and same
// accepted-risk reasoning as spark-adapter/build.sbt's own comment (see
// there for the full detail): CVE-2018-10237 and CVE-2020-8908, both
// arriving via org.apache.curator:curator-client:2.13.0 (confirmed via
// `sbt Test/dependencyTree`), which backs Spark's ZooKeeper-based
// standalone-cluster recovery mode - infrastructure this module's own
// `local[*]`-only tests never configure or exercise, so a passing suite
// here couldn't prove a bump safe either.

assembly / assemblyJarName := "invaract-spark-plugin-0.2.0.jar"
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
