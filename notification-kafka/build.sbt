name := "invaract-notification-kafka"
version := "0.2.0"
scalaVersion := "2.12.18"
organization := "com.invaract"

// A real, unscoped dependency of this module only - unlike Delta/Iceberg/
// Hive/Avro/ClickHouse in spark-adapter (all test-scope only, matched by
// reflection at runtime), kafka-clients' producer API is broad enough that
// reflecting the whole thing would be unidiomatic, so this sink is
// compiled directly against it. This is exactly why it lives in its own
// module rather than inside spark-adapter itself: a user who never
// configures a Kafka sink never resolves kafka-clients at all, since
// spark-adapter's own build.sbt declares no dependency on it whatsoever -
// a stronger guarantee than even the test-scoped connector dependencies
// get (those still get resolved to build/test spark-adapter itself).
libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka-clients" % "3.9.2",
  "org.scalatest" %% "scalatest" % "3.2.18" % "test"
)

// at.yawk.lz4:lz4-java: kafka-clients declares it as a runtime-scope
// dependency (3.9.2 -> 1.10.1), and an advisory affects <= 1.11.0 (fixed in
// 1.11.1): insufficient validation of the array/offset/length arguments of the
// JNI-based XXHash methods lets a caller that controls those arguments crash
// the JVM. Dependency route, in the order docs/CVE_REMEDIATION.md §1 gives:
//   1. Remove - not possible: it is kafka-clients' own transitive dependency,
//      and the sink passes producer properties straight through, so a user who
//      sets compression.type=lz4 needs it at runtime. Excluding it would turn
//      that setting into a NoClassDefFoundError.
//   2. Bump the direct dependency - no kafka-clients release helps: 3.9.2 is
//      the last 3.x, 4.0.2 and 4.1.2 still declare 1.10.1, and the newest
//      (4.3.1) declares 1.10.2, all <= 1.11.0 (checked against each release's
//      POM on Maven Central). 4.x is also a major-version move for a sink that
//      only needs the producer.
//   3. Pin - this. The same coordinate and version spark-adapter already pins
//      (test scope) for its own lz4 alert. Same net.jpountz.* classes
//      (LZ4Factory, LZ4Compressor, LZ4SafeDecompressor, XXHashFactory,
//      XXHash32 all present, Java 7 class files, checked in the jar), so
//      kafka-clients' LZ4 codec is unaffected.
// A dependencyOverrides entry rather than a direct dependency: lz4-java must
// stay a runtime-only transitive, not become a compile dependency of this
// module's own code.
//
// 1.11.1 -> 1.11.4: four more advisories against this same jar since -
// see spark-adapter/build.sbt's comment for the full detail, including
// why 1.11.4 (every fix, verified drop-in) rather than 1.12.0 (the
// maintainer's own notes flag extra validation tightening that "could
// in theory break some users").
dependencyOverrides += "at.yawk.lz4" % "lz4-java" % "1.11.4"

scalacOptions ++= Seq(
  "-target:jvm-1.8",
  "-deprecation",
  "-feature"
)

// spark-adapter's own assembly jar already bundles ir/contract's compiled
// classes (see runner/build.sbt's identical pattern for the precedent) -
// NotificationSink/NotificationEvent/Violation are reachable through it
// with no separate ir/contract jar needed here, and critically, no Spark
// dependency at all: spark-core/spark-sql are `provided` scope in
// spark-adapter's own build.sbt, so they're excluded from its assembly
// jar - this module needs no Spark on its classpath either, at compile or
// runtime.
//
// This filename must track spark-adapter/build.sbt's own current
// `assembly / assemblyJarName` exactly, the same "versions must track the
// producing module's own version :=" invariant runner/build.sbt's matching
// libraryDependencies comment already states for its own real Maven
// coordinate - confirmed the hard way: a spark-adapter version bump
// (0.8.0 -> 0.9.0, WriteEvent.datasetType) that updated assemblyJarName
// but not this literal path broke this module's own compile with "object
// NotificationEvent is not a member of package
// com.invaract.verification.notification" (the jar unmanagedJars pointed
// at simply didn't exist on disk under the old name), caught by this
// module's own CI job, not assumed fixed.
unmanagedJars in Compile += file("../spark-adapter/target/scala-2.12/invaract-spark-adapter-0.11.0.jar")

assembly / assemblyJarName := "invaract-notification-kafka-0.2.0.jar"
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

// Not part of the verification engine itself (contract/ir/spark-adapter) -
// an optional extension a real user opts into by adding this module's
// assembled jar to their classpath, the same relationship plugin/runner
// have to the engine. Mutation testing/MiMa (CLAUDE.md's guardrails,
// scoped explicitly to contract/ir/spark-adapter) are not required here
// for the same reason they aren't for plugin/runner - real example-based
// tests against Kafka's own MockProducer are the bar instead (see
// KafkaNotificationSinkSpec).
