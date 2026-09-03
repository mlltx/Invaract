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
  "org.apache.kafka" % "kafka-clients" % "3.8.0",
  "org.scalatest" %% "scalatest" % "3.2.18" % "test"
)

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
unmanagedJars in Compile += file("../spark-adapter/target/scala-2.12/invaract-spark-adapter-0.2.0.jar")

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
