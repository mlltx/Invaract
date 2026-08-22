name := "invariant-contract"
version := "0.1.0"
scalaVersion := "2.12.18"
organization := "com.example"

libraryDependencies ++= Seq(
  "org.yaml" % "snakeyaml" % "2.2",
  "org.scalatest" %% "scalatest" % "3.2.18" % "test",
  // Validates contract/schema/invariant-contract.schema.json against real
  // fixtures (ContractSchemaSpec) - test-scoped only. The schema is a
  // static artifact for external tooling to bind to; nothing in the
  // contract module's own runtime parses YAML against it (ContractParser/
  // ContractValidator remain the authoritative implementation).
  "com.networknt" % "json-schema-validator" % "1.4.1" % "test"
)

scalacOptions ++= Seq(
  "-target:jvm-1.8",
  "-deprecation",
  "-feature"
)

assembly / assemblyJarName := "invariant-contract-0.1.0.jar"
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
