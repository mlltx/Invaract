name := "invariant-contract"
version := "0.1.0"
scalaVersion := "2.12.18"
organization := "com.example"

libraryDependencies ++= Seq(
  "org.yaml" % "snakeyaml" % "2.2",
  "org.scalatest" %% "scalatest" % "3.2.18" % "test"
)

scalacOptions ++= Seq(
  "-target:jvm-1.8",
  "-deprecation",
  "-feature"
)

Test / parallelExecution := false

assembly / assemblyJarName := "invariant-contract-0.1.0.jar"
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
