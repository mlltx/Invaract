name := "invariant-spark-runner"
version := "0.1.0"
scalaVersion := "2.12.18"
organization := "com.example"

val sparkVersion = "3.5.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
)

unmanagedJars in Compile += file("../plugin/target/scala-2.12/invariant-spark-plugin-0.1.0.jar")
unmanagedJars in Compile += file("../ir/target/scala-2.12/invariant-ir-0.1.0.jar")
unmanagedJars in Compile += file("../spark-adapter/target/scala-2.12/invariant-spark-adapter-0.1.0.jar")

assembly / assemblyJarName := "invariant-spark-runner.jar"
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

scalacOptions ++= Seq(
  "-target:jvm-1.8",
  "-deprecation",
  "-feature"
)
