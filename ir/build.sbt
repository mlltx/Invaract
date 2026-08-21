name := "invariant-ir"
version := "0.1.0"
scalaVersion := "2.12.18"
organization := "com.example"

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.2.18" % "test"
)

scalacOptions ++= Seq(
  "-target:jvm-1.8",
  "-deprecation",
  "-feature"
)

Test / parallelExecution := false
