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

assembly / assemblyJarName := "invariant-ir-0.1.0.jar"

// Mutation testing (Stryker4s) config: see stryker4s.conf for the
// `mutate` scope rationale. Set here rather than in stryker4s.conf's
// `mutate` key, which was observed not to take effect via the config
// file in this sbt/plugin version combination - the CLI flag and this
// sbt setting both work, so this is used instead.
strykerMutate := Seq("src/main/scala/com/example/ir/Lineage.scala")
