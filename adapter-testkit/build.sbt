// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

// The adapter conformance kit (docs/MULTI_ENGINE_ADAPTERS.md, Stage 4): a catalogue of
// engine-neutral scenarios plus the machinery that runs them through any engine adapter and
// compares the verdict with what the adapter's own capability declaration promises.
//
// It is a library an adapter depends on `% Test` - spark-adapter is the first - not part of the
// engine itself: nothing at runtime depends on it, so it is not bundled into any assembly jar.
// It depends only on verification-core (and scalatest, because it ships the ScalaTest trait an
// adapter's test mixes in), never on an engine.
ThisBuild / version := "0.2.0"
scalaVersion := "2.12.18"
organization := "com.invaract"
name := "invaract-adapter-testkit"

libraryDependencies ++= Seq(
  "com.invaract" %% "invaract-contract" % "0.13.0",
  "com.invaract" %% "invaract-ir" % "0.6.0",
  "com.invaract" %% "invaract-verification-core" % "0.2.0",
  "org.scalatest" %% "scalatest" % "3.2.18",
  // verification-core's own `provided` dependencies, needed to run its classes here.
  "org.slf4j" % "slf4j-api" % "2.0.17" % "provided",
  "org.slf4j" % "slf4j-simple" % "2.0.7" % "test"
)

scalacOptions ++= Seq(
  "-target:jvm-1.8",
  "-deprecation",
  "-feature"
)

Test / fork := true
