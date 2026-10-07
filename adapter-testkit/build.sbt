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

// --- Maven Central publishing (Central Portal) ---
// Published with contract/ir/spark-adapter: spark-adapter's POM depends on this module, so a release
// that left it out would be unresolvable (docs/RELEASING.md). Everything below is the POM metadata and
// Central Portal settings Sonatype requires, copied from ir/build.sbt so the modules cannot drift.
publishMavenStyle := true
Test / publishArtifact := false
pomIncludeRepository := { _ => false }

homepage := Some(url("https://github.com/mlltx/Invaract"))
licenses := List("Apache-2.0" -> url("https://www.apache.org/licenses/LICENSE-2.0.txt"))
scmInfo := Some(
  ScmInfo(
    url("https://github.com/mlltx/Invaract"),
    "scm:git@github.com:mlltx/Invaract.git"
  )
)
developers := List(
  // id/url are the real GitHub org this repo lives under; email is
  // GitHub's own noreply-alias convention, to avoid publishing a personal
  // address in a public POM. Update `name` with a real maintainer name
  // before the first real release.
  Developer(
    id = "mlltx",
    name = "mlltx",
    email = "mlltx@users.noreply.github.com",
    url = url("https://github.com/mlltx")
  )
)

// Pre-1.0 (docs/VERSIONING.md): a 0.x -> 0.(x+1) bump may be
// binary-breaking, so "early-semver" is the accurate scheme - the same
// convention this file's own mimaPreviousArtifacts comment already bumps
// MINOR (not PATCH) for a deliberate break under.
versionScheme := Some("early-semver")

// The OSSRH/Nexus staging host sbt-sonatype originally targeted is
// retired; Central Portal (central.sonatype.com) is the only route onto
// Maven Central now. See docs/RELEASING.md for credentials/CI wiring and
// the actual release commands (publishSigned, then sonatypeCentralRelease).
import xerial.sbt.Sonatype.sonatypeCentralHost
sonatypeCredentialHost := sonatypeCentralHost
publishTo := sonatypePublishToBundle.value

// Non-interactive PGP passphrase for CI (crazy-max/ghaction-import-gpg
// imports the key + configures gpg-agent; this just supplies the
// passphrase sbt-pgp needs when it shells out to gpg). Unset locally -
// sbt-pgp falls back to an interactive prompt, which is fine for a
// maintainer cutting a release by hand.
pgpPassphrase := sys.env.get("PGP_PASSPHRASE").map(_.toCharArray)
name := "invaract-adapter-testkit"

libraryDependencies ++= Seq(
  "com.invaract" %% "invaract-contract" % "0.13.0",
  "com.invaract" %% "invaract-ir" % "0.6.0",
  "com.invaract" %% "invaract-verification-core" % "0.3.0",
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
