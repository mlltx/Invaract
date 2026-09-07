// SBOM generation needs no Central publishing/signing setup to be
// meaningful (unlike sonatype.sbt/pgp.sbt/mima.sbt, deliberately not yet
// added here - see build.sbt's own FOLLOW-UP comment) - it's a pure
// dependency-transparency report, so this module gets it now, matching
// contract/ir/spark-adapter's own sbom.sbt.
addSbtPlugin("com.github.sbt" %% "sbt-sbom" % "0.4.0")
