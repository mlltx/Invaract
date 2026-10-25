// Publishes to Sonatype's Central Portal (central.sonatype.com) - the
// OSSRH/Nexus staging host this plugin originally targeted was retired,
// and Central Portal is the only route onto Maven Central now. See
// docs/RELEASING.md for the full release process and required settings.
addSbtPlugin("org.xerial.sbt" % "sbt-sonatype" % "3.12.2")
