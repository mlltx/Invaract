// Pinned to 0.4.0, not the newer 0.5.0/0.6.0: 0.5.0+ requires sbt >= 1.10.11,
// which breaks contract/plugin/runner's sbt 1.9.8 pin. 0.4.0 supports sbt back
// to 1.5.2, so the same version works across both sbt lines this repo uses.
addSbtPlugin("com.github.sbt" %% "sbt-sbom" % "0.4.0")
