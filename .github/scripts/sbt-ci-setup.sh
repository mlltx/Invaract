#!/usr/bin/env bash
# Makes CI's sbt resilient to Maven Central refusing downloads. Run once per job, after sbt is
# installed: by .github/actions/setup-jdk-sbt, and by the test matrix job, which installs sbt itself.
#
# Maven Central answers bursts of requests with HTTP 429 ("too many requests"), sometimes for
# minutes on one file. sbt's resolver (coursier) reports a refused pom as "not found" and a refused
# jar as a download error, and retries neither; the sbt launcher fetching sbt itself fails the same
# way. Five jobs on PR #237 were lost to it, and a retry of up to two minutes was not enough on #239.
#
# 1. Every sbt download - every build, its plugins, and the launcher - goes first to Google's mirror
#    of Maven Central (the same artifacts, as Gradle and Bazel use it), then Maven Central itself.
#    sbt's repository override (`sbt.override.build.repos`, `sbt.repository.config`) sets this for
#    all of them, here, without touching any build.sbt. A file one repository refuses as "not
#    found" is looked for in the next. `local` stays first: the engine modules are published there.
# 2. .github/scripts/sbt-retry.sh is installed as `sbt` ahead of the real one: an sbt run that still
#    fails only on a download is run again.

set -euo pipefail

dir="$RUNNER_TEMP/sbt-ci"
mkdir -p "$dir"

cat > "$dir/repositories" <<'EOF'
[repositories]
  local
  maven-central-mirror: https://maven-central.storage-download.googleapis.com/maven2/
  maven-central
  sbt-plugin-releases: https://repo.scala-sbt.org/scalasbt/sbt-plugin-releases/, [organization]/[module]/(scala_[scalaVersion]/)(sbt_[sbtVersion]/)[revision]/[type]s/[artifact](-[classifier]).[ext]
EOF
echo "SBT_OPTS=${SBT_OPTS:+$SBT_OPTS }-Dsbt.override.build.repos=true -Dsbt.repository.config=$dir/repositories" >> "$GITHUB_ENV"

cp "$(dirname "$0")/sbt-retry.sh" "$dir/sbt"
chmod +x "$dir/sbt"
echo "$dir" >> "$GITHUB_PATH"

echo "sbt: downloads from Google's Maven Central mirror, then Maven Central; retried on a refused download ($dir)"
