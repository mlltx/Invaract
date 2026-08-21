#!/bin/bash
# Shared helpers for dev/test and dev/regression. Not meant to be run
# directly — source it after cd-ing to the repo root.

PLUGIN_JAR="plugin/target/scala-2.12/invariant-spark-plugin-0.1.0.jar"
RUNNER_JAR="runner/target/scala-2.12/invariant-spark-runner.jar"

# run_plugin_runner INPUT OUTPUT REPORT [CONTRACT]
#
# Runs PluginRunner via spark-submit when it's on PATH, falling back to a
# manually-flagged `java -cp` invocation otherwise. spark-submit's own
# launch scripts inject the --add-opens flags Spark needs on JDK 17+ (see
# plugin/build.sbt and spark-adapter/build.sbt for the same fix applied to
# `sbt test`); the fallback path bypasses spark-submit entirely, so it
# needs those flags reproduced explicitly.
run_plugin_runner() {
  local input="$1" output="$2" report="$3" contract="${4:-}"

  if command -v spark-submit &> /dev/null; then
    spark-submit \
      --class com.example.runner.PluginRunner \
      --master local[*] \
      --jars "$PLUGIN_JAR" \
      "$RUNNER_JAR" \
      "$input" "$output" "$report" $contract
  else
    java \
      --add-opens=java.base/java.lang=ALL-UNNAMED \
      --add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
      --add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
      --add-opens=java.base/java.io=ALL-UNNAMED \
      --add-opens=java.base/java.net=ALL-UNNAMED \
      --add-opens=java.base/java.nio=ALL-UNNAMED \
      --add-opens=java.base/java.util=ALL-UNNAMED \
      --add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
      --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED \
      --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
      --add-opens=java.base/sun.nio.cs=ALL-UNNAMED \
      --add-opens=java.base/sun.security.action=ALL-UNNAMED \
      --add-opens=java.base/sun.util.calendar=ALL-UNNAMED \
      -cp "$PLUGIN_JAR:$RUNNER_JAR" com.example.runner.PluginRunner \
      "$input" "$output" "$report" $contract
  fi
}
