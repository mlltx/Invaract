#!/bin/bash
# Shared helpers for dev/test and dev/regression. Not meant to be run
# directly — source it after cd-ing to the repo root.

PLUGIN_JAR="plugin/target/scala-2.12/invariant-spark-plugin-0.1.0.jar"
RUNNER_JAR="runner/target/scala-2.12/invariant-spark-runner.jar"

# run_demo_job_harness INPUT OUTPUT REPORT [CONTRACT]
#
# Runs DemoJobHarness (the example Spark job / test harness — see its class
# doc in runner/src/main/scala/com/example/runner/DemoJobHarness.scala; it
# is not Invariant's verification engine, just the job that exercises it)
# via spark-submit when it's on PATH, falling back to a manually-flagged
# `java -cp` invocation otherwise.
#
# On Windows (git-bash/MSYS, $OSTYPE=msys), Spark's bin/spark-submit is a
# bash script whose bin/spark-class internals call `ps -o`, which
# git-bash's `ps` doesn't support ("bad array subscript" a few lines
# later, once the command array never gets built). Spark ships a native
# bin/spark-submit.cmd launcher for exactly this — use it instead when
# present. It needs SPARK_HOME as a real Windows path (D:\...), not
# git-bash's POSIX form (/d/...), which is why cygpath -w wraps it here
# but nowhere else in this repo's scripts.
run_demo_job_harness() {
  local input="$1" output="$2" report="$3" contract="${4:-}"

  if [[ "$OSTYPE" == "msys" || "$OSTYPE" == "cygwin" ]] && command -v spark-submit.cmd &> /dev/null; then
    SPARK_HOME="$(cygpath -w "$SPARK_HOME")" \
      spark-submit.cmd \
        --class com.example.runner.DemoJobHarness \
        --master local[*] \
        --jars "$PLUGIN_JAR" \
        "$RUNNER_JAR" \
        "$input" "$output" "$report" $contract
  elif command -v spark-submit &> /dev/null; then
    spark-submit \
      --class com.example.runner.DemoJobHarness \
      --master local[*] \
      --jars "$PLUGIN_JAR" \
      "$RUNNER_JAR" \
      "$input" "$output" "$report" $contract
  else
    # spark-submit's own launch scripts inject the --add-opens flags Spark
    # needs on JDK 17+ (see plugin/build.sbt and spark-adapter/build.sbt
    # for the same fix applied to `sbt test`); this fallback bypasses
    # spark-submit entirely, so it needs those flags reproduced explicitly.
    # Classpath separator is OS-dependent: ';' on Windows, ':' elsewhere.
    local cp_sep=":"
    [[ "$OSTYPE" == "msys" || "$OSTYPE" == "cygwin" ]] && cp_sep=";"
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
      -cp "$PLUGIN_JAR${cp_sep}$RUNNER_JAR" com.example.runner.DemoJobHarness \
      "$input" "$output" "$report" $contract
  fi
}
