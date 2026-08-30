#!/bin/bash
# Shared helpers for dev/test, dev/regression, and dev/dry-run. Not meant to
# be run directly — source it after cd-ing to the repo root.

PLUGIN_JAR="plugin/target/scala-2.12/invaract-spark-plugin-0.1.0.jar"
RUNNER_JAR="runner/target/scala-2.12/invaract-spark-runner.jar"

# ANSI color codes every dev/ script's console output uses.
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

_dev_failure_message="Script failed"

_dev_cleanup_trap() {
  local exit_code=$?
  if [ $exit_code -ne 0 ]; then
    echo -e "${RED}✕ ${_dev_failure_message}${NC}"
  fi
  return $exit_code
}

# install_failure_trap MESSAGE
#
# Registers an EXIT trap that prints MESSAGE (prefixed with ✕, in red) only
# if the script exits non-zero, then preserves that exit code — the same
# cleanup()/trap pattern dev/test and dev/regression each still define
# inline (their own `cleanup()` functions); dev/dry-run uses this shared
# version instead of a third inline copy.
install_failure_trap() {
  _dev_failure_message="$1"
  trap _dev_cleanup_trap EXIT
}

# run_demo_job_harness INPUT OUTPUT REPORT [CONTRACT] [EXTRA_ARG...]
#
# Runs DemoJobHarness (the example Spark job / test harness — see its class
# doc in runner/src/main/scala/com/example/runner/DemoJobHarness.scala; it
# is not Invaract's verification engine, just the job that exercises it)
# via spark-submit when it's on PATH, falling back to a manually-flagged
# `java -cp` invocation otherwise. Any arguments beyond CONTRACT are passed
# straight through after it — used by dev/dry-run to append `--dry-run`,
# which DemoJobHarness recognizes anywhere in its argument list.
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
  # A plain word-split string, not a bash array: dev/regression runs under
  # `set -u`, and macOS's default /bin/bash (still 3.2, Apple ships it
  # GPLv2-only and hasn't upgraded) throws "unbound variable" expanding
  # "${arr[@]}" on a zero-length array under nounset - confirmed the hard
  # way by a real CI failure on macos-latest, not assumed. Unquoted
  # interpolation below matches the existing `$contract` convention in this
  # same function, which has the identical "no spaces in a real argument"
  # assumption already.
  local extra="${*:5}"

  if [[ "$OSTYPE" == "msys" || "$OSTYPE" == "cygwin" ]] && command -v spark-submit.cmd &> /dev/null; then
    SPARK_HOME="$(cygpath -w "$SPARK_HOME")" \
      spark-submit.cmd \
        --class com.example.runner.DemoJobHarness \
        --master local[*] \
        --jars "$PLUGIN_JAR" \
        "$RUNNER_JAR" \
        "$input" "$output" "$report" $contract $extra
  elif command -v spark-submit &> /dev/null; then
    spark-submit \
      --class com.example.runner.DemoJobHarness \
      --master local[*] \
      --jars "$PLUGIN_JAR" \
      "$RUNNER_JAR" \
      "$input" "$output" "$report" $contract $extra
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
      "$input" "$output" "$report" $contract $extra
  fi
}
