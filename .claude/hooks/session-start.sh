#!/bin/bash
# SessionStart hook for Claude Code on the web: provisions the Scala/sbt/Spark
# toolchain this repo needs (see CLAUDE.md), so a remote session can run
# `sbt test`/`./dev/build`/`./dev/test` without re-bootstrapping from zero.
#
# This is the Claude Code on the web equivalent of .devcontainer/post-create.sh
# (which only runs in an actual GitHub Codespace / VS Code Dev Container) -
# without it, a fresh remote session's container has JDK and Node already
# present, but no sbt and no Spark distribution (spark-submit) at all.
#
# Idempotent: every install step is skipped if already present, so re-running
# this (e.g. on session resume) is safe and cheap.
set -uo pipefail

if [ "${CLAUDE_CODE_REMOTE:-}" != "true" ]; then
  exit 0
fi

CACERT_ARGS=()
if [ -f /root/.ccr/ca-bundle.crt ]; then
  CACERT_ARGS=(--cacert /root/.ccr/ca-bundle.crt)
fi

echo "=== Invaract session-start: provisioning sbt + Spark ==="

# --- sbt (universal tarball, not coursier's `cs install sbt`) --------------
#
# `cs install sbt` was tried first and rejected: coursier's native launcher
# ships its own embedded JVM that does not honor this environment's
# JAVA_TOOL_OPTIONS-configured truststore, so every HTTPS request through the
# egress proxy fails PKIX validation. The plain sbt universal tarball's
# bin/sbt script is an ordinary shell script that execs `java -jar
# sbt-launch.jar`, which *does* pick up JAVA_TOOL_OPTIONS - confirmed
# directly, not assumed.
SBT_VERSION="1.11.7"
if ! command -v sbt &> /dev/null; then
  echo "Installing sbt ${SBT_VERSION}..."
  for i in 1 2 3 4 5; do
    if curl -fsSL "${CACERT_ARGS[@]}" \
      "https://github.com/sbt/sbt/releases/download/v${SBT_VERSION}/sbt-${SBT_VERSION}.tgz" \
      -o /tmp/sbt.tgz; then
      break
    fi
    echo "  sbt download attempt $i failed, retrying..."
    sleep $((i * 5))
  done
  if [ -f /tmp/sbt.tgz ]; then
    tar -xzf /tmp/sbt.tgz -C /opt
    ln -sf /opt/sbt/bin/sbt /usr/local/bin/sbt
    rm -f /tmp/sbt.tgz
  else
    echo "WARNING: could not download sbt - the session will need to install it manually" >&2
  fi
else
  echo "sbt already present: $(command -v sbt)"
fi

# --- Spark (for spark-submit; sbt itself only compiles against the Spark
# artifacts it resolves as a library dependency, not this distribution) -----
SPARK_VERSION="3.5.7"
if [ ! -x /opt/spark/bin/spark-submit ]; then
  echo "Installing Spark ${SPARK_VERSION}..."
  for i in 1 2 3 4 5; do
    if curl -fsSL "${CACERT_ARGS[@]}" \
      "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz" \
      -o /tmp/spark.tgz; then
      break
    fi
    echo "  Spark download attempt $i failed, retrying..."
    sleep $((i * 5))
  done
  if [ -f /tmp/spark.tgz ]; then
    mkdir -p /opt/spark
    tar -xzf /tmp/spark.tgz -C /opt/spark --strip-components=1
    rm -f /tmp/spark.tgz
  else
    echo "WARNING: could not download Spark - spark-submit will be unavailable; ./dev/test falls back to a plain java -cp launch (see dev/lib.sh) but will still be missing spark-submit's own JDK 17+ --add-opens flags unless those are already applied elsewhere" >&2
  fi
else
  echo "Spark already present: /opt/spark"
fi

if [ -x /opt/spark/bin/spark-submit ]; then
  {
    echo "export SPARK_HOME=\"/opt/spark\""
    echo "export PATH=\"/opt/spark/bin:\$PATH\""
  } >> "$CLAUDE_ENV_FILE"
fi

if command -v sbt &> /dev/null; then
  echo "sbt: $(sbt --version 2>&1 | tail -1)"
fi
if [ -x /opt/spark/bin/spark-submit ]; then
  echo "spark-submit: $(/opt/spark/bin/spark-submit --version 2>&1 | grep -i version | head -1)"
fi

# --- Warm each module's dependency cache, in the same order dev/build uses
# (see that script's own comment for why: contract/ir/fingerprint must be
# publishLocal'd before spark-adapter/runner can resolve them as library
# dependencies - they're real Maven coordinates, not source references,
# since there's no aggregating root build.sbt). Best-effort and non-fatal:
# this environment's Maven Central access can be rate-limited, so a module
# that doesn't fully warm here just means `sbt test`/`./dev/build` inside
# the session retries the remainder - a slower first real build, not a
# broken one. Each attempt's own partial progress (successfully-downloaded
# jars) persists in ~/.ivy2/~/.cache/coursier across retries.
warm_module() {
  local dir="$1"
  local cmd="$2"
  if [ ! -d "$CLAUDE_PROJECT_DIR/$dir" ]; then
    return 0
  fi
  echo "Warming $dir ($cmd)..."
  (
    cd "$CLAUDE_PROJECT_DIR/$dir" || exit 1
    for i in 1 2 3 4 5 6; do
      if sbt -Dsbt.log.noformat=true -batch "$cmd" > /tmp/sbt-warm-"${dir//\//_}"-"$i".log 2>&1; then
        echo "  $dir warmed"
        exit 0
      fi
      echo "  $dir attempt $i failed (Maven Central rate limiting is common here), retrying..."
      sleep $((i * 15))
    done
    echo "WARNING: $dir did not fully warm after 6 attempts - see /tmp/sbt-warm-${dir//\//_}-*.log; the session will finish this on first real build" >&2
    exit 0
  )
}

if command -v sbt &> /dev/null; then
  warm_module "contract" "publishLocal"
  warm_module "ir" "publishLocal"
  warm_module "plugin" "compile"
  warm_module "fingerprint" "publishLocal"
  warm_module "spark-adapter" "compile"
  warm_module "runner" "compile"
fi

echo "=== Invaract session-start: done ==="
