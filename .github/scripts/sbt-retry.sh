#!/usr/bin/env bash
# sbt, run again when it failed only because a dependency could not be downloaded.
#
# Installed as `sbt`, ahead of the real one on PATH, by .github/scripts/sbt-ci-setup.sh, which also
# sends sbt's downloads to a mirror first; this is the second line of defence. Maven Central answers
# bursts of requests with HTTP 429 ("too many requests"), and sbt's resolver (coursier) reports a 429
# as "not found": the build fails on an artifact that exists. Coursier's own retry covers connection errors, not this, and cannot reach the sbt
# launcher fetching sbt itself, which fails the same way. Five CI jobs on PR #237 were lost to it.
#
# So: run the real sbt; if it fails and its output shows one of sbt's own download failures, wait
# and run the same command again, up to SBT_RETRY_ATTEMPTS times in all (default 3), waiting
# SBT_RETRY_DELAY seconds (default 30) times the attempt number. Any other failure - a failing
# test, a compile error - is returned at once, unchanged. The output is printed as it comes;
# sbt's stderr is merged into stdout (no step reads them apart).

set -u

attempts="${SBT_RETRY_ATTEMPTS:-3}"
delay="${SBT_RETRY_DELAY:-30}"

# The real sbt: the first `sbt` on PATH that is not this script.
real=""
while IFS= read -r candidate; do
  if [ ! "$candidate" -ef "$0" ]; then real="$candidate"; break; fi
done < <(type -ap sbt)
if [ -z "$real" ]; then
  echo "sbt-retry: no sbt on PATH besides this wrapper" >&2
  exit 127
fi

# The failures this retries: sbt's dependency resolution, and the launcher fetching sbt itself.
download_failure='sbt\.librarymanagement\.ResolveException|\[launcher\] could not retrieve'

log="$(mktemp)"
trap 'rm -f "$log"' EXIT

attempt=1
while :; do
  "$real" "$@" 2>&1 | tee "$log"
  status="${PIPESTATUS[0]}"
  if [ "$status" -eq 0 ]; then exit 0; fi
  if ! grep -qE "$download_failure" "$log"; then exit "$status"; fi
  if [ "$attempt" -ge "$attempts" ]; then
    echo "sbt-retry: a dependency download failed on all $attempts attempts; giving up" >&2
    exit "$status"
  fi
  wait_for=$((delay * attempt))
  # A new line first: sbt ends its output without one, and an annotation must start a line.
  printf '\n::warning::sbt could not download a dependency (attempt %s of %s; Maven Central rate-limits bursts). Running it again in %ss.\n' "$attempt" "$attempts" "$wait_for"
  sleep "$wait_for"
  attempt=$((attempt + 1))
done
