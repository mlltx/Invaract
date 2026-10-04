#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Runs a command while streaming one line of machine health to *this step's output* every
# MEMWATCH_INTERVAL seconds (default 30):
#
#   memwatch.sh -- sbt "stryker --mutate ... --concurrency 2"
#
# Why it streams instead of writing a file for a later "report" step: the failure this exists to
# explain is a runner that dies mid-job ("The runner has received a shutdown signal"). A runner
# that dies never runs its later steps, so a file or an `if: always()` step is lost with it; but
# GitHub streams a step's output to the job log as it is produced, so the samples printed before
# the kill are still there afterwards. Read the last few `[memwatch]` lines of a killed job to see
# whether memory, threads or disk were exhausted just before it went.
#
# Each line: wall clock, memory in use / available / total, swap in use, 1-minute load, number
# of java processes with their combined resident memory and thread count, free disk on /, and
# the three biggest processes. When the command ends a SUMMARY line gives the peaks, and the same
# is appended to the job summary page when GITHUB_STEP_SUMMARY is set. The exit status is the
# command's own. Linux only (it reads /proc), which is all the jobs that use it run on.
set -uo pipefail

interval="${MEMWATCH_INTERVAL:-30}"
if [[ "${1:-}" == "--interval" ]]; then interval="$2"; shift 2; fi
if [[ "${1:-}" != "--" || $# -lt 2 ]]; then
  echo "usage: memwatch.sh [--interval SECONDS] -- command [args...]" >&2
  exit 2
fi
shift

log="$(mktemp)"

sample() {
  local total avail swap load jcount jrss jthreads disk top used
  read -r total avail < <(awk '/^MemTotal/{t=$2} /^MemAvailable/{a=$2} END{printf "%d %d", t/1024, a/1024}' /proc/meminfo)
  swap=$(awk '/^SwapTotal/{t=$2} /^SwapFree/{f=$2} END{printf "%d", (t-f)/1024}' /proc/meminfo)
  load=$(cut -d' ' -f1 /proc/loadavg)
  read -r jcount jrss jthreads < <(ps -eo rss=,nlwp=,comm= | awk '$3=="java"{c++; r+=$1; t+=$2} END{printf "%d %d %d", c, r/1024, t}')
  disk=$(df -BG --output=avail / | awk 'NR==2{gsub("G","",$1); print $1}')
  top=$(ps -eo rss=,comm= --sort=-rss | head -3 | awk '{printf "%s=%dMB ", $2, $1/1024}')
  used=$((total - avail))
  # machine-readable line for the summary, human-readable line for the log
  echo "$used $avail $jrss $jthreads $swap $disk" >> "$log"
  echo "[memwatch] $(date -u +%H:%M:%S) used=${used}MB avail=${avail}MB/${total}MB swap=${swap}MB load=${load} java=${jcount}x rss=${jrss}MB threads=${jthreads} disk=${disk}GB top: ${top}"
}

( while true; do sample; sleep "$interval"; done ) &
sampler=$!
stop_sampler() { kill "$sampler" 2>/dev/null; wait "$sampler" 2>/dev/null; }
trap stop_sampler EXIT

"$@"
status=$?

stop_sampler
trap - EXIT
sample

summary=$(awk '
  NR==1 || $1>mu {mu=$1} NR==1 || $2<ma {ma=$2} $3>mr {mr=$3} $4>mt {mt=$4} $5>ms {ms=$5} NR==1 || $6<md {md=$6}
  END { if (NR==0) print "no samples"; else printf "peak used=%dMB, min available=%dMB, peak java rss=%dMB, peak java threads=%d, peak swap=%dMB, min free disk=%dGB (%d samples)", mu, ma, mr, mt, ms, md, NR }' "$log")
echo "[memwatch] SUMMARY (exit status ${status}): ${summary}"
if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
  { echo "### Memory telemetry"; echo; echo "${summary} (command exit status ${status})"; } >> "$GITHUB_STEP_SUMMARY"
fi
# Kernel OOM evidence, where the runner lets us read it (best effort).
oom=$( (sudo -n dmesg 2>/dev/null || dmesg 2>/dev/null) | grep -ciE "out of memory|oom-kill" || true)
if [[ "${oom:-0}" -gt 0 ]]; then echo "[memwatch] kernel log reports ${oom} out-of-memory line(s)"; fi
rm -f "$log"
exit "$status"
