#!/usr/bin/env python3
"""Decide whether a finished "Test and Build" run failed only because GitHub lost runners.

A job whose runner was shut down mid-run ("The runner has received a shutdown signal ...") is not
a test failure: it ends with conclusion `failure` and a single check-run annotation
"The operation was canceled.", whereas a job whose step really failed carries
"Process completed with exit code N." (and a job superseded by a newer push is `cancelled`, not
`failure`). Re-running a runner-loss job is therefore safe and a real failure must never be
re-run into a green, so the rule here is deliberately narrow:

  re-run only if EVERY failed job (other than the "Test Summary" gate, which fails whenever
  anything upstream did) is a runner loss, and the run has not already been retried.

Usage (from the workflow):  runner_loss.py <owner/repo> <run_id>
Prints a decision line and, when it says to re-run, exits 0 after writing `rerun=true` to
$GITHUB_OUTPUT. Everything GitHub-specific is in `fetch`; `decide` is pure and unit tested.
"""
import json
import os
import subprocess
import sys

GATE_JOB = "Test Summary"
# One re-run per head. A second loss is reported, not retried, so a genuinely broken
# configuration (e.g. a job that always exhausts the runner) cannot loop.
MAX_ATTEMPTS = 2

RUNNER_LOSS_MARKERS = (
    "the operation was canceled",
    "runner has received a shutdown signal",
    "lost communication with the server",
    "the hosted runner",  # "The hosted runner lost connection with the server" / "encountered an error"
)
REAL_FAILURE_MARKERS = ("process completed with exit code",)


def is_runner_loss(annotations):
    """True if this failed job's annotations show a lost runner and no real step failure."""
    messages = [(a.get("message") or "").lower() for a in annotations]
    if not messages:
        return False
    if any(m for m in messages if any(k in m for k in REAL_FAILURE_MARKERS)):
        return False
    return any(any(k in m for k in RUNNER_LOSS_MARKERS) for m in messages)


def decide(run_attempt, jobs):
    """jobs: list of {"name", "conclusion", "annotations": [...]}. Returns (rerun, reason)."""
    if run_attempt >= MAX_ATTEMPTS:
        return False, f"already attempt {run_attempt}; not retrying again"
    failed = [j for j in jobs if j["conclusion"] == "failure" and j["name"] != GATE_JOB]
    if not failed:
        return False, "no failed jobs besides the summary gate"
    real = [j["name"] for j in failed if not is_runner_loss(j.get("annotations", []))]
    if real:
        return False, "real failure(s), not retrying: " + ", ".join(sorted(real))
    return True, "only runner loss: " + ", ".join(sorted(j["name"] for j in failed))


def gh(*args):
    out = subprocess.run(["gh", "api", "--paginate", *args], check=True, capture_output=True, text=True).stdout
    return out


def fetch(repo, run_id):
    run = json.loads(gh(f"repos/{repo}/actions/runs/{run_id}"))
    # --paginate emits one JSON document per page for list endpoints; merge them.
    decoder, text, idx, jobs = json.JSONDecoder(), gh(f"repos/{repo}/actions/runs/{run_id}/jobs?per_page=100"), 0, []
    while idx < len(text):
        while idx < len(text) and text[idx].isspace():
            idx += 1
        if idx >= len(text):
            break
        page, idx = decoder.raw_decode(text, idx)
        jobs.extend(page.get("jobs", []))
    result = []
    for j in jobs:
        ann = []
        if j["conclusion"] == "failure" and j["name"] != GATE_JOB:
            ann = json.loads(gh(f"repos/{repo}/check-runs/{j['id']}/annotations") or "[]")
        result.append({"name": j["name"], "conclusion": j["conclusion"], "annotations": ann})
    return run["run_attempt"], result


def main(argv):
    repo, run_id = argv[1], argv[2]
    attempt, jobs = fetch(repo, run_id)
    rerun, reason = decide(attempt, jobs)
    print(f"rerun={str(rerun).lower()} ({reason})")
    out = os.environ.get("GITHUB_OUTPUT")
    if out:
        with open(out, "a") as f:
            f.write(f"rerun={str(rerun).lower()}\nreason={reason}\n")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
