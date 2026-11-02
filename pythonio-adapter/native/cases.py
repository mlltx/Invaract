#!/usr/bin/env python3
"""Runs the checker's recorded protocol cases against a command, the way the `invaract` package will run it.

    cases.py CASES_DIR [--record] [--native] [--max-seconds S] -- COMMAND...

Each case is a directory holding `requests.jsonl` (one request per line), `replies.jsonl` (the reply expected for each), the
files the requests name, and optionally `events.txt` (the events a file sink must have written to `events.jsonl`, in order, each
as its type and what it concluded: `CONTRACT_VALIDATION FAILED`) and `http-events.txt` (the same for the events an HTTP sink
must have posted to `${HTTP}`, a local endpoint the run provides). A case is copied to a fresh directory first, and `${CASE}` in any of its files becomes that directory, so the
requests can name the case's own files whatever runs them. COMMAND runs once per case in that directory, with the requests on
its standard input: `java -jar invaract-pythonio-checker.jar`, or the native program built from it.

PythonIoProtocolCasesSpec runs the same cases inside the JVM; this script holds the native program to the same replies
(milestone 0, docs/PYTHON_IO_ADAPTER_DESIGN.md). `--native` skips a case holding a `jvm-only` file, which says why: a case
that needs a class the native program cannot load by name. `--record` writes each case's replies and events instead of
comparing them, for a new case; read what it wrote before committing it. It skips a `jvm-only` case too, whose replies are
written by hand. `--max-seconds` fails a case that takes longer,
start-up included.
"""
import argparse
import http.server
import json
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time

PLACEHOLDER = "${CASE}"
HTTP_PLACEHOLDER = "${HTTP}"


class Receiver:
    """A local HTTP endpoint for a case's HTTP sink: `${HTTP}` in a case's files is its URL, and the event types posted to it
    must match the case's `http-events.txt`."""

    def __init__(self):
        received = self.received = []

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_POST(self):
                body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
                received.append(describe(json.loads(body)))
                self.send_response(200)
                self.end_headers()

            def log_message(self, *args):
                pass

        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}/events"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def close(self):
        self.server.shutdown()
        self.server.server_close()


def copy_case(source, target, url):
    shutil.copytree(source, target)
    for root, _, files in os.walk(target):
        for name in files:
            path = os.path.join(root, name)
            with open(path, encoding="utf-8") as f:
                text = f.read()
            if PLACEHOLDER in text or HTTP_PLACEHOLDER in text:
                with open(path, "w", encoding="utf-8") as f:
                    f.write(text.replace(PLACEHOLDER, target).replace(HTTP_PLACEHOLDER, url))


def json_lines(text):
    return [json.loads(line) for line in text.splitlines() if line.strip()]


def describe(event):
    """An event as `events.txt` records it: its type, then what it concluded (a validation's status; an inference's status and
    self-check; a dry-run summary's merge status)."""
    parts = [event["eventType"]] + [str(event[k]) for k in ("status", "selfCheck", "mergeStatus") if event.get(k) is not None]
    return " ".join(parts)


def event_types(directory):
    path = os.path.join(directory, "events.jsonl")
    if not os.path.exists(path):
        return []
    with open(path, encoding="utf-8") as f:
        return [describe(event) for event in json_lines(f.read())]


def run_case(name, source, command, record, max_seconds):
    """Runs one case; returns the problems found and how long it took."""
    work = os.path.join(tempfile.mkdtemp(prefix="invaract-case-"), name)
    receiver = Receiver()
    copy_case(source, work, receiver.url)
    with open(os.path.join(work, "requests.jsonl"), encoding="utf-8") as f:
        requests = f.read()
    started = time.monotonic()
    result = subprocess.run(command, cwd=work, input=requests, capture_output=True, text=True, timeout=120)
    elapsed = time.monotonic() - started
    receiver.close()
    problems = []
    if result.returncode != 0:
        problems.append(f"exited with {result.returncode}: {result.stderr.strip()[-2000:]}")
    replies = result.stdout.replace(work, PLACEHOLDER)
    events = event_types(work)
    expected_events_path = os.path.join(source, "events.txt")
    expected_http_path = os.path.join(source, "http-events.txt")
    if record:
        with open(os.path.join(source, "replies.jsonl"), "w", encoding="utf-8") as f:
            f.write("".join(json.dumps(r, sort_keys=True) + "\n" for r in json_lines(replies)))
        if events:
            with open(expected_events_path, "w", encoding="utf-8") as f:
                f.write("".join(e + "\n" for e in events))
        if receiver.received:
            with open(expected_http_path, "w", encoding="utf-8") as f:
                f.write("".join(e + "\n" for e in receiver.received))
    else:
        with open(os.path.join(source, "replies.jsonl"), encoding="utf-8") as f:
            expected = json_lines(f.read())
        actual = json_lines(replies)
        if len(actual) != len(expected):
            problems.append(f"{len(actual)} replies, expected {len(expected)}")
        for i, (a, e) in enumerate(zip(actual, expected), start=1):
            if a != e:
                problems.append(f"reply {i}:\n    got      {json.dumps(a, sort_keys=True)}\n    expected {json.dumps(e, sort_keys=True)}")
        for label, got, path in (("events", events, expected_events_path), ("HTTP events", receiver.received, expected_http_path)):
            wanted = []
            if os.path.exists(path):
                with open(path, encoding="utf-8") as f:
                    wanted = [line.strip() for line in f if line.strip()]
            if got != wanted:
                problems.append(f"{label} {got}, expected {wanted}")
    if max_seconds is not None and elapsed > max_seconds:
        problems.append(f"took {elapsed:.3f}s, more than the {max_seconds}s allowed")
    return problems, elapsed


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("cases")
    parser.add_argument("--record", action="store_true")
    parser.add_argument("--native", action="store_true")
    parser.add_argument("--max-seconds", type=float)
    argv = sys.argv[1:]
    split = argv.index("--") if "--" in argv else len(argv)
    args = parser.parse_args(argv[:split])
    command = argv[split + 1:]
    if not command:
        parser.error("give the command that runs the checker after --")
    failed, timings = 0, []
    for name in sorted(os.listdir(args.cases)):
        source = os.path.join(args.cases, name)
        if not os.path.isdir(source):
            continue
        if (args.native or args.record) and os.path.exists(os.path.join(source, "jvm-only")):
            # its classes are on the test classpath only, so no checker this script runs can give its replies
            print(f"SKIP {name}: jvm-only")
            continue
        problems, elapsed = run_case(name, source, command, args.record, args.max_seconds)
        timings.append(elapsed)
        print(f"{'FAIL' if problems else 'ok  '} {name} ({elapsed:.3f}s)")
        for p in problems:
            print("    " + p)
        failed += bool(problems)
    if timings:
        print(f"{len(timings)} case(s), {failed} failed; slowest {max(timings):.3f}s, median {sorted(timings)[len(timings) // 2]:.3f}s")
    sys.exit(1 if failed or not timings else 0)


if __name__ == "__main__":
    main()
