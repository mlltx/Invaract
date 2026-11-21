"""sbt-retry.sh runs sbt again only when it failed on a dependency download.

A fake `sbt` stands in for the real one: it fails a set number of times with a chosen message,
then succeeds, counting its runs and recording its arguments. The wrapper is installed as `sbt`
ahead of it on PATH, the way .github/actions/setup-jdk-sbt installs it.
"""

import os
import shutil
import stat
import subprocess
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
WRAPPER = os.path.join(HERE, "sbt-retry.sh")

RESOLVE = "[error] (update) sbt.librarymanagement.ResolveException: Error downloading org.yaml:snakeyaml:2.2"
LAUNCHER = "[error] [launcher] could not retrieve sbt 1.9.8"
TEST_FAILURE = "[error] Failed tests:\n[error] \tcom.invaract.sparkadapter.SomeSpec"

FAKE_SBT = """#!/usr/bin/env bash
dir="$(dirname "$0")"
n=$(( $(cat "$dir/runs" 2>/dev/null || echo 0) + 1 ))
echo "$n" > "$dir/runs"
echo "$*" >> "$dir/args"
if [ "$n" -le "$FAKE_FAILURES" ]; then
  printf '%b\\n' "$FAKE_MESSAGE" >&2
  exit "${FAKE_STATUS:-1}"
fi
echo "[success] Total time: 1 s"
"""


class SbtRetry(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.real_dir = os.path.join(self.tmp, "real")
        self.wrapper_dir = os.path.join(self.tmp, "wrapper")
        os.makedirs(self.real_dir)
        os.makedirs(self.wrapper_dir)
        fake = os.path.join(self.real_dir, "sbt")
        with open(fake, "w") as f:
            f.write(FAKE_SBT)
        shutil.copy(WRAPPER, os.path.join(self.wrapper_dir, "sbt"))
        for path in (fake, os.path.join(self.wrapper_dir, "sbt")):
            os.chmod(path, os.stat(path).st_mode | stat.S_IEXEC)

    def tearDown(self):
        shutil.rmtree(self.tmp)

    def run_sbt(self, failures, message, status=1, attempts=None, args=("-batch", "test")):
        env = dict(os.environ)
        env["PATH"] = os.pathsep.join([self.wrapper_dir, self.real_dir, env["PATH"]])
        env.update(FAKE_FAILURES=str(failures), FAKE_MESSAGE=message, FAKE_STATUS=str(status), SBT_RETRY_DELAY="0")
        if attempts is not None:
            env["SBT_RETRY_ATTEMPTS"] = str(attempts)
        result = subprocess.run(["sbt", *args], env=env, capture_output=True, text=True)
        return result, self.runs()

    def runs(self):
        path = os.path.join(self.real_dir, "runs")
        if not os.path.exists(path):
            return 0
        with open(path) as f:
            return int(f.read())

    def test_success_runs_once(self):
        result, runs = self.run_sbt(0, "")
        self.assertEqual((result.returncode, runs), (0, 1))
        self.assertIn("[success]", result.stdout)

    def test_a_dependency_download_failure_is_run_again_until_it_succeeds(self):
        result, runs = self.run_sbt(2, RESOLVE)
        self.assertEqual((result.returncode, runs), (0, 3))
        self.assertEqual(result.stdout.count("::warning::sbt could not download a dependency"), 2)
        self.assertIn("attempt 1 of 3", result.stdout)
        self.assertIn("attempt 2 of 3", result.stdout)

    def test_the_launcher_failing_to_fetch_sbt_is_run_again(self):
        result, runs = self.run_sbt(1, LAUNCHER)
        self.assertEqual((result.returncode, runs), (0, 2))

    def test_other_failures_are_returned_at_once_with_their_status(self):
        result, runs = self.run_sbt(5, TEST_FAILURE, status=7)
        self.assertEqual((result.returncode, runs), (7, 1))
        self.assertNotIn("::warning::", result.stdout)
        self.assertIn("Failed tests", result.stdout)

    def test_gives_up_after_the_last_attempt_with_sbts_status(self):
        result, runs = self.run_sbt(10, RESOLVE, status=3)
        self.assertEqual((result.returncode, runs), (3, 3))
        self.assertIn("failed on all 3 attempts", result.stderr)

    def test_the_number_of_attempts_is_configurable(self):
        result, runs = self.run_sbt(10, RESOLVE, attempts=1)
        self.assertEqual((result.returncode, runs), (1, 1))
        os.remove(os.path.join(self.real_dir, "runs"))  # count the second call on its own
        result, runs = self.run_sbt(4, RESOLVE, attempts=5)
        self.assertEqual((result.returncode, runs), (0, 5))

    def test_arguments_reach_sbt_unchanged_on_every_attempt(self):
        self.run_sbt(1, RESOLVE, args=("-batch", "stryker --mutate \"{a.scala,b.scala}\""))
        with open(os.path.join(self.real_dir, "args")) as f:
            recorded = f.read().splitlines()
        self.assertEqual(recorded, ['-batch stryker --mutate "{a.scala,b.scala}"'] * 2)

    def test_without_a_real_sbt_it_says_so(self):
        env = dict(os.environ)
        env["PATH"] = os.pathsep.join([self.wrapper_dir, "/usr/bin", "/bin"])
        result = subprocess.run(["sbt"], env=env, capture_output=True, text=True)
        self.assertEqual(result.returncode, 127)
        self.assertIn("no sbt on PATH besides this wrapper", result.stderr)



class SbtCiSetup(unittest.TestCase):
    """sbt-ci-setup.sh, run as a CI step: the repositories file, SBT_OPTS and the wrapper on PATH."""

    def run_setup(self, sbt_opts=None):
        tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, tmp)
        env = dict(os.environ)
        env.update(RUNNER_TEMP=tmp, GITHUB_ENV=os.path.join(tmp, "env"), GITHUB_PATH=os.path.join(tmp, "path"))
        env.pop("SBT_OPTS", None)
        if sbt_opts is not None:
            env["SBT_OPTS"] = sbt_opts
        subprocess.run(["bash", os.path.join(HERE, "sbt-ci-setup.sh")], env=env, check=True, capture_output=True)
        def read(name):
            with open(os.path.join(tmp, name)) as f:
                return f.read()

        return tmp, read

    def test_downloads_go_to_the_mirror_first_then_maven_central_with_local_ahead_of_both(self):
        tmp, read = self.run_setup()
        repos = [line.strip() for line in read("sbt-ci/repositories").splitlines()]
        self.assertEqual(repos[0], "[repositories]")
        self.assertEqual(repos[1], "local")
        self.assertEqual(repos[2], "maven-central-mirror: https://maven-central.storage-download.googleapis.com/maven2/")
        self.assertEqual(repos[3], "maven-central")
        self.assertTrue(repos[4].startswith("sbt-plugin-releases: https://repo.scala-sbt.org/scalasbt/sbt-plugin-releases/, "))

    def test_sbt_is_told_to_use_that_file_for_every_build(self):
        tmp, read = self.run_setup()
        self.assertEqual(
            read("env").strip(),
            "SBT_OPTS=-Dsbt.override.build.repos=true -Dsbt.repository.config=" + os.path.join(tmp, "sbt-ci", "repositories"),
        )

    def test_existing_sbt_opts_are_kept(self):
        _, read = self.run_setup(sbt_opts="-Xmx2g")
        self.assertTrue(read("env").startswith("SBT_OPTS=-Xmx2g -Dsbt.override.build.repos=true "))

    def test_the_retry_wrapper_is_installed_as_sbt_on_path(self):
        tmp, read = self.run_setup()
        installed = os.path.join(tmp, "sbt-ci", "sbt")
        self.assertEqual(read("path").strip(), os.path.join(tmp, "sbt-ci"))
        self.assertTrue(os.access(installed, os.X_OK))
        with open(installed) as f, open(WRAPPER) as g:
            self.assertEqual(f.read(), g.read())


if __name__ == "__main__":
    unittest.main()
