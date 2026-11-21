// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.pythonioadapter

import org.scalatest.funsuite.AnyFunSuite

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters._

/** How Invaract is switched on for a Python job without touching its source (CLAUDE.md's External Attachability Requirement,
  * docs/PYTHONIO_ATTACH_DESIGN.md): the `invaract` wheel installs a start-up hook (`00_invaract_attach.pth`), idle until the
  * platform sets `INVARACT_*` variables, and `INVARACT_AUTO_RUN` opens the run the job never opens itself. The tests here are
  * the ones `PythonIoAttestedClaimsSpec` names for `config.zeroCodeInstall`, and the limits that make it `partial`.
  *
  * Each job runs in a virtual environment of its own holding the wheel's real `.pth` file, so the hook is attached exactly as
  * an installed wheel attaches it (`python_wheel.py` checks the same from the built wheel). The jobs need the Python
  * environment `PythonIoPackageConformanceAdapter` uses, and are canceled without it (failed under
  * `INVARACT_CONFORMANCE_STRICT=true`, as CI runs them).
  */
class PythonIoAttachSpec extends AnyFunSuite {
  import PythonIoAttachSpec._
  import PythonIoPackageConformanceAdapter.{checkerCommand, deleteQuietly, environment, write}

  private def needsPython(): Unit = {
    if (File.separatorChar == '\\') cancel("the jobs use a POSIX virtual environment")
    environment.left.foreach { why =>
      if (sys.env.get("INVARACT_CONFORMANCE_STRICT").contains("true")) fail(why) else cancel(why)
    }
  }

  /** A job folder holding `contract.yaml`, `job.py` and an `orders` input of two rows. */
  private def jobFolder(source: String): Path = {
    val folder = Files.createTempDirectory("pythonio-attach-")
    write(folder, "contract.yaml", Contract)
    write(folder, "job.py", source)
    val (status, out) = run(folder, List("-c",
      "import os, pyarrow as pa, pyarrow.parquet as pq; os.makedirs('data/in/orders'); " +
        "pq.write_table(pa.table({'region': ['north', 'south'], 'total': [10, 20]}), 'data/in/orders/part-0.parquet')"), Map.empty)
    assert(status == 0, out)
    folder
  }

  /** Runs `python args` in `folder` with the installed hook, the package found as an installed one is, and `variables` (and
    * no other `INVARACT_*` setting). */
  private def run(folder: Path, args: List[String], variables: Map[String, String]): (Int, String) = {
    val builder = new ProcessBuilder((installed.python :: args).asJava).redirectErrorStream(true).directory(folder.toFile)
    val env = builder.environment()
    env.keySet().removeIf(_.startsWith("INVARACT_"))
    env.put("PYTHONPATH", PythonIoPackageConformanceAdapter.PythonPath)
    env.put("PYTHONDONTWRITEBYTECODE", "1")
    env.put("TMPDIR", folder.toString)
    variables.foreach { case (k, v) => env.put(k, v) }
    val process = builder.start()
    val output = new String(process.getInputStream.readAllBytes(), StandardCharsets.UTF_8)
    if (!process.waitFor(5, TimeUnit.MINUTES)) { process.destroyForcibly(); fail(s"the job did not finish:\n$output") }
    (process.exitValue(), output)
  }

  private def job(source: String, variables: Map[String, String]): (Path, Int, String) = {
    val folder = jobFolder(source)
    val (status, output) = run(folder, List("job.py"), variables)
    (folder, status, output)
  }

  /** What a platform sets: the contract, a run Invaract opens, and strict mode enforced. Nothing in the job's source. */
  private def platform: Map[String, String] = Map(
    "INVARACT_CHECKER" -> checkerCommand,
    "INVARACT_CONTRACT" -> "contract.yaml",
    "INVARACT_AUTO_RUN" -> "true",
    "INVARACT_STRICT" -> "enforce"
  )

  test(ZeroCodeInstall) {
    needsPython()
    // a plain pandas job, written with no thought of Invaract: its write is not one the contract allows
    val (raw, refused, why) = job(Raw, platform)
    try {
      assert(refused == 1, why)
      assert(why.contains("UNGOVERNED_DATA_ACCESS"), why)
      assert(!why.contains("the job ended"), why)
      assert(!Files.exists(raw.resolve("copy.csv")), "the write was refused before it happened")
    } finally deleteQuietly(raw)

    // the same settings: a job reading and writing through the package, with no annotation, is checked and published...
    val (kept, status, output) = job(Ambient, platform)
    try {
      assert(status == 0 && output.contains("the job ended"), output)
      assert(Files.isDirectory(kept.resolve("data/out/report")), output)
    } finally deleteQuietly(kept)

    // ...and refused, with nothing published, when what it writes breaks the contract
    val (broken, brokenStatus, brokenOutput) = job(MissingColumn, platform)
    try {
      assert(brokenStatus == 1 && brokenOutput.contains("ContractViolation"), brokenOutput)
      assert(!Files.exists(broken.resolve("data/out/report")), brokenOutput)
    } finally deleteQuietly(broken)
  }

  test(OwnWritesAreNotChecked) {
    needsPython()
    // the job's own write is exactly the declared output, yet strict mode can only refuse it: it is not checked
    val (folder, status, output) = job(RawDeclared, platform)
    try {
      assert(status == 1 && output.contains("UNGOVERNED_DATA_ACCESS"), output)
      assert(!Files.exists(folder.resolve("data/out/report")), output)
    } finally deleteQuietly(folder)
  }

  test(IdleWithoutSettings) {
    needsPython()
    val (folder, status, output) = job("import sys\nprint('invaract' in sys.modules)\n", Map.empty)
    try assert(status == 0 && output.trim == "False", output)
    finally deleteQuietly(folder)
  }
}

object PythonIoAttachSpec {

  /** Named by `PythonIoAttestedClaimsSpec`, shared so a rename cannot leave the attestation dangling. */
  val ZeroCodeInstall =
    "zero-code install: with the wheel installed, INVARACT_* variables alone attach Invaract to an unmodified job and refuse a write the contract does not allow, before it happens"

  val OwnWritesAreNotChecked =
    "zero-code install, its limit: a write the job makes itself is refused or reported, never checked against the contract, even when it matches the declared output"

  val IdleWithoutSettings = "zero-code install: with no setting, the installed hook leaves a job alone"

  val Contract: String =
    """id: orders_report
      |version: "1.0.0"
      |inputs:
      |  - { name: orders, location: data/in/orders, format: parquet }
      |outputs:
      |  - name: report
      |    location: data/out/report
      |    format: parquet
      |    saveMode: overwrite
      |    schema:
      |      fields:
      |        - { name: region, type: string, required: true }
      |        - { name: total,  type: long,   required: true }
      |""".stripMargin

  /** No Invaract in it: pandas reads the input and writes a copy the contract does not declare. */
  val Raw: String = "import pandas as pd\npd.read_parquet('data/in/orders').to_csv('copy.csv')\nprint('the job ended')\n"

  /** No Invaract in it: pandas writes the declared output itself, in the declared shape. */
  val RawDeclared: String =
    "import os, pandas as pd\nos.makedirs('data/out/report')\npd.read_parquet('data/in/orders').to_parquet('data/out/report/part-0.parquet')\n" +
      "print('the job ended')\n"

  /** Reads and writes through the package, and opens no run. */
  val Ambient: String = "import invaract\ninvaract.write(invaract.read('orders'), 'report')\nprint('the job ended')\n"

  /** The same, writing a table without the required `total`. */
  val MissingColumn: String =
    "import invaract, pyarrow as pa\ninvaract.write(pa.table({'region': ['north']}), 'report')\nprint('the job ended')\n"

  /** A virtual environment with the wheel's start-up hook in its `site-packages`, and the test Python's own packages on its
    * path (named first, `000_`, so they are there when the hook runs). Made once per test run. */
  final case class Installed(python: String)

  lazy val installed: Installed = {
    val root = Files.createTempDirectory("pythonio-attach-venv-")
    def python(command: String, args: String*): String = {
      val process = new ProcessBuilder((command :: args.toList).asJava).redirectErrorStream(true).start()
      val output = new String(process.getInputStream.readAllBytes(), StandardCharsets.UTF_8)
      process.waitFor(5, TimeUnit.MINUTES)
      if (process.exitValue() != 0) throw new IllegalStateException(s"$command ${args.mkString(" ")} failed:\n$output")
      output
    }
    val venv = root.resolve("venv")
    python(PythonIoPackageConformanceAdapter.Python, "-m", "venv", "--without-pip", venv.toString)
    val venvPython = venv.resolve("bin/python").toString
    val sitePackages = Paths.get(python(venvPython, "-c", "import sysconfig; print(sysconfig.get_paths()['purelib'])").trim)
    val outer = python(PythonIoPackageConformanceAdapter.Python, "-c",
      "import os, sys; print('\\n'.join(p for p in sys.path if p and os.path.isdir(p) and p.rstrip('/').endswith('-packages')))")
    Files.write(sitePackages.resolve("000_test_python_packages.pth"), outer.getBytes(StandardCharsets.UTF_8))
    Files.copy(new File(PythonIoPackageConformanceAdapter.PythonPath, "00_invaract_attach.pth").toPath, sitePackages.resolve("00_invaract_attach.pth"))
    Installed(venvPython)
  }
}
