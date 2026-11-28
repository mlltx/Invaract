// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.pythonioadapter

import org.scalatest.funsuite.AnyFunSuite

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters._

/** How Invaract is switched on for a Python job without touching its source (CLAUDE.md's External Attachability Requirement,
  * docs/PYTHONIO_ATTACH_DESIGN.md): the `invaract` wheel installs a start-up hook (`00_invaract_attach.pth`), idle until the
  * platform sets `INVARACT_*` variables, and `INVARACT_AUTO_RUN` opens the run the job never opens itself. The tests here are
  * the ones `PythonIoAttestedClaimsSpec` names for `config.zeroCodeInstall`, and the limit that makes it `partial`.
  *
  * Python reads a `.pth` file only from an installed package's folder, so each job runs the wheel's `.pth` line, read from
  * the file itself, as a `sitecustomize` at the same point of start-up (`python/tests/test_attach.py` does the same; the
  * built wheel's own `.pth` is checked by `.github/scripts/python_wheel.py`). The package and its `lint` extra are on the
  * job's path as `pip install invaract[lint]` leaves them. The jobs need the Python environment
  * `PythonIoPackageConformanceAdapter` uses, and are canceled without it (failed under `INVARACT_CONFORMANCE_STRICT=true`,
  * as CI runs them).
  */
class PythonIoAttachSpec extends AnyFunSuite {
  import PythonIoAttachSpec._
  import PythonIoPackageConformanceAdapter.{Python, PythonPath, checkerCommand, deleteQuietly, environment, write}

  private def needsPython(): Unit = {
    if (File.separatorChar == '\\') cancel("the jobs use POSIX paths")
    environment.left.foreach { why =>
      if (sys.env.get("INVARACT_CONFORMANCE_STRICT").contains("true")) fail(why) else cancel(why)
    }
  }

  /** Runs `source` as `job.py` in a folder of its own holding `contract.yaml` and an `orders` input of two rows, with the
    * start-up hook installed and `variables` set (and no other `INVARACT_*` setting). The temporary folder is elsewhere:
    * strict mode takes what is written there for scratch, not data. Returns the folder, the exit status and the output. */
  private def job(source: String, variables: Map[String, String]): (Path, Int, String) = {
    val root = Files.createTempDirectory("pythonio-attach-")
    val (folder, hook, scratch) = (root.resolve("job"), root.resolve("hook"), root.resolve("tmp"))
    List(folder, hook, scratch).foreach(Files.createDirectories(_))
    Files.write(hook.resolve("sitecustomize.py"), Files.readAllBytes(new File(PythonPath, "00_invaract_attach.pth").toPath))
    write(folder, "contract.yaml", Contract)
    write(folder, "job.py", source)
    val path = List(hook.toString, PythonPath, new File("lint").getAbsolutePath).mkString(File.pathSeparator)
    def python(args: List[String], settings: Map[String, String]): (Int, String) = {
      val builder = new ProcessBuilder((Python :: args).asJava).redirectErrorStream(true).directory(folder.toFile)
      val env = builder.environment()
      env.keySet().removeIf(_.startsWith("INVARACT_"))
      env.put("PYTHONPATH", path)
      env.put("PYTHONDONTWRITEBYTECODE", "1")
      env.put("TMPDIR", scratch.toString)
      settings.foreach { case (k, v) => env.put(k, v) }
      val process = builder.start()
      val output = new String(process.getInputStream.readAllBytes(), StandardCharsets.UTF_8)
      if (!process.waitFor(5, TimeUnit.MINUTES)) { process.destroyForcibly(); fail(s"the job did not finish:\n$output") }
      (process.exitValue(), output)
    }
    val (made, why) = python(List("-c", Orders), Map.empty)
    assert(made == 0, why)
    val (status, output) = python(List("job.py"), variables)
    (root, status, output)
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
    // a plain job, written with no thought of Invaract: refused at its first read, so it never writes its copy
    val (raw, refused, why) = job(Raw, platform)
    try {
      assert(refused == 1, why)
      assert(why.contains("[UNGOVERNED_DATA_ACCESS] `pyarrow.parquet.read_table`"), why)
      assert(!why.contains("the job ended"), why)
      assert(!Files.exists(raw.resolve("job/copy.csv")), "nothing was written")
    } finally deleteQuietly(raw)

    // the same settings: a job reading and writing through the package, with no annotation, is checked and published...
    val (kept, status, output) = job(Ambient, platform)
    try {
      assert(status == 0 && output.contains("the job ended"), output)
      assert(Files.isDirectory(kept.resolve("job/data/out/report")), output)
    } finally deleteQuietly(kept)

    // ...and refused, with nothing published, when what it writes breaks the contract
    val (broken, brokenStatus, brokenOutput) = job(MissingColumn, platform)
    try {
      assert(brokenStatus == 1 && brokenOutput.contains("ContractViolation"), brokenOutput)
      assert(!Files.exists(broken.resolve("job/data/out/report")), brokenOutput)
    } finally deleteQuietly(broken)
  }

  test(OwnWritesAreNotChecked) {
    needsPython()
    // the job writes exactly the declared output itself; strict mode can only refuse the write, not check it
    val (root, status, output) = job(RawDeclared, platform)
    try {
      assert(status == 1 && output.contains("[UNGOVERNED_DATA_ACCESS] `pyarrow.parquet.write_table`"), output)
      assert(output.contains("wrote data/out/report/part-0.parquet"), output)
      assert(!output.contains("the job ended"), output)
      assert(!Files.exists(root.resolve("job/data/out/report/part-0.parquet")), output)
    } finally deleteQuietly(root)
  }

  test(IdleWithoutSettings) {
    needsPython()
    val (root, status, output) = job("import sys\nprint('invaract' in sys.modules)\n", Map.empty)
    try assert(status == 0 && output.trim == "False", output)
    finally deleteQuietly(root)
  }
}

object PythonIoAttachSpec {

  /** Named by `PythonIoAttestedClaimsSpec`, shared so a rename cannot leave the attestation dangling. */
  val ZeroCodeInstall =
    "zero-code install: with the package installed, INVARACT_* variables alone attach Invaract to an unmodified job and refuse what the contract does not allow, before it happens"

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

  /** Writes the `orders` input, before the job runs and with no hook asked for. */
  val Orders: String =
    "import os, pyarrow as pa, pyarrow.parquet as pq; os.makedirs('data/in/orders'); " +
      "pq.write_table(pa.table({'region': ['north', 'south'], 'total': [10, 20]}), 'data/in/orders/part-0.parquet')"

  /** No Invaract in it: pyarrow reads the input and writes a copy the contract does not declare. */
  val Raw: String =
    "import pyarrow.csv as pc, pyarrow.parquet as pq\npc.write_csv(pq.read_table('data/in/orders'), 'copy.csv')\nprint('the job ended')\n"

  /** No Invaract in it: pyarrow writes the declared output itself, in the declared shape, from rows of its own. */
  val RawDeclared: String =
    "import os, pyarrow as pa, pyarrow.parquet as pq\nos.makedirs('data/out/report')\n" +
      "pq.write_table(pa.table({'region': ['north'], 'total': [10]}), 'data/out/report/part-0.parquet')\nprint('the job ended')\n"

  /** Reads and writes through the package, and opens no run. Read as an Arrow table: the jobs need nothing but pyarrow. */
  val Ambient: String = "import invaract\ninvaract.write(invaract.read('orders', as_='arrow'), 'report')\nprint('the job ended')\n"

  /** The same, writing a table without the required `total`. */
  val MissingColumn: String =
    "import invaract, pyarrow as pa\ninvaract.write(pa.table({'region': ['north']}), 'report')\nprint('the job ended')\n"
}
