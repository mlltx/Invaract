// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.beamadapter

import com.invaract.verification.ContractViolationException
import org.scalatest.funsuite.AnyFunSuite

import java.nio.charset.StandardCharsets
import java.nio.file.Files

/** The hand-over to a production runner: the unmodified example job, attached from its command line only, verified by
  * `InvaractRunner` and then run on Beam's Flink runner (a local Flink mini-cluster in this JVM, `--flinkMaster=[local]`), the
  * runner a platform would name with `--invaractDelegate=FlinkRunner`. The runner's own options travel with the job's. */
class BeamProductionRunnerSpec extends AnyFunSuite {

  private def withJob(test: (ExampleJob, java.nio.file.Path, java.nio.file.Path) => Unit): Unit = {
    val root = Files.createTempDirectory("beam-flink-")
    try {
      ExampleJob.withOrders(root, (1L, 10L), (2L, 20L))
      val contract = root.resolve("contract.yaml")
      Files.write(contract, ExampleJob.contractYaml().getBytes(StandardCharsets.UTF_8))
      test(new ExampleJob(root), root, contract)
    } finally BeamConformanceAdapter.deleteQuietly(root)
  }

  test("a pipeline that keeps to the contract is verified, then runs on the Flink runner and writes its output") {
    withJob { (job, _, contract) =>
      job.main(Array("--runner=Invaract", s"--invaractContract=$contract", "--invaractDelegate=FlinkRunner", "--flinkMaster=[local]", "--parallelism=1"))
      assert(job.written.nonEmpty, "the Flink runner ran the pipeline it was handed")
    }
  }

  test("a pipeline that breaks the contract never reaches the Flink runner") {
    withJob { (_, root, contract) =>
      val elsewhere = new ExampleJob(root, outputFolder = "out/elsewhere")
      intercept[ContractViolationException](
        elsewhere.main(Array("--runner=Invaract", s"--invaractContract=$contract", "--invaractDelegate=FlinkRunner", "--flinkMaster=[local]"))
      )
      assert(elsewhere.written.isEmpty)
    }
  }
}
