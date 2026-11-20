// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import com.invaract.verification.ContractViolationException
import org.apache.spark.sql.SparkSession

/** A job that tries to shed Invaract, run in a JVM of its own by `InvaractSparkPluginJobSpec`: its own code sets
  * `spark.sql.extensions` (as the Delta quick-start says to) and `spark.invaract.dryRun=true`. The platform attaches
  * Invaract with `spark.plugins` (`plugins`, the first argument) and pins the contract and `dryRun=false` through
  * `INVARACT_PINNED_CONF`. Exits 3 when Invaract refuses the write, 0 when the write lands.
  */
object PinnedAttachJob {
  def main(args: Array[String]): Unit = {
    val Array(plugins, out) = args
    val spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("PinnedAttachJob")
      .config("spark.plugins", plugins)
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.invaract.dryRun", "true")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
    val status =
      try { spark.range(3).write.parquet(out); println("PinnedAttachJob: WRITTEN"); 0 }
      catch {
        case e: ContractViolationException =>
          println("PinnedAttachJob: REFUSED " + e.result.violations.map(_.violationType).mkString(","))
          3
      }
    spark.stop()
    sys.exit(status)
  }
}
