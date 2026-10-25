// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import org.apache.spark.sql.SparkSession

/** A real Spark job for `DeclaredOutputsSpec`, run in a JVM of its own so its exit status can be read: Invaract attached by
  * configuration alone (`spark.sql.extensions`), the contract and `failOnMissingOutput` named on the command line. It writes
  * `report`, and `audit` too only when asked, then returns without stopping its session, as many jobs do.
  *
  * Arguments: contract path, output folder, `failOnMissingOutput` (`true`/`false`), whether to write the audit (`true`/`false`).
  */
object MissingOutputJob {
  def main(args: Array[String]): Unit = {
    val Array(contract, out, failOnMissing, writeAudit) = args
    val spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("MissingOutputJob")
      .config("spark.sql.extensions", classOf[InvaractSparkSessionExtension].getName)
      .config("spark.invaract.contract", contract)
      .config("spark.invaract.failOnMissingOutput", failOnMissing)
      .config("spark.ui.enabled", "false")
      .getOrCreate()
    spark.range(3).write.parquet(s"$out/report")
    if (writeAudit.toBoolean) spark.range(3).write.parquet(s"$out/audit")
  }
}
