// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter


import com.invaract.verification.ConfigSource
import org.apache.spark.sql.SparkSession

/** Spark's spelling of the neutral `InvaractConf` names: every Invaract setting
  * lives under `spark.invaract.` - the only prefix `spark-submit --conf` passes
  * through to a job - so `locationMap` is `spark.invaract.locationMap`, exactly
  * as documented and as every existing deployment sets it. Reads the live
  * session's `conf`, at the moment the caller asks (session construction).
  */
private[sparkadapter] object SparkConfigSource {
  val Prefix = "spark.invaract."

  def apply(session: SparkSession): ConfigSource =
    ConfigSource.prefixed(Prefix)(key => session.conf.getOption(key))
}
