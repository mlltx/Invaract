// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import java.util.{Collections, Map => JMap}

import org.apache.spark.SparkContext
import org.apache.spark.api.plugin.{DriverPlugin, ExecutorPlugin, PluginContext, SparkPlugin}
import org.slf4j.LoggerFactory

/** Attaches Invaract so that it cannot be left out by accident (docs/SPARK_ADAPTER_HARDENING_PLAN.md, H-07):
  *
  * {{{
  * spark-submit --jars invaract-spark-adapter-<version>.jar \
  *   --conf spark.plugins=com.invaract.sparkadapter.InvaractSparkPlugin \
  *   --conf spark.invaract.contract=/path/to/contract.yaml  my-job.py
  * }}}
  *
  * Attached through `spark.sql.extensions` alone, Invaract is missing without a sound when the class name is misspelt
  * or the jar is absent (Spark logs one warning and carries on), and when the job's own code sets
  * `spark.sql.extensions` - as the Delta and Iceberg quick-starts tell people to - since that replaces the list
  * `spark-submit` gave. Spark refuses to start an application whose `spark.plugins` names a class it cannot load, and
  * this plugin's driver half runs before any `SparkSession` exists: it adds `InvaractSparkSessionExtension` to
  * whatever `spark.sql.extensions` the application ends up with, and puts back any pinned setting (`PinnedSettings`)
  * the job's configuration changed.
  */
class InvaractSparkPlugin extends SparkPlugin {
  override def driverPlugin(): DriverPlugin = new InvaractSparkPlugin.Driver
  override def executorPlugin(): ExecutorPlugin = null
}

object InvaractSparkPlugin {
  private val logger = LoggerFactory.getLogger(classOf[InvaractSparkPlugin])

  val ExtensionsConfKey = "spark.sql.extensions"
  val ExtensionClassName: String = classOf[InvaractSparkSessionExtension].getName

  /** `current` (a comma-separated `spark.sql.extensions` list) with Invaract's extension in it: unchanged when it is
    * already listed, otherwise with it added at the end. */
  def withInvaract(current: String): String = {
    val listed = current.split(',').map(_.trim).filter(_.nonEmpty).toList
    if (listed.contains(ExtensionClassName)) listed.mkString(",") else (listed :+ ExtensionClassName).mkString(",")
  }

  /** What the driver half does to the application's configuration, as `get`/`set` on it. */
  private[sparkadapter] def attach(get: String => Option[String], set: (String, String) => Unit, pins: Map[String, String]): Unit = {
    val current = get(ExtensionsConfKey).getOrElse("")
    val wanted = withInvaract(current)
    if (!current.split(',').map(_.trim).contains(ExtensionClassName)) {
      set(ExtensionsConfKey, wanted)
      logger.warn(s"Invaract: $ExtensionsConfKey did not name Invaract (the job's own configuration may have replaced it); added it.")
    }
    pins.toList.sortBy(_._1).foreach { case (key, value) =>
      if (!get(key).contains(value)) {
        set(key, value)
        logger.warn(s"Invaract: the job's configuration set $key, which the platform pins (${PinnedSettings.EnvVar}); the pinned value is used.")
      }
    }
  }

  private[sparkadapter] class Driver extends DriverPlugin {
    override def init(sc: SparkContext, ctx: PluginContext): JMap[String, String] = {
      val conf = ctx.conf()
      attach(key => conf.getOption(key), (key, value) => conf.set(key, value), PinnedSettings.current)
      Collections.emptyMap[String, String]()
    }
  }
}
