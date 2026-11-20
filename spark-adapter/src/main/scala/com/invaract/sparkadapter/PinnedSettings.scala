// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import java.nio.file.{Files, Paths}
import java.util.Properties

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

/** Invaract settings a platform pins, so the job they are applied to cannot change them through its own Spark
  * configuration (docs/SPARK_ADAPTER_HARDENING_PLAN.md, H-07). A job's `SparkSession.builder.config(k, v)` replaces the
  * same key given by `spark-submit --conf`, and `spark.conf.set` can change it later, so without this a job could turn
  * on `spark.invaract.dryRun`, swap `spark.invaract.contract` for its own, or empty `spark.invaract.orgPolicy`.
  *
  * The environment variable `INVARACT_PINNED_CONF` names a `.properties` file of settings spelt as on Spark
  * (`spark.invaract.contract=...`). `SparkConfigSource` looks every key up here first. An environment variable is the
  * one input a job's Spark calls cannot overwrite, and every scheduler can set it for the driver
  * (`spark.yarn.appMasterEnv.INVARACT_PINNED_CONF`, `spark.kubernetes.driverEnv.INVARACT_PINNED_CONF`,
  * `spark-env.sh`). A JVM system property is deliberately not read: job code can set one.
  *
  * This does not stop code that sets out to defeat Invaract (a job's code runs with every permission its driver has);
  * it stops a job's configuration from loosening what the platform set.
  */
private[sparkadapter] object PinnedSettings {

  val EnvVar = "INVARACT_PINNED_CONF"

  /** The pinned settings `env` names: none when the variable is unset or empty. A file that cannot be read, or that pins
    * a key Invaract does not read (a typo would otherwise pin nothing), stops the job: pins a platform relies on must
    * never be silently missing.
    */
  def load(env: Map[String, String]): Map[String, String] = env.get(EnvVar).map(_.trim).filter(_.nonEmpty) match {
    case None => Map.empty
    case Some(path) =>
      val properties = new Properties()
      try {
        val in = Files.newInputStream(Paths.get(path))
        try properties.load(in)
        finally in.close()
      } catch {
        case NonFatal(e) => throw new IllegalStateException(s"Invaract: $EnvVar names '$path', which cannot be read: $e", e)
      }
      val pins = properties.stringPropertyNames.asScala.toList.map(key => key -> properties.getProperty(key).trim).toMap
      pins.keys.toList.sorted.find(!_.startsWith(SparkConfigSource.Prefix)).foreach { key =>
        throw new IllegalArgumentException(
          s"Invaract: '$path' (named by $EnvVar) pins '$key', which is not an Invaract setting: every key starts with ${SparkConfigSource.Prefix}"
        )
      }
      val unknown = SparkSettingNames.unknown(pins)
      if (unknown.nonEmpty) throw new IllegalArgumentException(s"Invaract: in '$path' (named by $EnvVar):\n" + SparkSettingNames.describe(unknown))
      pins
  }

  private lazy val fromEnvironment: Map[String, String] = load(sys.env)

  @volatile private var testOverride: Option[Map[String, String]] = None

  /** The pinned settings of this driver. */
  def current: Map[String, String] = testOverride.getOrElse(fromEnvironment)

  def get(key: String): Option[String] = current.get(key)

  /** `body` with `pins` in place of the environment's. */
  private[sparkadapter] def withPinsForTest[T](pins: Map[String, String])(body: => T): T = {
    val previous = testOverride
    testOverride = Some(pins)
    try body
    finally testOverride = previous
  }
}
