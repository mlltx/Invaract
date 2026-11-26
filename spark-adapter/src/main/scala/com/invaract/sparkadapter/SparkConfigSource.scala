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

  /** A setting the platform pinned (`PinnedSettings`) first, then the session's. A setting that names a file may name
    * it by any Hadoop file system URI: it reads as a local copy (`RemoteFiles`). */
  def apply(session: SparkSession): ConfigSource = new ConfigSource {
    def get(name: String): Option[String] = {
      val key = Prefix + name
      PinnedSettings.get(key).orElse(session.conf.getOption(key)).map(RemoteFiles.localizeSetting(name, key, _, session))
    }
    override def describe(name: String): String = Prefix + name
    override def environment(name: String): Option[String] =
      SparkConfigSource.OrchestratorRuns.get(name).flatMap { case (namespace, job, run) =>
        List(namespace, job, run).map(k => session.conf.getOption(k).map(_.trim).filter(_.nonEmpty)) match {
          case List(Some(n), Some(j), Some(r)) => Some(s"$n/$j/$r")
          case _                               => None
        }
      }.orElse(super.environment(name))
  }

  /** The OpenLineage parent and root runs as an orchestrator hands them to a Spark job (the keys Airflow's OpenLineage
    * provider injects, and openlineage-spark reads), by the environment variable each stands for: `<namespace>/<job>/<run>`
    * when all three are set. */
  val OrchestratorRuns: Map[String, (String, String, String)] = Map(
    "OPENLINEAGE_PARENT_ID" ->
      (("spark.openlineage.parentJobNamespace", "spark.openlineage.parentJobName", "spark.openlineage.parentRunId")),
    "OPENLINEAGE_ROOT_PARENT_ID" ->
      (("spark.openlineage.rootParentJobNamespace", "spark.openlineage.rootParentJobName", "spark.openlineage.rootParentRunId"))
  )
}

/** The `spark.invaract.*` keys Invaract reads, so that one it does not read - almost always a typo, such as
  * `spark.invaract.rejectUndeclaredInput` for `rejectUndeclaredInputs` - is refused rather than silently ignored. A
  * mistyped strict-mode flag left unnoticed would leave a platform believing a check is on when it is not.
  *
  * Known: every neutral `InvaractConf` name under `SparkConfigSource.Prefix`, and any key in the two per-key families
  * dry-run reporting reads (`spark.invaract.job.metadata.<key>`, `spark.invaract.contract.metadata.<key>`).
  */
private[sparkadapter] object SparkSettingNames {
  private val logger = org.slf4j.LoggerFactory.getLogger(SparkSettingNames.getClass)

  /** The settings page every refusal points at. */
  val SettingsPage = "https://mlltx.github.io/Invaract/reference/configuration/"

  /** How far (in single-character edits, ignoring case) a key may be from a known one to be offered as its spelling. */
  val MaxSuggestionDistance = 3

  val Known: List[String] = com.invaract.verification.InvaractConf.All.map(SparkConfigSource.Prefix + _)

  private val Families: List[String] = List(DryRunReporter.JobMetadataConfPrefix, DryRunReporter.ContractMetadataConfPrefix)

  def isKnown(key: String): Boolean = Known.contains(key) || Families.exists(key.startsWith)

  /** The `spark.invaract.*` keys in `conf` that Invaract does not read, in key order, each with the nearest key it does
    * read when one is close enough to be what was meant.
    */
  def unknown(conf: Map[String, String]): List[(String, Option[String])] =
    conf.keys.toList.sorted.filter(k => k.startsWith(SparkConfigSource.Prefix) && !isKnown(k)).map(k => k -> nearest(k))

  /** The known key closest to `key` ignoring case, if within `MaxSuggestionDistance`; ties go to the alphabetically first. */
  def nearest(key: String): Option[String] = {
    val lower = key.toLowerCase(java.util.Locale.ROOT)
    Known
      .map(k => (com.invaract.contract.Suggestions.distance(lower, k.toLowerCase(java.util.Locale.ROOT)), k))
      .filter(_._1 <= MaxSuggestionDistance)
      .sorted
      .headOption
      .map(_._2)
  }

  /** One line per unknown key, then where the settings are listed. */
  def describe(unknown: List[(String, Option[String])]): String =
    unknown.map { case (key, suggestion) => s"Invaract does not read '$key'." + suggestion.fold("")(s => s" Did you mean '$s'?") }
      .mkString("", "\n", s"\n(The settings Invaract reads: $SettingsPage)")

  // Sessions already checked, so a session's settings are read once, not on every rule built for it.
  private val checked = java.util.Collections.synchronizedMap(new java.util.WeakHashMap[SparkSession, java.lang.Boolean]())

  /** Refuses (enforcement) or warns about (dry-run, which never stops a job) the unknown keys in `session`'s settings.
    * A refusal is not remembered: Spark builds the rule again for the next query, which is refused the same way.
    */
  def check(session: SparkSession, enforcing: Boolean): Unit =
    if (!checked.containsKey(session)) {
      val found = unknown(session.conf.getAll)
      if (found.nonEmpty && enforcing) throw new IllegalArgumentException(describe(found))
      if (found.nonEmpty) logger.warn(describe(found) + "\nDry-run mode carries on; these settings have no effect.")
      checked.put(session, java.lang.Boolean.TRUE)
    }
}
