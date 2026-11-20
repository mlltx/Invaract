// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import com.invaract.verification.InvaractConf
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession

import java.nio.file.{Files, StandardCopyOption}
import java.util.concurrent.ConcurrentHashMap
import scala.util.control.NonFatal

/** The settings that name a file (the contract, the organizational policy and its overlays, the location map, the
  * notification configuration) accept any Hadoop file system URI on Spark (docs/SPARK_ADAPTER_HARDENING_PLAN.md, H-15):
  * `s3a://`, `hdfs://`, `abfss://`, `gs://`, `dbfs:/`, `file:/`. They used to be read as local paths only, so even a
  * `file:` URI failed, and in cluster deploy mode - where the driver runs on a machine the submitter does not control -
  * every file had to be shipped with `--files`. A plain path keeps working exactly as before.
  *
  * A remote file is copied to a local temporary file, with the session's Hadoop configuration (its credentials and
  * endpoints), once per session; the setting then reads as that copy's path.
  */
private[sparkadapter] object RemoteFiles {

  /** The settings whose value names one file. */
  val FileSettings: Set[String] = Set(InvaractConf.Contract, InvaractConf.OrgPolicy, InvaractConf.LocationMap, InvaractConf.NotifyConfig)

  /** The setting whose value names several, comma-separated. */
  val FileListSetting: String = InvaractConf.OrgPolicyOverlays

  private val UrlScheme = "^[A-Za-z][A-Za-z0-9+.-]+:/.*".r

  /** Whether `value` names a file through a URL scheme (`s3a://...`, `hdfs://...`, `file:/...`, `dbfs:/...`). A scheme
    * is two letters or more, so a Windows drive (`C:/x`) is a plain path; `registry://` and `ref://` are references
    * Invaract resolves itself, not files. */
  def isRemote(value: String): Boolean =
    UrlScheme.pattern.matcher(value).matches() && !value.startsWith("registry://") && !value.startsWith("ref://")

  /** A local copy of `value`, read with `session`'s Hadoop configuration; `value` itself if it is a plain path.
    * @param setting the setting's name, for the error when the file cannot be read */
  def localize(setting: String, value: String, session: SparkSession): String =
    if (!isRemote(value)) value
    else
      try {
        val path = new Path(value)
        val fs = path.getFileSystem(session.sparkContext.hadoopConfiguration)
        val copy = Files.createTempFile("invaract-", "-" + path.getName)
        copy.toFile.deleteOnExit()
        val in = fs.open(path)
        try Files.copy(in, copy, StandardCopyOption.REPLACE_EXISTING)
        finally in.close()
        copy.toString
      } catch {
        case NonFatal(e) => throw new IllegalArgumentException(s"'$setting' names '$value', which could not be read: $e", e)
      }

  // Each session's copies, by the value that named them, so a file is fetched once per session.
  private val copies = java.util.Collections.synchronizedMap(new java.util.WeakHashMap[SparkSession, ConcurrentHashMap[String, String]]())

  /** `value` of the setting `name` (a neutral name, `InvaractConf`), with every file it names made local. */
  def localizeSetting(name: String, key: String, value: String, session: SparkSession): String = {
    def local(one: String): String =
      if (!isRemote(one)) one
      else copies.computeIfAbsent(session, _ => new ConcurrentHashMap[String, String]()).computeIfAbsent(one, v => localize(key, v, session))
    if (FileSettings.contains(name)) local(value.trim)
    else if (name == FileListSetting) value.split(',').map(_.trim).filter(_.nonEmpty).map(local).mkString(",")
    else value
  }
}
