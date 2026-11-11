// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.beamadapter

import org.apache.beam.sdk.io.{Compression, FileIO}
import org.apache.beam.sdk.transforms.windowing.{BoundedWindow, PaneInfo}
import org.apache.beam.sdk.transforms.{Contextful, Requirements}
import org.apache.beam.sdk.values.Row

import scala.annotation.varargs

/** A Hive-style partitioned file write for Beam, in a form Invaract can check (`check.partitioning`, docs/BEAM_ADAPTER.md, D23).
  *
  * Beam has no file writer that lays out folders by column values: a team writes one with `FileIO.writeDynamic()`, whose
  * destination and file naming are functions of its own, which the pipeline graph cannot see into. These two are such
  * functions, written once, that the adapter recognises and reads the keys of:
  *
  * {{{
  * rows.apply(FileIO.<String, Row>writeDynamic()
  *     .by(HivePartitions.by("day", "region"))
  *     .withNaming(HivePartitions.naming(".parquet"))
  *     .withDestinationCoder(StringUtf8Coder.of())
  *     .via(sink)
  *     .to("out/revenue"));
  * }}}
  *
  * writes `out/revenue/day=2026-11-04/region=north/part-...parquet`, as Spark lays it out: each key a folder, in order, its
  * value escaped as Spark escapes it, and an empty value `__HIVE_DEFAULT_PARTITION__`. Unlike Spark's, the files keep the key
  * columns too. The elements are Beam rows (`Row`) or Avro records (anything with a `get(String)`), read by field name.
  */
object HivePartitions {

  /** The folder Spark and Hive give a row whose key is missing. */
  val DefaultPartition = "__HIVE_DEFAULT_PARTITION__"

  /** The destination of each element: its partition folder, `day=2026-11-04/region=north` for `by("day", "region")`. */
  @varargs def by[T](fields: String*): Contextful[Contextful.Fn[T, String]] = {
    require(fields.nonEmpty, "a partitioned write names at least one partition key")
    Contextful.of(new Key[T](fields.toList), Requirements.empty())
  }

  /** How the files of each partition are named: under the partition's folder, `part-...` ending in `suffix`. */
  def naming(suffix: String): Contextful[Contextful.Fn[String, FileIO.Write.FileNaming]] = Contextful.of(new Naming(suffix), Requirements.empty())

  /** The partition folder of an element, by the values of `fields`, in order. */
  final class Key[T] private[HivePartitions] (val fields: List[String]) extends Contextful.Fn[T, String] {
    override def apply(element: T, c: Contextful.Fn.Context): String =
      fields.map(field => s"$field=${folderValue(valueOf(element, field))}").mkString("/")
  }

  /** Files named `part-<window>-<pane>-<shard>-of-<shards><suffix>` within each partition's folder, which the write resolves
    * against its own directory (`to(...)`). */
  final class Naming private[HivePartitions] (val suffix: String) extends Contextful.Fn[String, FileIO.Write.FileNaming] {
    override def apply(folder: String, c: Contextful.Fn.Context): FileIO.Write.FileNaming = new InFolder(folder, FileIO.Write.defaultNaming("part", suffix))
  }

  private final class InFolder(folder: String, inner: FileIO.Write.FileNaming) extends FileIO.Write.FileNaming {
    override def getFilename(window: BoundedWindow, pane: PaneInfo, numShards: Int, shardIndex: Int, compression: Compression): String =
      s"$folder/${inner.getFilename(window, pane, numShards, shardIndex, compression)}"
  }

  private def valueOf(element: Any, field: String): Any = element match {
    case row: Row => row.getValue[Any](field)
    case other    => other.getClass.getMethod("get", classOf[String]).invoke(other, field)
  }

  /** A key's value as a folder name, escaped as Spark escapes it (`ExternalCatalogUtils.escapePathName`). */
  private[beamadapter] def folderValue(value: Any): String =
    Option(value).map(_.toString).filter(_.nonEmpty).fold(DefaultPartition)(_.flatMap { ch =>
      if (ch < ' ' || ch == '\u007F' || "\"#%'*/:=?\\{[]^".indexOf(ch) >= 0) f"%%${ch.toInt}%02X" else ch.toString
    })
}
