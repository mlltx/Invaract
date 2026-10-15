// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.beamadapter

import com.invaract.contract.{CatalogTechnologies, Formats, LogicalSchema}
import com.invaract.ir.CatalogIdentity
import com.invaract.verification.LocationMatching
import org.apache.beam.sdk.io.WriteFiles
import org.apache.beam.sdk.options.ValueProvider
import org.apache.beam.sdk.transforms.Contextful
import org.yaml.snakeyaml.Yaml

import scala.collection.JavaConverters._
import scala.util.Try

/** A step that brings data into the pipeline from outside it. */
private[beamadapter] sealed trait IoRead
private[beamadapter] object IoRead {

  /** A read whose source the step states: `locations` (one per topic or file pattern), in the canonical location form.
    * `schema` is the dataset's own schema when the step states it (a Parquet or Avro read names the file schema, which a
    * projection narrows only for the job); `None` falls back to the columns of what the read produces. */
  final case class Located(locations: List[String], format: Option[String], catalog: Option[CatalogIdentity] = None, schema: Option[LogicalSchema] = None)
      extends IoRead

  /** A reader of files (`ParquetIO.readFiles()`, `TextIO.readFiles()`, ...) handed to it by an upstream step: located by the
    * `FileIO.match()` that feeds it, if one does. */
  final case class FileReader(format: Option[String], schema: Option[LogicalSchema] = None) extends IoRead

  /** A read from outside the pipeline whose source the step does not state (a query, a source class it does not know, file
    * patterns the job computes): seen, so never mistaken for data the job makes itself, but not located. */
  final case class Unlocated(what: String) extends IoRead
}

/** A step that sends data out of the pipeline, other than a file write (`BeamIO.fileWrite`). */
private[beamadapter] sealed trait IoWrite
private[beamadapter] object IoWrite {

  /** `schema` is the one the write itself declares (a BigQuery table schema); `None` falls back to the data's own schema. */
  final case class Located(
      location: String,
      format: Option[String],
      saveMode: Option[String] = None,
      catalog: Option[CatalogIdentity] = None,
      schema: Option[LogicalSchema] = None
  ) extends IoWrite

  final case class Unverifiable(why: String) extends IoWrite
}

/** Every IO transform the adapter recognises, and what it reads from each one's display data - the one place that knows Beam's
  * IO class names, its display-data keys and the two non-public members it reads by reflection (`FileIO.Write`'s destination and
  * sink, `JdbcIO.Write`'s connection URL). Those are not a stable API: they are held to the Beam releases in
  * `supported-beam-versions.properties` by CI's beam-version-matrix job, and a change in a later release makes the step fail
  * closed (an unrecognised read is not located, an unreadable write is refused), never guessed (docs/BEAM_ADAPTER.md, D2).
  */
private[beamadapter] object BeamIO {

  def simpleName(step: BeamStep): String = step.className.substring(step.className.lastIndexOf('.') + 1)

  // --- reads ---

  /** Reads stated by a file pattern (`filePattern` display item), with the format of the IO. */
  private val PatternReads: Map[String, Option[String]] = Map(
    "AutoValue_ParquetIO_Read" -> Some(Formats.Parquet),
    "AutoValue_ParquetIO_Parse" -> Some(Formats.Parquet),
    "AutoValue_TextIO_Read" -> Some(Formats.Text),
    "AutoValue_AvroIO_Read" -> Some(Formats.Avro),
    "AutoValue_AvroIO_Parse" -> Some(Formats.Avro),
    "AutoValue_TFRecordIO_Read" -> Some("tfrecord"),
    // FileIO.match(): the files it matches are read by whatever consumes them, so the format is the reader's
    "AutoValue_FileIO_Match" -> None
  )

  /** Readers of files matched upstream (`FileIO.readMatches()` output), with their format. */
  private val FileReaders: Map[String, Option[String]] = Map(
    "AutoValue_ParquetIO_ReadFiles" -> Some(Formats.Parquet),
    "AutoValue_ParquetIO_ParseFiles" -> Some(Formats.Parquet),
    "AutoValue_TextIO_ReadFiles" -> Some(Formats.Text),
    "AutoValue_AvroIO_ReadFiles" -> Some(Formats.Avro),
    "AutoValue_AvroIO_ParseFiles" -> Some(Formats.Avro),
    "AutoValue_TFRecordIO_ReadFiles" -> Some("tfrecord")
  )

  /** Reads whose source is computed by the job (from a collection of patterns or queries), so no step states it. */
  private val ComputedReads: Map[String, String] = Map(
    "AutoValue_FileIO_MatchAll" -> "files whose patterns the job computes (FileIO.matchAll)",
    "AutoValue_TextIO_ReadAll" -> "files whose patterns the job computes (TextIO.readAll)",
    "AutoValue_AvroIO_ReadAll" -> "files whose patterns the job computes (AvroIO.readAll)",
    "AutoValue_JdbcIO_ReadAll" -> "JDBC queries the job computes (JdbcIO.readAll)",
    "AutoValue_KafkaIO_ReadSourceDescriptors" -> "Kafka topics the job computes (KafkaIO.readSourceDescriptors)",
    "AutoValue_BigQueryIO_TypedReadAll" -> "BigQuery tables the job computes"
  )

  /** The read `step` is, if it is one. Composites are asked before what they contain, so the outermost recognised step wins. */
  def read(step: BeamStep): Option[IoRead] = {
    val name = simpleName(step)
    // the file schema a Parquet or Avro read states (`schema`), not its projection
    lazy val fileSchema = step.display.get("schema").flatMap(BeamSchemas.fromAvroJson)
    PatternReads.get(name).flatMap(format => step.display.get("filePattern").map(p => IoRead.Located(List(BeamLocations.folderOf(p)), format, schema = fileSchema)))
      .orElse(FileReaders.get(name).map(format => IoRead.FileReader(format, fileSchema)))
      .orElse(ComputedReads.get(name).map(IoRead.Unlocated))
      .orElse(name match {
        case "AutoValue_BigQueryIO_TypedRead" => Some(bigQueryRead(step))
        case "KafkaIO$TypedWithoutMetadata" | "AutoValue_KafkaIO_Read" => Some(kafkaRead(step))
        case "AutoValue_JdbcIO_Read" | "AutoValue_JdbcIO_ReadRows" | "AutoValue_JdbcIO_ReadWithPartitions" => Some(jdbcRead(step))
        case "AutoValue_PubsubIO_Read" => Some(pubsubRead(step))
        // Read.from(source): any bounded or unbounded source, including one an IO above wraps (asked first, so never here),
        // except the sources Beam uses for data the job makes itself (Create.of, GenerateSequence), which read nothing outside it
        case "Read$Bounded" | "Read$Unbounded" if !step.display.get("source").exists(isInJobSource) =>
          Some(IoRead.Unlocated(s"a read from ${step.display.getOrElse("source", "a source")} (Read.from), whose source states no location"))
        case _ => None
      })
  }

  private def bigQueryRead(step: BeamStep): IoRead =
    step.display.get("table").flatMap(BeamLocations.bigQueryTable) match {
      case Some(table) => IoRead.Located(List(table.location), Some(Formats.BigQuery), Some(table.catalog))
      case None =>
        if (step.display.contains("query")) IoRead.Unlocated("a BigQuery query, whose tables are named only in its SQL")
        else IoRead.Unlocated("a BigQuery read whose table is not stated")
    }

  private def kafkaRead(step: BeamStep): IoRead =
    (step.display.get("topics").map(_.split(",").map(_.trim).filter(_.nonEmpty).toList).filter(_.nonEmpty), step.display.get("bootstrap.servers")) match {
      case (Some(topics), servers) => IoRead.Located(topics.map(t => BeamLocations.kafkaTopic(servers, t)), Some(Formats.Kafka))
      case (None, _) =>
        if (step.display.contains("topicPattern")) IoRead.Unlocated(s"Kafka topics matching the pattern ${step.display("topicPattern")}")
        else IoRead.Unlocated("a Kafka read whose topics are not stated")
    }

  private def jdbcRead(step: BeamStep): IoRead = {
    val table = step.display.get("table").orElse(step.display.get("query").flatMap(BeamLocations.singleTableOf))
    (step.display.get("jdbcUrl"), table) match {
      case (Some(url), Some(t)) => IoRead.Located(List(BeamLocations.jdbcTable(url, t)), Some(Formats.Jdbc))
      case (Some(_), None) =>
        IoRead.Unlocated(s"a JDBC query that reads more than one plain table${step.display.get("query").map(q => s" ($q)").getOrElse("")}")
      case (None, _) => IoRead.Unlocated("a JDBC read whose connection is not stated")
    }
  }

  private def pubsubRead(step: BeamStep): IoRead =
    step.display.get("topic").orElse(step.display.get("subscription")) match {
      case Some(name) => IoRead.Located(List(LocationMatching.fromParts(name.split('/').toSeq)), Some("pubsub"))
      case None       => IoRead.Unlocated("a Pub/Sub read whose topic or subscription is not stated")
    }

  /** The sources behind `Create.of(...)` and `GenerateSequence`: data made by the job itself, not read from anywhere. */
  private def isInJobSource(sourceClass: String): Boolean =
    sourceClass.endsWith("Create$Values$CreateSource") || sourceClass.contains("CountingSource")

  // --- writes other than file writes ---

  /** The write `step` is, if it is one of the non-file IOs (BigQuery, Kafka, JDBC, Pub/Sub). */
  def write(step: BeamStep): Option[IoWrite] =
    simpleName(step) match {
      case "AutoValue_BigQueryIO_Write"                                     => Some(bigQueryWrite(step))
      case "AutoValue_KafkaIO_Write" | "AutoValue_KafkaIO_WriteRecords"     => Some(kafkaWrite(step))
      case "JdbcIO$Write" | "AutoValue_JdbcIO_Write" | "AutoValue_JdbcIO_WriteVoid" | "AutoValue_JdbcIO_WriteWithResults" =>
        Some(jdbcWrite(step))
      case "AutoValue_PubsubIO_Write" =>
        Some(step.display.get("topic").fold[IoWrite](IoWrite.Unverifiable("a Pub/Sub write whose topic is not stated")) { t =>
          IoWrite.Located(LocationMatching.fromParts(t.split('/').toSeq), Some("pubsub"))
        })
      case _ => None
    }

  /** BigQuery's write dispositions as the canonical save modes (docs/MULTI_ENGINE_ADAPTERS.md, "Write modes"). */
  private val BigQuerySaveModes = Map("WRITE_APPEND" -> "append", "WRITE_TRUNCATE" -> "overwrite", "WRITE_EMPTY" -> "error")

  private def bigQueryWrite(step: BeamStep): IoWrite =
    step.display.get("table").flatMap(BeamLocations.bigQueryTable) match {
      case None => IoWrite.Unverifiable("a BigQuery write whose table is computed by the job (dynamic destinations), so where it lands cannot be told")
      case Some(table) =>
        IoWrite.Located(
          table.location,
          Some(Formats.BigQuery),
          saveMode = step.display.get("writeDisposition").flatMap(BigQuerySaveModes.get),
          catalog = Some(table.catalog),
          // "Custom Schema Function" (a schema the job computes per destination) is not a schema the graph states
          schema = step.display.get("schema").flatMap(BeamSchemas.fromBigQueryJson)
        )
    }

  private def kafkaWrite(step: BeamStep): IoWrite =
    step.withDescendants.flatMap(_.display.get("topic")).headOption match {
      case Some(topic) =>
        IoWrite.Located(BeamLocations.kafkaTopic(step.withDescendants.flatMap(_.display.get("bootstrap.servers")).headOption, topic), Some(Formats.Kafka))
      case None => IoWrite.Unverifiable("a Kafka write whose topic is computed by the job (a ProducerRecord per element)")
    }

  private def jdbcWrite(step: BeamStep): IoWrite = {
    val all = step.withDescendants
    val table = all.flatMap(_.display.get("table")).headOption.orElse(all.flatMap(_.display.get("statement")).flatMap(BeamLocations.insertTableOf).headOption)
    (jdbcUrlOf(step), table) match {
      case (Some(url), Some(t)) => IoWrite.Located(BeamLocations.jdbcTable(url, t), Some(Formats.Jdbc))
      case (None, _)            => IoWrite.Unverifiable("a JDBC write whose connection URL cannot be read")
      case (_, None)            => IoWrite.Unverifiable("a JDBC write whose statement is not a plain INSERT INTO one table")
    }
  }

  /** The connection URL of a `JdbcIO.write()`. It publishes its statement but not its connection: the URL is held by the
    * `DataSourceConfiguration` behind its data-source provider, read here by reflection, `None` on any failure. */
  private def jdbcUrlOf(step: BeamStep): Option[String] =
    step.withDescendants.iterator.map(s => Try(jdbcUrlOfTransform(s.transform)).toOption.flatten).collectFirst { case Some(u) => u }

  private def jdbcUrlOfTransform(t: AnyRef): Option[String] = {
    def field(o: AnyRef, name: String): Option[AnyRef] =
      Iterator.iterate[Class[_]](o.getClass)(_.getSuperclass).takeWhile(_ != null)
        .flatMap(c => Try(c.getDeclaredField(name)).toOption).toSeq.headOption
        .flatMap { f => f.setAccessible(true); Option(f.get(o)) }
    def call(o: AnyRef, name: String): Option[AnyRef] =
      Try { val m = o.getClass.getDeclaredMethod(name); m.setAccessible(true); m.invoke(o) }.toOption.flatMap(Option(_))
    val provider = call(t, "getDataSourceProviderFn").orElse(field(t, "inner").flatMap(i => call(i, "getDataSourceProviderFn")))
    provider.flatMap(p => field(p, "config")).flatMap(c => call(c, "getUrl")).map {
      case v: ValueProvider[_] => String.valueOf(v.get())
      case other               => other.toString
    }
  }

  // --- file writes (WriteFiles) ---

  def isFileWrite(step: BeamStep): Boolean = step.transform.isInstanceOf[WriteFiles[_, _, _]]

  /** The file write a write unit (a step holding exactly one `WriteFiles`) makes: where its files land and in which format. */
  def fileWrite(unit: BeamStep): IoWrite = {
    val all = unit.withDescendants
    val sink = all.map(_.transform).collectFirst { case w: WriteFiles[_, _, _] => w.getSink.getClass.getName }
    val prefix = all.flatMap(_.display.get("filePrefix")).headOption
    def prefixed(format: String, what: String): IoWrite =
      prefix.fold[IoWrite](IoWrite.Unverifiable(s"$what write whose file prefix is not published"))(p => IoWrite.Located(BeamLocations.folder(p), Some(format)))
    sink match {
      case Some("org.apache.beam.sdk.io.TextSink")  => prefixed(Formats.Text, "a text")
      case Some(avro) if avro.endsWith(".AvroSink") => prefixed(Formats.Avro, "an Avro")
      case Some(tf) if tf.endsWith("TFRecordIO$TFRecordSink") => prefixed("tfrecord", "a TFRecord")
      case Some("org.apache.beam.sdk.io.FileIO$Write$ViaFileBasedSink") =>
        fileIoWrite(all) match {
          case Some((location, sinkClass)) => IoWrite.Located(location, formatOfSink(sinkClass))
          case None                        => IoWrite.Unverifiable("a FileIO write whose destination or sink cannot be read")
        }
      case Some(other) => IoWrite.Unverifiable(s"a file write through $other, which this adapter does not recognise")
      case None        => IoWrite.Unverifiable("a write whose sink cannot be read")
    }
  }

  private def formatOfSink(sinkClass: String): Option[String] =
    if (sinkClass.contains("ParquetIO")) Some(Formats.Parquet)
    else if (sinkClass.contains("AvroIO")) Some(Formats.Avro)
    else if (sinkClass.contains("TextIO")) Some(Formats.Text)
    else if (sinkClass.contains("TFRecordIO")) Some("tfrecord")
    else None

  /** `(output directory, sink class)` of a `FileIO.write()`. Beam publishes neither: the writer's display data names only its
    * sink class, and the destination and the sink are held in non-public members of `FileIO.Write` (docs/BEAM_ADAPTER.md,
    * "Friction log"). Any failure returns `None` and the write is refused as unverifiable rather than guessed. A separate
    * temporary directory (`withTempDirectory`) is not the destination, and the output directory is read directly, so it
    * cannot be mistaken for one. */
  private def fileIoWrite(steps: List[BeamStep]): Option[(String, String)] =
    steps.find(_.className.endsWith("AutoValue_FileIO_Write")).flatMap { s =>
      Try {
        val t = s.transform
        def member(name: String): AnyRef = {
          val m = t.getClass.getDeclaredMethod(name)
          m.setAccessible(true)
          m.invoke(t)
        }
        val directory = Option(member("getOutputDirectory")).map(_.asInstanceOf[ValueProvider[String]].get()).filter(_.nonEmpty)
        val sinkFn = member("getSinkFn").asInstanceOf[Contextful[Contextful.Fn[AnyRef, AnyRef]]]
        val sink = sinkFn.getClosure.apply(null, null)
        directory.map(d => BeamLocations.folder(d) -> sink.getClass.getName)
      }.toOption.flatten
    }
}

/** How a Beam IO's own name for its data becomes the canonical location form (`LocationMatching`): a path, or `/`-joined parts. */
private[beamadapter] object BeamLocations {

  final case class BigQueryTable(location: String, catalog: CatalogIdentity)

  def folder(path: String): String = LocationMatching.normalizeDeclared(path).stripSuffix("/")

  /** The folder a read pattern reads from: the pattern without its trailing glob segments, so a glob file name, a
    * partitioned layout (`date=*` then `part-*.parquet`) and a recursive `**` all leave `in/orders` (a contract declares the
    * dataset's folder). A glob in a folder before the last plain segment (a star for the tenant, then `orders`) is left as written:
    * which folder is the dataset is then not the pattern's to say. */
  def folderOf(pattern: String): String = {
    val segments = LocationMatching.normalizeDeclared(pattern).split("/", -1).toList
    val kept = segments.reverse.dropWhile(_.exists("*?[{".contains(_))).reverse
    folder(kept.mkString("/"))
  }

  /** A BigQuery table as Beam states it: a table spec (`proj:ds.table`, `proj.ds.table` or `ds.table`) or its JSON form
    * (`{"projectId":..,"datasetId":..,"tableId":..}`). Located as `proj/ds/table` (`ds/table` when no project is stated, as
    * the job's default project is not known), and registered in the `bigquery` catalog under `[proj, ds]`, `table`. */
  def bigQueryTable(spec: String): Option[BigQueryTable] = {
    val parts: Option[(Option[String], String, String)] =
      if (spec.trim.startsWith("{"))
        Try(new Yaml().load[java.util.Map[String, AnyRef]](spec)).toOption.flatMap { m =>
          def get(k: String) = Option(m.get(k)).map(_.toString).filter(_.nonEmpty)
          for (d <- get("datasetId"); t <- get("tableId")) yield (get("projectId"), d, t)
        }
      else {
        // a legacy spec's project id may itself contain '.' (a domain-scoped project), so it is split at ':' first
        val (project, rest) = spec.trim.lastIndexOf(':') match {
          case -1 => (None, spec.trim)
          case i  => (Some(spec.trim.substring(0, i)), spec.trim.substring(i + 1))
        }
        rest.split('.').toList match {
          case d :: t :: Nil if project.isDefined => Some((project, d, t))
          case p :: d :: t :: Nil if project.isEmpty => Some((Some(p), d, t))
          case d :: t :: Nil => Some((None, d, t))
          case _ => None
        }
      }
    parts.map { case (project, dataset, table) =>
      BigQueryTable(
        LocationMatching.fromParts(project.toSeq ++ Seq(dataset, table)),
        CatalogIdentity(technology = Some(CatalogTechnologies.BigQuery), namespace = project.toList :+ dataset, table = Some(table))
      )
    }
  }

  /** A Kafka topic, located under the cluster it is read from or written to: `broker:9092/orders` (`orders` when no servers are
    * stated). A contract that declares `orders` matches it on any cluster. */
  def kafkaTopic(bootstrapServers: Option[String], topic: String): String = LocationMatching.fromParts(bootstrapServers.toSeq :+ topic)

  /** A table in a JDBC database: the connection URL, then the table (`jdbc:postgresql://db/shop/orders`). */
  def jdbcTable(url: String, table: String): String = LocationMatching.fromParts(Seq(url.stripSuffix("/")) ++ table.split('.').toSeq)

  private val Identifier = """[A-Za-z_][\w$]*|"[^"]+"|`[^`]+`"""
  private val QualifiedName = s"(?:$Identifier)(?:\\.(?:$Identifier))*"

  /** The one table a query reads, when the query is a plain `SELECT ... FROM <table>` with nothing that names another (a join,
    * a sub-query, a second table, a set operation); `None` otherwise, since anything more needs SQL analysis. */
  def singleTableOf(query: String): Option[String] = {
    val q = query.trim.stripSuffix(";").trim
    val plain = s"""(?is)select\\s+.+?\\s+from\\s+($QualifiedName)(?:\\s+(?:as\\s+)?(?!where\\b|group\\b|order\\b|limit\\b|having\\b)[A-Za-z_]\\w*)?(?:\\s+(?:where|group\\s+by|order\\s+by|limit|having)\\b.*)?""".r
    val namesAnother = """(?is).*(\bjoin\b|\bunion\b|\bintersect\b|\bexcept\b|\(\s*select\b|\bfrom\b[^()]*,).*""".r
    q match {
      case namesAnother(_*) => None
      case plain(table)     => Some(table)
      case _                => None
    }
  }

  /** The table a plain `INSERT INTO <table> ...` statement writes. */
  def insertTableOf(statement: String): Option[String] = {
    val insert = s"""(?is)\\s*insert\\s+into\\s+($QualifiedName)\\b.*""".r
    statement match {
      case insert(table) => Some(table)
      case _             => None
    }
  }
}
