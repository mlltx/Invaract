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

  // --- the reads both catalogues (`JavaSteps`, `PythonSteps`) describe alike ---

  /** A read from a classic template's runtime parameter, which has no value until the job runs. */
  def templateParameter(parameter: String): IoRead =
    Unlocated(s"a read whose source is a template parameter ($parameter), known only when the job runs")

  /** A BigQuery read of `table`; without one, a query (whose tables are named only in its SQL) or a read that states no table. */
  def bigQuery(table: Option[BeamLocations.BigQueryTable], isQuery: Boolean): IoRead = table match {
    case Some(t)          => Located(List(t.location), Some(Formats.BigQuery), Some(t.catalog))
    case None if isQuery  => Unlocated("a BigQuery query, whose tables are named only in its SQL")
    case None             => Unlocated("a BigQuery read whose table is not stated")
  }

  /** A Pub/Sub read of `topicOrSubscription` (`projects/p/topics/t`), when it is stated. */
  def pubsub(topicOrSubscription: Option[String]): IoRead =
    topicOrSubscription.fold[IoRead](Unlocated("a Pub/Sub read whose topic or subscription is not stated")) { name =>
      Located(List(BeamLocations.pubsub(name)), Some(BeamFormats.PubSub))
    }
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

  // --- the writes both catalogues (`JavaSteps`, `PythonSteps`) describe alike ---

  /** A write to a classic template's runtime parameter, which has no value until the job runs. */
  def templateParameter(parameter: String): IoWrite =
    Unverifiable(s"a write whose destination is a template parameter ($parameter), known only when the job runs")

  /** A BigQuery write to `table`, with the write disposition and table schema it states. Without a table it cannot be placed. */
  def bigQuery(table: Option[BeamLocations.BigQueryTable], disposition: Option[String], schema: Option[LogicalSchema]): IoWrite =
    table.fold[IoWrite](
      Unverifiable("a BigQuery write whose table is computed by the job (dynamic destinations) or not stated, so where it lands cannot be told")
    ) { t =>
      Located(t.location, Some(Formats.BigQuery), disposition.flatMap(BigQuerySaveModes.get), Some(t.catalog), schema)
    }

  /** BigQuery's write dispositions as the canonical save modes (docs/MULTI_ENGINE_ADAPTERS.md, "Write modes"). */
  private val BigQuerySaveModes = Map("WRITE_APPEND" -> "append", "WRITE_TRUNCATE" -> "overwrite", "WRITE_EMPTY" -> "error")

  /** A Pub/Sub write to `topic` (`projects/p/topics/t`), when it is stated. */
  def pubsub(topic: Option[String]): IoWrite =
    topic.fold[IoWrite](Unverifiable("a Pub/Sub write whose topic is not stated")) { t =>
      Located(BeamLocations.pubsub(t), Some(BeamFormats.PubSub))
    }
}

/** Formats Beam reads and writes that the contract's canonical vocabulary (`Formats`) has no name for, as the adapter reports them. */
private[beamadapter] object BeamFormats {
  val TFRecord = "tfrecord"
  val PubSub = "pubsub"
}

/** Every IO transform the adapter recognises, and what it reads from each one's display data - the one place that knows Beam's
  * IO class names, its display-data keys and the two non-public members it reads by reflection (`FileIO.Write`'s destination and
  * sink, `JdbcIO.Write`'s connection URL). Those are not a stable API: they are held to the Beam releases in
  * `supported-beam-versions.properties` by CI's beam-version-matrix job, and a change in a later release makes the step fail
  * closed (an unrecognised read is not located, an unreadable write is refused), never guessed (docs/BEAM_ADAPTER.md, D2).
  */
private[beamadapter] object BeamIO {

  // --- reads ---

  /** Reads stated by a file pattern (`filePattern` display item), with the format of the IO. */
  private val PatternReads: Map[String, Option[String]] = Map(
    "AutoValue_ParquetIO_Read" -> Some(Formats.Parquet),
    "AutoValue_ParquetIO_Parse" -> Some(Formats.Parquet),
    "AutoValue_TextIO_Read" -> Some(Formats.Text),
    "AutoValue_AvroIO_Read" -> Some(Formats.Avro),
    "AutoValue_AvroIO_Parse" -> Some(Formats.Avro),
    "AutoValue_TFRecordIO_Read" -> Some(BeamFormats.TFRecord),
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
    "AutoValue_TFRecordIO_ReadFiles" -> Some(BeamFormats.TFRecord)
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
  def read(step: BeamStep): Option[IoRead] =
    statedRead(step).map {
      case located: IoRead.Located => runtimeParameter(step).fold[IoRead](located)(IoRead.templateParameter)
      case other                   => other
    }

  /** The keys of the display data an IO names its data by. */
  private val LocationKeys = List("filePattern", "table", "query", "topic", "topics", "subscription", "bootstrap.servers", "jdbcUrl", "statement", "filePrefix")

  /** A value Beam cannot state before the job runs: a classic template's runtime parameter (a `ValueProvider` with no value yet),
    * which display data shows as the provider itself. A Flex Template builds its pipeline with the values, so it has none. */
  def isRuntimeParameter(value: String): Boolean = value.startsWith("RuntimeValueProvider{") || value.startsWith("NestedValueProvider{")

  /** The name of the runtime parameter `step` names its data by, if it does (`inputFile`, or the value as shown). */
  private def runtimeParameter(step: BeamStep): Option[String] =
    LocationKeys.flatMap(step.display.get).find(isRuntimeParameter)
      .map(v => """propertyName=([^,}]+)""".r.findFirstMatchIn(v).fold(v)(_.group(1)))

  private def statedRead(step: BeamStep): Option[IoRead] = {
    val name = step.simpleName
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
        // Impulse, outside the steps that make data in the job: what a splittable DoFn (the modern way to write an IO) reads from
        case "Impulse" => Some(IoRead.Unlocated("a source started from an Impulse (a splittable DoFn or an IO built on one), whose source states no location"))
        // Read.from(source): any bounded or unbounded source, including one an IO above wraps (asked first, so never here); one of
        // the sources Beam uses for data the job makes itself is left out before this is asked (isInJobData)
        case read if SourceReads.contains(read) =>
          Some(IoRead.Unlocated(s"a read from ${step.display.getOrElse("source", "a source")} (Read.from), whose source states no location"))
        case _ => None
      })
  }

  private def bigQueryRead(step: BeamStep): IoRead =
    IoRead.bigQuery(step.display.get("table").flatMap(BeamLocations.bigQueryTable), isQuery = step.display.contains("query"))

  private def kafkaRead(step: BeamStep): IoRead =
    (step.display.get("topics").map(_.split(",").map(_.trim).filter(_.nonEmpty).toList).filter(_.nonEmpty), step.display.get("bootstrap.servers")) match {
      case (Some(topics), servers) => IoRead.Located(topics.map(t => BeamLocations.kafkaTopic(servers, t)), Some(Formats.Kafka))
      // KafkaIO refuses to build a read with neither topics nor a pattern, so the pattern is the one other case
      case (None, _) => IoRead.Unlocated(s"Kafka topics matching the pattern ${step.display.getOrElse("topicPattern", "(not stated)")}")
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

  private def pubsubRead(step: BeamStep): IoRead = IoRead.pubsub(step.display.get("topic").orElse(step.display.get("subscription")))

  /** The steps that make data in the job itself (`Create.of`, `GenerateSequence`, `PeriodicImpulse`, `TestStream`, a
    * `Read.from` of one of their sources): never a read from outside the pipeline, whatever primitives (an Impulse, a splittable
    * DoFn) they are built from. */
  def isInJobData(step: BeamStep): Boolean = {
    val name = step.simpleName
    name.startsWith("Create$") || JobData.contains(name) || (SourceReads.contains(name) && step.display.get("source").exists(isInJobSource))
  }

  private val JobData = Set("AutoValue_GenerateSequence", "PeriodicImpulse", "PeriodicSequence", "TestStream")

  /** `Read.from(source)`, bounded or unbounded. */
  private val SourceReads = Set("Read$Bounded", "Read$Unbounded")

  /** Whether `result`, something a write hands on, holds only what the write reports - names (the files it wrote, the tables it
    * loaded) or nothing - rather than rows: a FileIO or TextIO write's file names, a BigQuery load's table destinations. A BigQuery
    * write's failed or successful inserts are rows, and so is anything whose coder is not one of these (docs/BEAM_ADAPTER.md, D14). */
  def isWriteReport(result: BeamCollection): Boolean = result.coder.exists(reportCoder)

  private def reportCoder(coder: org.apache.beam.sdk.coders.Coder[_]): Boolean = coder match {
    case kv: org.apache.beam.sdk.coders.KvCoder[_, _] => reportCoder(kv.getKeyCoder) && reportCoder(kv.getValueCoder)
    case _: org.apache.beam.sdk.coders.StringUtf8Coder | _: org.apache.beam.sdk.coders.VoidCoder => true
    // TableDestinationCoder, V2, V3: a BigQuery table's name and description
    case other => other.getClass.getName.startsWith("org.apache.beam.sdk.io.gcp.bigquery.TableDestinationCoder")
  }

  /** A union of collections (`Flatten.pCollections()`): it combines several collections without matching any row with another. */
  def isUnion(step: BeamStep): Boolean = step.transform.exists(_.isInstanceOf[org.apache.beam.sdk.transforms.Flatten.PCollections[_]])

  /** The sources behind `Create.of(...)`, `GenerateSequence` and a direct `Read.from(CountingSource...)`: data made by the job
    * itself, not read from anywhere. */
  private def isInJobSource(sourceClass: String): Boolean =
    sourceClass.endsWith("Create$Values$CreateSource") || sourceClass.contains("CountingSource")

  /** Steps that turn lines of text into rows with a schema (`JsonToRow`, `CsvIO.parseRows`): the columns a text read really has
    * when it is parsed straight away. */
  def isTextParser(step: BeamStep): Boolean =
    Set("JsonToRow$JsonToRowFn", "JsonToRow$JsonToRowWithFailures", "AutoValue_CsvIOParse").contains(step.simpleName)

  /** Steps that turn rows into lines of text in a known format (`ToJson`): the columns and format a text write of their output has. */
  def textFormatterFormat(step: BeamStep): Option[String] = step.simpleName match {
    case "ToJson" => Some(Formats.Json)
    case _        => None
  }

  // --- writes other than file writes ---

  /** The write `step` is, if it is one of the non-file IOs (BigQuery, Kafka, JDBC, Pub/Sub). */
  def write(step: BeamStep): Option[IoWrite] =
    statedWrite(step).map {
      case located: IoWrite.Located => runtimeParameter(step).fold[IoWrite](located)(IoWrite.templateParameter)
      case other                    => other
    }

  private def statedWrite(step: BeamStep): Option[IoWrite] =
    step.simpleName match {
      case "AutoValue_BigQueryIO_Write"                                     => Some(bigQueryWrite(step))
      case "AutoValue_KafkaIO_Write" | "AutoValue_KafkaIO_WriteRecords"     => Some(kafkaWrite(step))
      case "JdbcIO$Write" | "AutoValue_JdbcIO_Write" | "AutoValue_JdbcIO_WriteVoid" | "AutoValue_JdbcIO_WriteWithResults" =>
        Some(jdbcWrite(step))
      case "AutoValue_PubsubIO_Write"                                       => Some(IoWrite.pubsub(step.display.get("topic")))
      case _                                                                => None
    }

  private def bigQueryWrite(step: BeamStep): IoWrite =
    IoWrite.bigQuery(
      step.display.get("table").flatMap(BeamLocations.bigQueryTable),
      step.display.get("writeDisposition"),
      // "Custom Schema Function" (a schema the job computes per destination) is not a schema the graph states
      step.display.get("schema").flatMap(BeamSchemas.fromBigQueryJson)
    )

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
    step.withDescendants.iterator.flatMap(_.transform).map(jdbcUrlOfTransform).collectFirst { case Some(u) => u }

  private def jdbcUrlOfTransform(t: AnyRef): Option[String] = {
    def field(o: AnyRef, name: String): Option[AnyRef] = Try(Option(Reflection.field(o, name))).toOption.flatten
    def call(o: AnyRef, name: String): Option[AnyRef] = Try(Option(Reflection.call(o, name))).toOption.flatten
    // JdbcIO.Write holds the provider itself; the other writes hold it through the `inner` write they wrap
    val provider = call(t, "getDataSourceProviderFn").orElse(field(t, "inner").flatMap(i => call(i, "getDataSourceProviderFn")))
    provider.flatMap(p => field(p, "config")).flatMap(c => call(c, "getUrl")).map {
      case v: ValueProvider[_] => String.valueOf(v.get())
      case other               => other.toString
    }
  }

  // --- file writes (WriteFiles) ---

  def isFileWrite(step: BeamStep): Boolean = step.transform.exists(_.isInstanceOf[WriteFiles[_, _, _]])

  /** The file write a write unit (a step holding exactly one `WriteFiles`) makes: where its files land and in which format. */
  def fileWrite(unit: BeamStep): IoWrite = {
    // CsvIO writes its rows as CSV through a text sink: the format is known even though the sink's is only "text"
    val knownFormat = if (unit.simpleName == "AutoValue_CsvIO_Write") Some(Formats.Csv) else None
    val all = unit.withDescendants
    val sink = all.flatMap(_.transform).collectFirst { case w: WriteFiles[_, _, _] => w.getSink.getClass.getName }
    val prefix = all.flatMap(_.display.get("filePrefix")).headOption
    // the schema the files are written with (an Avro write states it): the records' own may have columns it leaves out
    val writtenSchema = all.flatMap(_.display.get("schema")).headOption.flatMap(BeamSchemas.fromAvroJson)
    def prefixed(format: String, what: String): IoWrite = prefix match {
      case None                              => IoWrite.Unverifiable(s"$what write whose file prefix is not published")
      case Some(p) if isRuntimeParameter(p)  => IoWrite.templateParameter(all.flatMap(runtimeParameter).headOption.getOrElse(p))
      case Some(p)                           => IoWrite.Located(BeamLocations.folder(p), knownFormat.orElse(Some(format)), schema = writtenSchema)
    }
    sink match {
      case Some("org.apache.beam.sdk.io.TextSink")  => prefixed(Formats.Text, "a text")
      case Some(avro) if avro.endsWith(".AvroSink") => prefixed(Formats.Avro, "an Avro")
      case Some(tf) if tf.endsWith("TFRecordIO$TFRecordSink") => prefixed(BeamFormats.TFRecord, "a TFRecord")
      case Some("org.apache.beam.sdk.io.FileIO$Write$ViaFileBasedSink") =>
        fileIoWrite(all) match {
          case Some((location, sink)) => IoWrite.Located(location, formatOfSink(sink.getClass.getName), schema = sinkSchema(sink))
          case None                   => IoWrite.Unverifiable("a FileIO write whose destination or sink cannot be read")
        }
      case Some(other) => IoWrite.Unverifiable(s"a file write through $other, which this adapter does not recognise")
      case None        => IoWrite.Unverifiable("a write whose sink cannot be read")
    }
  }

  private def formatOfSink(sinkClass: String): Option[String] =
    if (sinkClass.contains("ParquetIO")) Some(Formats.Parquet)
    else if (sinkClass.contains("AvroIO")) Some(Formats.Avro)
    else if (sinkClass.contains("TextIO")) Some(Formats.Text)
    else if (sinkClass.contains("TFRecordIO")) Some(BeamFormats.TFRecord)
    else None

  /** The schema a sink writes its files with (`ParquetIO.sink(schema)`, `AvroIO.sink(schema)`: `getJsonSchema`), which is
    * what the files hold whatever the records carry; `None` for a sink that states none (a text sink). */
  private def sinkSchema(sink: AnyRef): Option[LogicalSchema] =
    Try(Option(Reflection.call(sink, "getJsonSchema")).map(_.toString)).toOption.flatten.flatMap(BeamSchemas.fromAvroJson)

  /** `(output directory, sink)` of a `FileIO.write()`. Beam publishes neither: the writer's display data names only its
    * sink class, and the destination and the sink are held in non-public members of `FileIO.Write` (docs/BEAM_ADAPTER.md,
    * "Friction log"). Any failure returns `None` and the write is refused as unverifiable rather than guessed. A separate
    * temporary directory (`withTempDirectory`) is not the destination, and the output directory is read directly, so it
    * cannot be mistaken for one. */
  private def fileIoWrite(steps: List[BeamStep]): Option[(String, AnyRef)] =
    steps.find(_.className.endsWith("AutoValue_FileIO_Write")).flatMap { s =>
      Try {
        val t = s.transform.get
        val directory = Option(Reflection.call(t, "getOutputDirectory")).map(_.asInstanceOf[ValueProvider[String]].get()).filter(_.nonEmpty)
        val sinkFn = Reflection.call(t, "getSinkFn").asInstanceOf[Contextful[Contextful.Fn[AnyRef, AnyRef]]]
        val sink = sinkFn.getClosure.apply(null, null)
        directory.map(d => BeamLocations.folder(d) -> sink)
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

  /** A BigQuery table as Beam states it: a table spec (`proj:ds.table`, `proj.ds.table` or `ds.table`, with or without a
    * partition decorator) or its JSON form (`{"projectId":..,"datasetId":..,"tableId":..}`). Located as `proj/ds/table`
    * (`ds/table` when no project is stated, as the job's default project is not known), and registered in the `bigquery`
    * catalog under `[proj, ds]`, `table`. */
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
    parts.map { case (project, dataset, decorated) =>
      // a partition decorator (`report$20240101`) names a partition of the table, not another table
      val table = decorated.takeWhile(_ != '$')
      BigQueryTable(
        LocationMatching.fromParts(project.toSeq ++ Seq(dataset, table)),
        CatalogIdentity(technology = Some(CatalogTechnologies.BigQuery), namespace = project.toList :+ dataset, table = Some(table))
      )
    }
  }

  /** A Pub/Sub topic or subscription (`projects/p/topics/t`), located by its path. */
  def pubsub(name: String): String = LocationMatching.fromParts(name.split('/').toSeq)

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
