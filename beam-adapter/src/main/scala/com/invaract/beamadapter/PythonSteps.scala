// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.beamadapter

import com.invaract.contract.{Formats, LogicalSchema}

/** The steps of a pipeline written with Beam's Python SDK, as its portable form describes them (`PortablePipeline`): the one
  * place that knows the Python SDK's class names and display-data keys (docs/BEAM_PYTHON_ADAPTER.md, D3).
  *
  * The Python SDK is uniform about IO, and this catalogue leans on that rather than on a table of transforms:
  *
  *  - every read is an `iobase.Read` step whose `source` display item names the source class; a file source also states its
  *    `file_pattern`, published by the source class itself (and repeated on the composite around it, such as `ReadFromParquet`);
  *  - every file write states its `file_pattern`, published by the sink class (`_ParquetSink`, `_TextSink`, ...), with the
  *    schema its files are written with where the format has one;
  *  - BigQuery and Pub/Sub writes are their own transforms (`WriteToBigQuery`, `WriteToPubSub`) that name their table or topic.
  *
  * Anything else that brings data into the pipeline (a step with no input that is not data the job makes itself, such as a CSV
  * read through pandas, a source class not listed here, or a Java transform used from Python) is a read whose source is not
  * located, never data the job made; anything else that ends the pipeline is refused by the translator. None of these names is
  * a stable API: they are held to the Python SDK releases CI tests, and a renamed one fails closed, never guessed.
  */
private[beamadapter] object PythonSteps extends StepCatalogue {

  /** The file sources, by class, with their format. */
  val FileSources: Map[String, String] = Map(
    "apache_beam.io.parquetio._ParquetSource" -> Formats.Parquet,
    "apache_beam.io.textio._TextSource" -> Formats.Text,
    "apache_beam.io.textio._TextSourceWithFilename" -> Formats.Text,
    "apache_beam.io.avroio._FastAvroSource" -> Formats.Avro,
    "apache_beam.io.avroio._AvroSource" -> Formats.Avro,
    "apache_beam.io.tfrecordio._TFRecordSource" -> BeamFormats.TFRecord
  )

  /** The file sinks, by class, with their format. */
  val FileSinks: Map[String, String] = Map(
    "apache_beam.io.parquetio._ParquetSink" -> Formats.Parquet,
    "apache_beam.io.textio._TextSink" -> Formats.Text,
    "apache_beam.io.avroio._FastAvroSink" -> Formats.Avro,
    "apache_beam.io.avroio._AvroSink" -> Formats.Avro,
    "apache_beam.io.tfrecordio._TFRecordSink" -> BeamFormats.TFRecord
  )

  private val BigQuerySources = Set("apache_beam.io.gcp.bigquery._CustomBigQuerySource", "apache_beam.io.gcp.bigquery._CustomBigQueryStorageSource")
  private val PubSubSource = "apache_beam.io.gcp.pubsub._PubSubSource"
  private val Read = "apache_beam.io.iobase.Read"
  private val FilePattern = "file_pattern"

  /** The steps that make data in the job itself. */
  private val JobData = Set(
    "apache_beam.transforms.core.Create",
    "apache_beam.transforms.periodicsequence.PeriodicImpulse",
    "apache_beam.transforms.periodicsequence.PeriodicSequence",
    "apache_beam.testing.test_stream.TestStream",
    "beam:transform:teststream:v1"
  )

  // --- reads ---

  override def read(step: BeamStep): Option[IoRead] =
    statedRead(step).orElse(
      if (!step.inputs.isEmpty || isInJobData(step)) None
      else
        // (data the job makes itself, such as a Create inside the step, never states a read, so it needs no leaving out)
        step.withDescendants.tail.flatMap(statedRead) match {
          // a step with no input brings data in from somewhere; if no read it holds says where, it is a read that is not located
          case Nil => Some(IoRead.Unlocated(s"${step.simpleName}, a step that brings data into the pipeline without stating where from"))
          // one of the SDK's own read transforms (ReadFromText, ReadFromBigQuery) is the one source read it is built around, so the
          // steps it adds around that read (a conversion, a clean-up) are not taken for the job's own; a team's composite is looked into
          case only :: Nil if step.className.startsWith(SdkIo) => Some(only)
          case _                                              => None
        }
    )

  /** The package of the Python SDK's IO transforms. */
  private val SdkIo = "apache_beam.io."

  private def statedRead(step: BeamStep): Option[IoRead] = fileRead(step).orElse(sourceRead(step))

  private def fileRead(step: BeamStep): Option[IoRead] =
    step.displayNamespaces.get(FilePattern).flatMap(FileSources.get).flatMap { format =>
      step.display.get(FilePattern).map { pattern =>
        runtimeParameter(pattern).fold[IoRead](IoRead.Located(List(BeamLocations.folderOf(pattern)), Some(format)))(IoRead.templateParameter)
      }
    }

  /** An `iobase.Read` of a source that is not a file source (those are `fileRead`'s). */
  private def sourceRead(step: BeamStep): Option[IoRead] =
    if (!step.displayNamespaces.get("source").contains(Read)) None
    else
      step.display.get("source").map {
        case file if FileSources.contains(file) => IoRead.Unlocated(s"a read from $file whose file pattern is not stated")
        case bq if BigQuerySources.contains(bq) => bigQueryRead(step)
        case PubSubSource                       => IoRead.pubsub(step.display.get("topic").orElse(step.display.get("subscription")).filter(stated))
        case other => IoRead.Unlocated(s"a read from $other, a source this adapter does not know")
      }

  private def bigQueryRead(step: BeamStep): IoRead =
    IoRead.bigQuery(
      step.display.get("table").orElse(step.display.get("table_reference")).filter(stated).flatMap(tableReference),
      isQuery = step.display.get("query").exists(stated)
    )

  private val Field = """(projectId|datasetId|tableId): '([^']*)'""".r

  /** A BigQuery table as the Python SDK prints its `TableReference` (`<TableReference\n datasetId: 'ds'\n projectId: 'p'\n
    * tableId: 't'>`), or a table spec (`p:ds.t`). */
  def tableReference(value: String): Option[BeamLocations.BigQueryTable] =
    if (value.startsWith("<TableReference")) {
      val parts = Field.findAllMatchIn(value).map(m => m.group(1) -> m.group(2)).toMap
      for (d <- parts.get("datasetId"); t <- parts.get("tableId"); table <- BeamLocations.bigQueryTable(parts.get("projectId").fold("")(_ + ":") + s"$d.$t"))
        yield table
    } else if (value.exists(c => c.isWhitespace || c == '<')) None // a function the job computes the table with, or anything else not a spec
    else BeamLocations.bigQueryTable(value)

  // --- writes ---

  override def write(step: BeamStep): Option[IoWrite] =
    statedFileWrite(step).orElse(step.className match {
      case "apache_beam.io.gcp.bigquery.WriteToBigQuery" => Some(bigQueryWrite(step))
      case "apache_beam.io.gcp.pubsub.WriteToPubSub"     => Some(IoWrite.pubsub(step.display.get("topic").filter(stated)))
      case _                                             => None
    })

  private def statedFileWrite(step: BeamStep): Option[IoWrite] =
    step.displayNamespaces.get(FilePattern).flatMap(FileSinks.get).map { format =>
      step.display.get(FilePattern).fold[IoWrite](IoWrite.Unverifiable(s"a $format write whose file pattern is not stated")) { pattern =>
        runtimeParameter(pattern).fold[IoWrite](
          IoWrite.Located(BeamLocations.folder(prefixOf(pattern)), Some(format), schema = writtenSchema(step, format))
        )(IoWrite.templateParameter)
      }
    }

  /** The schema a file write's files are written with: a Parquet write's pyarrow schema, an Avro write's record schema (which the
    * SDK prints as a Python dictionary, a form the YAML reader also reads). A text write has none: its columns are the data's. */
  private def writtenSchema(step: BeamStep, format: String): Option[LogicalSchema] =
    step.display.get("schema").filter(stated).flatMap { schema =>
      format match {
        case Formats.Parquet => PyArrowSchemas.parse(schema)
        case Formats.Avro    => BeamSchemas.fromAvroJson(schema)
        case _               => None
      }
    }

  /** Where a write's files land, named as the job named it: the file pattern up to its shard template (`out/report` for
    * `out/report-%(shard_num)05d-of-%(num_shards)05d.parquet`), the same prefix a Java `TextIO.write().to(...)` is located by. A
    * pattern with no template (a single file) is the file itself. */
  def prefixOf(pattern: String): String = pattern.indexOf("%(") match {
    case -1 => pattern
    case at => pattern.substring(0, at).reverse.dropWhile("-_.".contains(_)).reverse
  }

  private def bigQueryWrite(step: BeamStep): IoWrite = {
    val table = step.display.get("table").filter(stated)
    table.flatMap(runtimeParameter).fold(
      IoWrite.bigQuery(
        table.flatMap(tableReference),
        // the disposition and the schema are published by the steps the write is made of, which differ by method
        firstStated(step, "write_disposition"),
        firstStated(step, "schema").flatMap(BeamSchemas.fromBigQueryJson)
      )
    )(IoWrite.templateParameter)
  }

  private def firstStated(step: BeamStep, key: String): Option[String] = step.withDescendants.flatMap(_.display.get(key)).find(stated)

  // --- the rest ---

  override def isInJobData(step: BeamStep): Boolean = JobData.contains(step.className)
  override def isUnion(step: BeamStep): Boolean = step.className == "apache_beam.transforms.core.Flatten" || step.className == "beam:transform:flatten:v1"

  // the Python SDK has no file write built on a WriteFiles primitive, no text parser or formatter this adapter reads, and no schema
  // transform whose columns its portable form states
  override def isFileWrite(step: BeamStep): Boolean = false
  override def fileWrite(unit: BeamStep): IoWrite = IoWrite.Unverifiable(s"${unit.name}: not a file write")
  override def isTextParser(step: BeamStep): Boolean = false
  override def textFormatterFormat(step: BeamStep): Option[String] = None
  override def recognisesShape(step: BeamStep): Boolean = false
  override def shape(step: BeamStep): Option[SchemaShape] = None

  /** Whether a display value states something: the Python SDK prints an unset value as `None`. */
  private def stated(value: String): Boolean = value.nonEmpty && value != "None"

  /** The option a value is a template parameter of, when it is one: the SDK prints a classic template's runtime parameter as
    * `RuntimeValueProvider(option: input, ...)` (a value computed from one as `NestedValueProvider(...)`), and a value given when
    * the pipeline was built as the value itself. */
  def runtimeParameter(value: String): Option[String] =
    if (value.startsWith("RuntimeValueProvider(")) Some("""option: ([^,)]+)""".r.findFirstMatchIn(value).fold(value)(_.group(1)))
    else if (value.startsWith("NestedValueProvider(")) Some(value)
    else None
}
