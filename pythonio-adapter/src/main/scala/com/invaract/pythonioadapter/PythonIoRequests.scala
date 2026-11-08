// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.pythonioadapter

import com.invaract.contract.{LogicalField, LogicalType, PartitionOverwriteModes}
import com.invaract.ir.CatalogIdentity
import com.invaract.verification.{ArrowSchemas, ConfigSource, InvaractConf, WritePartitioning}
import org.yaml.snakeyaml.{LoaderOptions, Yaml}
import org.yaml.snakeyaml.constructor.SafeConstructor
import java.util.{List => JList, Map => JMap}
import scala.collection.JavaConverters._
import scala.util.control.NonFatal

/** One request of the protocol the `invaract` Python package speaks to `PythonIoCheck`: a JSON object per line
  * (docs/PYTHON_IO_ADAPTER_DESIGN.md, section 4; the requests and replies are listed in docs/PYTHONIO_ADAPTER.md). A request
  * describes data; it never carries it.
  */
private[pythonioadapter] sealed trait Request

private[pythonioadapter] object Request {

  /** Start a run: the Invaract settings by their neutral names (`contract`, `orgPolicy`, ...), and the job's details. `check`
    * opens a check (`invaract.check`) rather than a run: one frame judged as a write would be, whatever the job read.
    * `protocol` is the protocol version the package speaks (`Requests.Version` when it does not say: a package from before
    * the version was sent). */
  final case class Open(settings: Map[String, String], job: JobDetails, check: Boolean = false, protocol: Long = Requests.Version) extends Request

  /** A dataset the job reads through `invaract.read`, by its declared name, with the columns read and how many empty values
    * each holds. `location` is the job's own only in dry-run mode, where there is no contract to give it. `format` is the
    * format the package reads it in. `catalog` is the catalog the dataset is registered in, when it is a catalog table (an
    * Iceberg table names its catalog and namespace). `schema` is `None` for a read the package has not made because its
    * description already disagrees with the contract (a `format` the contract does not declare): it can only be refused. */
  final case class Read(name: String, location: Option[String], schema: Option[List[Column]], catalog: Option[CatalogIdentity] = None,
      csv: Map[String, String] = Map.empty, format: Option[String] = None) extends Request

  /** A table the job hands to `invaract.write`, described before any of it is staged: the output's name, the format and save
    * mode the package writes, and the table's columns. `unverifiable` says why the package could not describe the table (it
    * offers no Arrow stream), and replaces the rest. `location` is the job's own only in dry-run mode. `catalog` is the catalog
    * the output is registered in, as for `Read`. */
  final case class Write(
      name: String,
      location: Option[String],
      format: Option[String],
      saveMode: Option[String],
      schema: List[Column],
      unverifiable: Option[String],
      catalog: Option[CatalogIdentity] = None,
      partitioning: WritePartitioning = WritePartitioning(Nil, PartitionOverwriteModes.Static),
      csv: Map[String, String] = Map.empty
  ) extends Request

  /** A table the package has finished staging, after its `write` passed: how many empty values each column turned out to hold,
    * counted batch by batch as it was written, and what it found evaluating the output's data checks (sent with `open`), in
    * their order - none when it was given none. */
  final case class Written(name: String, nulls: Map[String, Long], checks: List[Found] = Nil) extends Request

  /** One data check's outcome: how many rows broke it, and some of the values they held. */
  final case class Found(failed: Long, examples: List[String])

  /** The end of a run: `completed` is false when the job itself failed, so nothing is published. */
  final case class Finish(completed: Boolean) extends Request
}

/** Which job a run belongs to, for the events it publishes. */
private[pythonioadapter] final case class JobDetails(
    name: Option[String],
    runId: Option[String],
    pythonVersion: Option[String],
    libraries: Map[String, String]
)

/** A column as the package describes it: its name, its Arrow type mapped to the contract's (`ArrowSchemas.dataType`), whether
  * Arrow lets it hold empty values, and, where the data has been seen, how many it holds. */
private[pythonioadapter] final case class Column(name: String, dataType: LogicalType, nullable: Boolean, nulls: Option[Long]) {

  /** The column as the checks see it, given how many empty values it holds: nullable when it holds one (decision 3: what the
    * data holds, not what the frame library's flag says), or, with no count, when Arrow lets it. */
  def field(nulls: Option[Long]): LogicalField = LogicalField(name, dataType, nulls.fold(nullable)(_ > 0))
}

private[pythonioadapter] object Requests {

  /** The protocol version this checker speaks: what `open` says and what its reply says back. It changes only when a request
    * or reply changes in a way an older package or checker would misread (a field removed, renamed or given another meaning);
    * a new optional field leaves it as it is (docs/PYTHONIO_PROTOCOL.md, "Versions"). */
  val Version: Long = 1L

  /** The request on one line, or why it is not one. */
  def parse(line: String): Either[String, Request] =
    try {
      new Yaml(new SafeConstructor(new LoaderOptions)).load[AnyRef](line) match {
        case m: JMap[_, _] => request(asMap(m))
        case _             => Left("a request must be a JSON object")
      }
    } catch {
      case NonFatal(e) => Left(s"a request must be a JSON object: ${e.getMessage}")
    }

  private def request(m: Map[String, AnyRef]): Either[String, Request] =
    m.get("op") match {
      case Some("open")    => open(m)
      case Some("read")    => read(m)
      case Some("write")   => write(m)
      case Some("written") => written(m)
      case Some("finish")  => Right(Request.Finish(m.get("completed").forall(_ == java.lang.Boolean.TRUE)))
      case other           => Left(s"unknown op ${other.fold("(none)")(o => s"'$o'")}: expected open, read, write, written or finish")
    }

  private def open(m: Map[String, AnyRef]): Either[String, Request] = {
    val job = section(m, "job")
    m.get("protocol").fold[Either[String, Long]](Right(Version))(count(_, "the protocol version")).map { protocol =>
      Request.Open(strings(m, "settings"), JobDetails(text(job, "name"), text(job, "runId"), text(job, "pythonVersion"), strings(job, "libraries")),
        check = m.get("check").contains(java.lang.Boolean.TRUE), protocol = protocol)
    }
  }

  private def read(m: Map[String, AnyRef]): Either[String, Request] =
    for {
      name <- text(m, "name").toRight("a read must name the input it reads")
      schema <- traverse(m.get("schema").toList)(s => columns(Some(s), s"the read of '$name'"))
      registered <- catalog(m.get("catalog"), s"the read of '$name'")
    } yield Request.Read(name, text(m, "location"), schema.headOption, registered, strings(m, "csv"), text(m, "format"))

  private def write(m: Map[String, AnyRef]): Either[String, Request] =
    for {
      name <- text(m, "name").toRight("a write must name the output it writes")
      schema <- columns(m.get("schema"), s"the write of '$name'")
      registered <- catalog(m.get("catalog"), s"the write of '$name'")
      partitionBy <- names(m.get("partitionBy"), s"the partition columns of the write of '$name'")
      mode <- overwriteMode(text(m, "partitionOverwriteMode"), name)
    } yield Request.Write(name, text(m, "location"), text(m, "format"), text(m, "saveMode"), schema, text(m, "unverifiable"), registered,
      WritePartitioning(partitionBy, mode), strings(m, "csv"))

  private def names(value: Option[AnyRef], what: String): Either[String, List[String]] = value match {
    case None => Right(Nil)
    case Some(items: JList[_]) =>
      traverse(items.asScala.toList) {
        case n: String => Right(n)
        case other     => Left(s"$what must be column names, not '$other'")
      }
    case Some(other) => Left(s"$what must be a list of column names, not '$other'")
  }

  private def overwriteMode(mode: Option[String], name: String): Either[String, String] =
    mode.fold[Either[String, String]](Right(PartitionOverwriteModes.Static)) { m =>
      if (PartitionOverwriteModes.All.contains(m)) Right(m)
      else Left(s"the partition overwrite mode of the write of '$name' must be static or dynamic, not '$m'")
    }

  /** A catalog registration as `{"technology": ..., "catalogName": ..., "namespace": [...], "table": ...}`, each part optional. */
  private def catalog(value: Option[AnyRef], what: String): Either[String, Option[CatalogIdentity]] =
    value match {
      case None => Right(None)
      case Some(c: JMap[_, _]) =>
        val m = asMap(c)
        val parts = m.get("namespace") match {
          case None | Some(_: JList[_]) => Right(list(m.get("namespace")))
          case Some(other)              => Left(s"the catalog namespace of $what must be a list of names, not '$other'")
        }
        parts.flatMap(traverse(_) {
          case part: String => Right(part)
          case other        => Left(s"the catalog namespace of $what must be a list of names, not '$other'")
        }).map(namespace => Some(CatalogIdentity(text(m, "technology"), text(m, "catalogName"), None, namespace, text(m, "table"))))
      case Some(other) => Left(s"the catalog of $what must be a JSON object, not '$other'")
    }

  private def written(m: Map[String, AnyRef]): Either[String, Request] =
    for {
      name <- text(m, "name").toRight("a written must name the output it staged")
      nulls <- traverse(section(m, "nulls").toList) { case (column, n) => count(n, s"the empty values of column '$column' of '$name'").map(column -> _) }
      checks <- found(m.get("checks"), name)
    } yield Request.Written(name, nulls.toMap, checks)

  private def found(value: Option[AnyRef], name: String): Either[String, List[Request.Found]] = value match {
    case None => Right(Nil)
    case Some(items: JList[_]) =>
      traverse(items.asScala.toList) {
        case c: JMap[_, _] =>
          val m = asMap(c)
          for {
            failed <- count(m.getOrElse("failed", null), s"the failing rows of a check of '$name'")
            examples <- traverse(list(m.get("examples")))(e => Right(String.valueOf(e)))
          } yield Request.Found(failed, examples)
        case other => Left(s"each check of '$name' must be a JSON object, not '$other'")
      }
    case Some(other) => Left(s"the checks of '$name' must be a list, not '$other'")
  }

  private def count(value: AnyRef, what: String): Either[String, Long] = value match {
    case n: java.lang.Number if n.longValue >= 0 && n.doubleValue == n.longValue.toDouble => Right(n.longValue)
    case other                                                                         => Left(s"$what must be a count, not '$other'")
  }

  /** Columns as `{"name": ..., "type": ..., "nullable": ..., "nulls": ...}`, the type as pyarrow names it (`str(field.type)`). */
  private def columns(value: Option[AnyRef], what: String): Either[String, List[Column]] =
    traverse(list(value)) {
      case f: JMap[_, _] =>
        val m = asMap(f)
        for {
          name <- text(m, "name").toRight(s"a column of $what has no name")
          typeText <- text(m, "type").toRight(s"column '$name' of $what has no type")
          logical <- ArrowSchemas.dataType(typeText).toRight(s"column '$name' of $what has the type '$typeText', which is not an Arrow type")
          nulls <- traverse(m.get("nulls").toList)(count(_, s"the empty values of column '$name' of $what"))
        } yield Column(name, logical, nullable = !m.get("nullable").contains(java.lang.Boolean.FALSE), nulls.headOption)
      case _ => Left(s"a column of $what must be a JSON object")
    }

  /** `f` of every item, or the first item's error. */
  private def traverse[A, B](items: List[A])(f: A => Either[String, B]): Either[String, List[B]] =
    items.foldRight[Either[String, List[B]]](Right(Nil))((item, rest) => for { tail <- rest; b <- f(item) } yield b :: tail)

  /** A section of string values (`settings`, `libraries`), each value as text. */
  private def strings(m: Map[String, AnyRef], key: String): Map[String, String] = section(m, key).map { case (k, v) => k -> String.valueOf(v) }

  private def asMap(m: JMap[_, _]): Map[String, AnyRef] = m.asScala.map { case (k, v) => String.valueOf(k) -> v.asInstanceOf[AnyRef] }.toMap

  private def section(m: Map[String, AnyRef], key: String): Map[String, AnyRef] = m.get(key) match {
    case Some(s: JMap[_, _]) => asMap(s)
    case _                   => Map.empty
  }

  private def list(value: Option[AnyRef]): List[AnyRef] = value match {
    case Some(l: JList[_]) => l.asScala.map(_.asInstanceOf[AnyRef]).toList
    case _                 => Nil
  }

  private def text(m: Map[String, AnyRef], key: String): Option[String] =
    m.get(key).collect { case s: String if s.trim.nonEmpty => s.trim }
}

/** The settings a run is given, by their neutral names (`InvaractConf`); the package reads them from the job's environment as
  * `INVARACT_<NAME>` (`INVARACT_ORG_POLICY`) and forwards them unchanged, so a setting means the same as on every engine. */
private[pythonioadapter] final class PythonIoConfigSource(settings: Map[String, String]) extends ConfigSource {
  override def get(name: String): Option[String] = settings.get(name).filter(_.trim.nonEmpty)
  override def describe(name: String): String = PythonIoConfigSource.variable(name)
}

private[pythonioadapter] object PythonIoConfigSource {

  /** Settings for what this adapter does not have: a missing output always fails a run, and there are no steps to trust. */
  val NotApplicable: Set[String] = Set(InvaractConf.FailOnMissingOutput, InvaractConf.TrustedSteps)

  /** Every setting a run can be given. */
  val Known: Set[String] = InvaractConf.All.toSet -- NotApplicable

  /** How a job's environment spells the setting `name`: `orgPolicy` is `INVARACT_ORG_POLICY`. */
  def variable(name: String): String = "INVARACT_" + ConfigSource.snakeCase(name).toUpperCase(java.util.Locale.ROOT)
}
