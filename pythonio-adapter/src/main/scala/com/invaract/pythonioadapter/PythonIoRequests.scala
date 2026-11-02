// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.pythonioadapter

import com.invaract.contract.{LogicalField, LogicalType}
import com.invaract.verification.{ArrowSchemas, ConfigSource, InvaractConf}
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

  /** Start a run: the Invaract settings by their neutral names (`contract`, `orgPolicy`, ...), and the job's details. */
  final case class Open(settings: Map[String, String], job: JobDetails) extends Request

  /** A dataset the job reads through `invaract.read`, by its declared name, with the columns read and how many empty values
    * each holds. `location` and `format` are the job's own only in dry-run mode, where there is no contract to give them. */
  final case class Read(name: String, location: Option[String], format: Option[String], schema: List[Column]) extends Request

  /** A table the job hands to `invaract.write`, described before any of it is staged: the output's name, the format and save
    * mode the package writes, and the table's columns. `unverifiable` says why the package could not describe the table (it
    * offers no Arrow stream), and replaces the rest. `location` is the job's own only in dry-run mode. */
  final case class Write(
      name: String,
      location: Option[String],
      format: Option[String],
      saveMode: Option[String],
      schema: List[Column],
      unverifiable: Option[String]
  ) extends Request

  /** A table the package has finished staging, after its `write` passed: how many empty values each column turned out to hold,
    * counted batch by batch as it was written, and how many rows. */
  final case class Written(name: String, rows: Option[Long], nulls: Map[String, Long]) extends Request

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

  /** Whether the column holds empty values (decision 3: what the data holds, not what the frame library's flag says), or, with
    * no count, whether Arrow lets it. */
  def holdsNulls: Boolean = nulls.fold(nullable)(_ > 0)

  def field(nullable: Boolean): LogicalField = LogicalField(name, dataType, nullable)
}

private[pythonioadapter] object Requests {

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
    val settings = section(m, "settings").map { case (k, v) => k -> String.valueOf(v) }
    val job = section(m, "job")
    val libraries = section(job, "libraries").map { case (k, v) => k -> String.valueOf(v) }
    Right(Request.Open(settings, JobDetails(text(job, "name"), text(job, "runId"), text(job, "pythonVersion"), libraries)))
  }

  private def read(m: Map[String, AnyRef]): Either[String, Request] =
    for {
      name <- text(m, "name").toRight("a read must name the input it reads")
      schema <- columns(m.get("schema"), s"the read of '$name'")
    } yield Request.Read(name, text(m, "location"), text(m, "format"), schema)

  private def write(m: Map[String, AnyRef]): Either[String, Request] =
    for {
      name <- text(m, "name").toRight("a write must name the output it writes")
      schema <- columns(m.get("schema"), s"the write of '$name'")
    } yield Request.Write(name, text(m, "location"), text(m, "format"), text(m, "saveMode"), schema, text(m, "unverifiable"))

  private def written(m: Map[String, AnyRef]): Either[String, Request] =
    for {
      name <- text(m, "name").toRight("a written must name the output it staged")
      rows <- m.get("rows").fold[Either[String, Option[Long]]](Right(None))(count(_, s"the rows of '$name'").map(Some(_)))
      nulls <- section(m, "nulls").toList.foldRight[Either[String, Map[String, Long]]](Right(Map.empty)) { case ((column, n), rest) =>
        for { tail <- rest; c <- count(n, s"the empty values of column '$column' of '$name'") } yield tail + (column -> c)
      }
    } yield Request.Written(name, rows, nulls)

  private def count(value: AnyRef, what: String): Either[String, Long] = value match {
    case n: java.lang.Number if n.longValue >= 0 && n.doubleValue == n.longValue.toDouble => Right(n.longValue)
    case other                                                                         => Left(s"$what must be a count, not '$other'")
  }

  /** Columns as `{"name": ..., "type": ..., "nullable": ..., "nulls": ...}`, the type as pyarrow names it (`str(field.type)`). */
  private def columns(value: Option[AnyRef], what: String): Either[String, List[Column]] =
    list(value).foldRight[Either[String, List[Column]]](Right(Nil)) { (item, rest) =>
      for {
        tail <- rest
        column <- item match {
          case f: JMap[_, _] =>
            val m = asMap(f)
            for {
              name <- text(m, "name").toRight(s"a column of $what has no name")
              typeText <- text(m, "type").toRight(s"column '$name' of $what has no type")
              logical <- ArrowSchemas.dataType(typeText).toRight(s"column '$name' of $what has the type '$typeText', which is not an Arrow type")
              nulls <- m.get("nulls").fold[Either[String, Option[Long]]](Right(None))(count(_, s"the empty values of column '$name' of $what").map(Some(_)))
            } yield Column(name, logical, nullable = !m.get("nullable").contains(java.lang.Boolean.FALSE), nulls)
          case _ => Left(s"a column of $what must be a JSON object")
        }
      } yield column :: tail
    }

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
  def variable(name: String): String = "INVARACT_" + name.flatMap(c => if (c.isUpper) "_" + c else c.toUpper.toString)
}
