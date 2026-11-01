// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.pythonioadapter

import com.invaract.contract.{LogicalField, LogicalType}
import com.invaract.verification.{ConfigSource, InvaractConf}
import org.yaml.snakeyaml.{LoaderOptions, Yaml}
import org.yaml.snakeyaml.constructor.SafeConstructor
import java.util.{List => JList, Map => JMap}
import scala.collection.JavaConverters._
import scala.util.control.NonFatal

/** One request of the protocol the `invaract` Python package speaks to `PythonIoCheck`: a JSON object per line
  * (docs/PYTHON_IO_ADAPTER_DESIGN.md, section 4). A request describes data; it never carries it.
  */
private[pythonioadapter] sealed trait Request

private[pythonioadapter] object Request {

  /** Start a run: the Invaract settings by their neutral names (`contract`, `orgPolicy`, ...), and the job's details. */
  final case class Open(settings: Map[String, String], job: JobDetails) extends Request

  /** A table the job hands to `invaract.write`: the output's name, what the package writes (format, save mode), the table's
    * columns in the contract's own types, and the declared inputs the job has read so far, with their columns where known. */
  final case class Write(name: String, format: Option[String], saveMode: Option[String], schema: List[LogicalField], reads: List[ReadInput])
      extends Request

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

/** An input the job read, by its declared name, with its columns when the package knows them. */
private[pythonioadapter] final case class ReadInput(name: String, schema: List[LogicalField])

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
      case Some("open")   => open(m)
      case Some("write")  => write(m)
      case Some("finish") => Right(Request.Finish(m.get("completed").forall(_ == java.lang.Boolean.TRUE)))
      case other          => Left(s"unknown op ${other.fold("(none)")(o => s"'$o'")}: expected open, write or finish")
    }

  private def open(m: Map[String, AnyRef]): Either[String, Request] = {
    val settings = section(m, "settings").map { case (k, v) => k -> String.valueOf(v) }
    val job = section(m, "job")
    val libraries = job.get("libraries") match {
      case Some(l: JMap[_, _]) => asMap(l).map { case (k, v) => k -> String.valueOf(v) }
      case _                   => Map.empty[String, String]
    }
    Right(Request.Open(settings, JobDetails(text(job, "name"), text(job, "runId"), text(job, "pythonVersion"), libraries)))
  }

  private def write(m: Map[String, AnyRef]): Either[String, Request] =
    for {
      name <- text(m, "name").toRight("a write must name the output it writes")
      schema <- fields(m.get("schema"), s"the write of '$name'")
      reads <- inputs(m.get("reads"))
    } yield Request.Write(name, text(m, "format"), text(m, "saveMode"), schema, reads)

  private def inputs(value: Option[AnyRef]): Either[String, List[ReadInput]] =
    list(value).foldRight[Either[String, List[ReadInput]]](Right(Nil)) { (item, rest) =>
      for {
        tail <- rest
        read <- item match {
          case r: JMap[_, _] =>
            val m = asMap(r)
            text(m, "name").toRight("every read must name the input it read").flatMap { name =>
              fields(m.get("schema"), s"the read of '$name'").map(ReadInput(name, _))
            }
          case _ => Left("every read must be a JSON object")
        }
      } yield read :: tail
    }

  /** Columns as `{"name": ..., "type": ..., "nullable": ...}`, the type in the contract's own syntax (`long`, `array<string>`). */
  private def fields(value: Option[AnyRef], what: String): Either[String, List[LogicalField]] =
    list(value).foldRight[Either[String, List[LogicalField]]](Right(Nil)) { (item, rest) =>
      for {
        tail <- rest
        field <- item match {
          case f: JMap[_, _] =>
            val m = asMap(f)
            for {
              name <- text(m, "name").toRight(s"a column of $what has no name")
              typeText <- text(m, "type").toRight(s"column '$name' of $what has no type")
              logical <- LogicalType.parse(typeText).toRight(s"column '$name' of $what has the type '$typeText', which is not a contract type")
            } yield LogicalField(name, logical, nullable = !m.get("nullable").contains(java.lang.Boolean.FALSE))
          case _ => Left(s"a column of $what must be a JSON object")
        }
      } yield field :: tail
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
