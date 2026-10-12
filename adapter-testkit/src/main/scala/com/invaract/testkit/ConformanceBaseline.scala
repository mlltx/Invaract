// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.testkit

import java.nio.charset.StandardCharsets

/** The scenarios an adapter is known to conform to, checked in beside its tests, and the ratchet that keeps it so.
  *
  * Without it, an adapter that quietly stops conforming to a scenario - a capability flipped to `unsupported` or
  * `not-applicable` to make a failure go away, an operation it no longer recognises - turns that scenario from a pass
  * into a cancel, and a canceled test is green. The baseline makes that visible: a scenario on it must still conform,
  * and one that now conforms must be added to it, so the file only ever grows by a deliberate, reviewed line.
  *
  * The file is `conformance-baseline-<adapter>.txt` among the adapter's test resources (`<adapter>` is the `adapter:`
  * of its capability declaration): one scenario id per line, `#` for comments. The scaffold writes an empty one.
  */
object ConformanceBaseline {

  def resource(adapter: String): String = s"conformance-baseline-$adapter.txt"

  /** The scenario ids in `text`. */
  def parse(text: String): Set[String] =
    text.split("\n").map(_.trim).filter(l => l.nonEmpty && !l.startsWith("#")).toSet

  /** The adapter's baseline from the classpath, or why there is none. */
  def load(adapter: String, loader: ClassLoader): Either[String, Set[String]] =
    Option(loader.getResourceAsStream(resource(adapter))) match {
      case None =>
        Left(
          s"no ${resource(adapter)} among the test resources. It lists the scenarios $adapter conforms to, one id per line, " +
            "so a scenario that stops conforming cannot hide as a cancel."
        )
      case Some(in) =>
        try Right(parse(new String(readAll(in), StandardCharsets.UTF_8)))
        finally in.close()
    }

  private def readAll(in: java.io.InputStream): Array[Byte] = {
    val out = new java.io.ByteArrayOutputStream
    val buffer = new Array[Byte](8192)
    Iterator.continually(in.read(buffer)).takeWhile(_ != -1).foreach(out.write(buffer, 0, _))
    out.toByteArray
  }

  /** What the baseline and a full run disagree on; empty when they agree. A scenario the environment could not run
    * (`Unavailable`) is not judged either way: nothing was learned about it.
    */
  def problems(baseline: Set[String], verdicts: Map[String, ScenarioVerdict]): List[String] = {
    val known = Scenarios.all.map(_.id)
    val unknown = baseline.toList.sorted.filterNot(known.contains).map(id => s"the baseline names '$id', which is not a scenario")
    val regressed = known.filter(baseline.contains).flatMap { id =>
      verdicts.get(id) match {
        case Some(ScenarioVerdict.Conforms) | Some(_: ScenarioVerdict.Unavailable) | None => None
        case Some(ScenarioVerdict.Skipped(why)) =>
          Some(s"$id conformed on the baseline and is now skipped ($why): a scenario must not drop out silently - restore the capability, or remove the line from the baseline with the reason in the PR")
        case Some(ScenarioVerdict.Diverges(why)) => Some(s"$id conformed on the baseline and now diverges: $why")
      }
    }
    val improved = known.filterNot(baseline.contains).filter(id => verdicts.get(id).contains(ScenarioVerdict.Conforms))
    val added = if (improved.isEmpty) Nil else List(s"these scenarios now conform and are not on the baseline - add them: ${improved.mkString(", ")}")
    unknown ++ regressed ++ added
  }

  /** The baseline file's body for the scenarios that conform in `verdicts`, for pasting. */
  def body(adapter: String, verdicts: Map[String, ScenarioVerdict]): String = {
    val conforming = Scenarios.all.map(_.id).filter(id => verdicts.get(id).contains(ScenarioVerdict.Conforms))
    s"# The scenarios $adapter conforms to (adapter-testkit ConformanceBaseline). A scenario on this list must keep conforming;\n" +
      "# one that now conforms is added here, in the PR that makes it conform.\n" + conforming.mkString("", "\n", "\n")
  }

  /** One adapter's run as JSON: the verdict of every scenario, for tooling and for the record. */
  def resultsJson(adapter: String, verdicts: Map[String, ScenarioVerdict]): String = {
    def q(s: String) = "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t") + "\""
    val entries = Scenarios.all.flatMap(s => verdicts.get(s.id).map(s -> _)).map { case (s, v) =>
      val (name, reason) = v match {
        case ScenarioVerdict.Conforms         => ("conforms", None)
        case ScenarioVerdict.Skipped(r)       => ("skipped", Some(r))
        case ScenarioVerdict.Diverges(r)      => ("diverges", Some(r))
        case ScenarioVerdict.Unavailable(r)   => ("unavailable", Some(r))
      }
      s"""    {"scenario": ${q(s.id)}, "verdict": ${q(name)}${reason.map(r => s""", "reason": ${q(r)}""").getOrElse("")}}"""
    }
    s"""{\n  "adapter": ${q(adapter)},\n  "scenarios": [\n${entries.mkString(",\n")}\n  ]\n}\n"""
  }
}
