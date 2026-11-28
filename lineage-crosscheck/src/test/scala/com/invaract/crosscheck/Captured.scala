// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.crosscheck

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}

import java.nio.file.{Files, Path}
import scala.collection.JavaConverters._

/** A dataset as OpenLineage names it. */
final case class Ds(namespace: String, name: String) {
  override def toString: String = s"$namespace:$name"
}

/** One output column taking (part of) its value from one input column. */
final case class Direct(field: String, source: Ds, sourceField: String) {
  override def toString: String = s"$field <- $source.$sourceField"
}

/** An input column shaping which rows the output holds or in what order (a filter, a join, a grouping, a sort: the
  * whole output, `field` empty), or which value one output column takes (a CASE WHEN's condition, a window's
  * partitioning: `field` that column). */
final case class Indirect(field: Option[String], source: Ds, sourceField: String) {
  override def toString: String = s"${field.getOrElse("*")} <~ $source.$sourceField"
  def unfielded: Indirect = copy(field = None)
}

/** What one tool reports about one write: its datasets, and its column lineage with the OpenLineage transformation
  * subtypes it gave each edge (empty for Spline, which has no such classification). */
final case class Captured(
    inputs: Set[Ds],
    output: Option[Ds],
    direct: Map[Direct, Set[String]],
    indirect: Map[Indirect, Set[String]]
) {
  /** Every input column each output column depends on, whether by value or by shaping the rows: what Spline's
    * attribute graph can say without classifying edges. */
  def dependencies: Set[Direct] = direct.keySet
}

object Captured {
  val empty: Captured = Captured(Set.empty, None, Map.empty, Map.empty)

  private val json = new ObjectMapper()

  private def lines(file: Path): List[JsonNode] =
    if (!Files.exists(file)) Nil
    else Files.readAllLines(file).asScala.toList.filter(_.trim.nonEmpty).map(json.readTree)

  private def ds(node: JsonNode): Ds = Ds(node.path("namespace").asText(), node.path("name").asText())

  /** The write to `output` as an OpenLineage producer (Invaract or openlineage-spark) published it, across every event
    * of the run that names that output: its inputs, and the union of its column lineage facets. */
  def fromOpenLineage(file: Path, output: Ds => Boolean): Captured = {
    val events = lines(file)
    val writes = events.filter(_.path("outputs").elements().asScala.exists(o => output(ds(o))))
    val inputs = writes.flatMap(_.path("inputs").elements().asScala.map(ds)).toSet
    val outputs = writes.flatMap(_.path("outputs").elements().asScala.filter(o => output(ds(o))))
    val facets = outputs.map(_.path("facets").path("columnLineage")).filterNot(_.isMissingNode)
    def subtypes(input: JsonNode): Set[String] =
      input.path("transformations").elements().asScala.map { t =>
        s"${t.path("type").asText()}/${t.path("subtype").asText()}${if (t.path("masking").asBoolean(false)) "+masking" else ""}"
      }.toSet
    val perField = for {
      facet <- facets
      (field, lineage) <- facet.path("fields").fields().asScala.map(e => e.getKey -> e.getValue).toList
      input <- lineage.path("inputFields").elements().asScala.toList
    } yield (field, input)
    // one input field can carry DIRECT and INDIRECT transformations at once (`coalesce(a, ...)`: a's value, and a
    // condition on it): each half is an edge of its own kind
    val direct = perField.flatMap { case (field, input) =>
      val s = subtypes(input).filter(_.startsWith("DIRECT"))
      if (s.isEmpty) Nil else List(Direct(field, ds(input), input.path("field").asText()) -> s)
    }
    val indirectPerField = perField.flatMap { case (field, input) =>
      val s = subtypes(input).filter(_.startsWith("INDIRECT"))
      if (s.isEmpty) Nil else List(Indirect(Some(field), ds(input), input.path("field").asText()) -> s)
    }
    val indirect = for {
      facet <- facets
      input <- facet.path("dataset").elements().asScala.toList
    } yield Indirect(None, ds(input), input.path("field").asText()) -> subtypes(input)
    Captured(inputs, outputs.headOption.map(ds), merge(direct), merge(indirect ++ indirectPerField))
  }

  private def merge[K](pairs: List[(K, Set[String])]): Map[K, Set[String]] =
    pairs.groupBy(_._1).map { case (k, vs) => k -> vs.flatMap(_._2).toSet }

  /** The write to `output` as Spline's agent recorded it (producer API 1.2 execution plans, one per line, as
    * `SplineCapture` writes them). Each written column is traced through Spline's attribute and expression graph to
    * the attributes of the reads it comes from (a window column's partitioning and ordering among them); the columns a
    * Filter, Join, Aggregate's grouping or Sort refer to are traced the same way and are the indirect lineage of the
    * whole output. Spline names datasets by
    * URI, which `uri` turns into the same (namespace, name) an OpenLineage producer gives a file. */
  def fromSpline(file: Path, output: Ds => Boolean): Captured = {
    val plans = lines(file).filter(_.has("operations"))
      .filter(p => output(uri(p.path("operations").path("write").path("outputSource").asText())))
    plans.map(splinePlan).foldLeft(empty) { (a, b) =>
      Captured(a.inputs ++ b.inputs, a.output.orElse(b.output), merge((a.direct ++ b.direct).toList), merge((a.indirect ++ b.indirect).toList))
    }
  }

  private def splinePlan(plan: JsonNode): Captured = {
    val ops = plan.path("operations")
    val write = ops.path("write")
    val reads = ops.path("reads").elements().asScala.toList
    val others = ops.path("other").elements().asScala.toList
    val readOf: Map[String, Ds] = reads.flatMap { r =>
      val source = uri(r.path("inputSources").get(0).asText())
      r.path("output").elements().asScala.map(_.asText() -> source)
    }.toMap
    val attributes = plan.path("attributes").elements().asScala.map(a => a.path("id").asText() -> a).toMap
    val expressions = (plan.path("expressions").path("functions").elements().asScala ++
      plan.path("expressions").path("constants").elements().asScala).map(e => e.path("id").asText() -> e).toMap
    def refs(node: JsonNode): List[String] =
      if (node.isObject) {
        val own = List("__attrId", "__exprId").flatMap(k => Option(node.get(k)).map(_.asText()))
        own ++ node.fields().asScala.toList.filterNot(e => e.getKey.startsWith("__")).flatMap(e => refs(e.getValue))
      } else if (node.isArray) node.elements().asScala.toList.flatMap(refs)
      else Nil
    // the read attributes an attribute or expression is computed from
    def sources(id: String, seen: Set[String]): Set[(Ds, String)] =
      if (seen(id)) Set.empty
      else readOf.get(id) match {
        case Some(read) => Set(read -> attributes.get(id).map(_.path("name").asText()).getOrElse(id))
        case None =>
          val node = attributes.get(id).orElse(expressions.get(id))
          node.toList.flatMap(n => refs(n.path("childRefs"))).toSet.flatMap((child: String) => sources(child, seen + id))
      }
    val opsById = (others ++ reads).map(o => o.path("id").asText() -> o).toMap
    val written = write.path("childIds").elements().asScala.toList.flatMap(id => opsById.get(id.asText()))
      .flatMap(_.path("output").elements().asScala.map(_.asText()))
    val direct = written.flatMap { attr =>
      val field = attributes.get(attr).map(_.path("name").asText()).getOrElse(attr)
      sources(attr, Set.empty).map { case (d, f) => Direct(field, d, f) -> Set.empty[String] }
    }
    val shaping = Map(
      "Filter" -> List("condition"),
      "Join" -> List("condition"),
      "Aggregate" -> List("groupingExpressions"),
      "Sort" -> List("order")
    ) // a Window's partitioning and ordering are already among its window column's dependencies in Spline's graph
    val indirect = others.flatMap { op =>
      shaping.getOrElse(op.path("name").asText(), Nil).flatMap(param => refs(op.path("params").path(param)))
        .flatMap(id => sources(id, Set.empty)).map { case (d, f) => Indirect(None, d, f) -> Set.empty[String] }
    }
    Captured(readOf.values.toSet, Some(uri(write.path("outputSource").asText())), merge(direct), merge(indirect))
  }

  /** A URI as an OpenLineage producer names the dataset at it: `file:/x` is (file, /x), `scheme://authority/path` is
    * (scheme://authority, /path). */
  def uri(raw: String): Ds = {
    val u = new java.net.URI(raw)
    if (u.getScheme == null || u.getScheme == "file") Ds("file", u.getPath)
    else Ds(s"${u.getScheme}://${Option(u.getAuthority).getOrElse("")}", u.getPath)
  }
}
