// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.beamadapter

import com.invaract.ir.JoinType
import org.apache.beam.sdk.schemas.{FieldAccessDescriptor, Schema => BeamSchema}
import org.apache.beam.sdk.schemas.transforms.{CoGroup, Filter, Group, Join}
import org.apache.beam.sdk.values.PCollection

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

/** What one of Beam's schema transforms does with columns, as far as the transformation-shape rules need it
  * (`required_filter_columns`, `required_group_by`, `required_join_columns`, `forbid_cross_join`; docs/BEAM_ADAPTER.md, D19).
  */
private[beamadapter] sealed trait SchemaShape

private[beamadapter] object SchemaShape {

  /** A filter: each predicate with the columns it is given (the predicate itself is code, so only which columns it reads is known). */
  final case class Filtered(predicates: List[List[String]]) extends SchemaShape

  /** A grouping by these columns; none when everything is one group (`Group.globally()`). */
  final case class Grouped(keys: List[String]) extends SchemaShape

  /** A join of the collections `sides` name, in order. A side's key columns are matched position by position with the first
    * side's; a side with no key columns is joined to the others without a condition (a cross product). */
  final case class Joined(joinType: JoinType, sides: List[JoinSide]) extends SchemaShape

  /** A schema transform it recognises but whose fields it could not read (a Beam release that changed them): what the rules ask of
    * it cannot be told, so a contract with such a rule refuses the write rather than guessing. */
  final case class Unreadable(why: String) extends SchemaShape
}

/** One side of a join: the collection, the name that qualifies its columns in the join condition, and its key columns. */
private[beamadapter] final case class JoinSide(collection: String, qualifier: String, keys: List[String])

/** Reads the columns Beam's schema transforms use - `Filter.create().whereFieldName(s)`, `Group.byFieldNames`/`byFieldIds` with or
  * without aggregation, `Group.globally()`, `Join` (inner, outer and broadcast) and `CoGroup` - from the transform objects in the graph.
  *
  * None of them publishes these through a public getter, so they are read by reflection, from members that are the same on every
  * Beam release in the version matrix (supported-beam-versions.properties): `Filter.Inner.filters` and each filter's
  * `getFieldAccessDescriptor()`, `Group.ByFields.getFieldAccessDescriptor()` (and `CombineFieldsByFields.getByFields()`),
  * `Join.Impl`'s `joinType`, `rhs` and `predicate` (whose `lhs`/`rhs` are the two sides' fields), and `CoGroup`'s `joinArgs`
  * (`allInputsJoinArgs`, `joinArgsMap`, each `By.getFieldAccessDescriptor()` and `getOptionalParticipation()`). The fields are then
  * resolved against the input's schema with Beam's own public `FieldAccessDescriptor.resolve`, so names given by position
  * (`byFieldIds`) or as `*` come out as column names. Any failure is `Unreadable`, never a guess.
  *
  * A filter, grouping or join written as plain code (`Filter.by`, a `DoFn`, `GroupByKey`, `CoGroupByKey`) is not here: it is
  * opaque, and the translator leaves it so.
  */
private[beamadapter] object BeamShapes {

  /** Whether `step` is one of the schema transforms read here (by class alone, without reading anything). */
  def recognises(step: BeamStep): Boolean = step.transform.orNull match {
    case _: Filter.Inner[_] | _: Group.ByFields[_] | _: Group.CombineFieldsByFields[_] | _: Group.Global[_] | _: Group.CombineGlobally[_, _] |
        _: Group.CombineFieldsGlobally[_] | _: Join.Impl[_, _] | _: CoGroup.Impl | _: CoGroup.ExpandCrossProduct =>
      true
    case _ => false
  }

  /** What `step` does with columns, when it is a schema transform. */
  def of(step: BeamStep): Option[SchemaShape] =
    if (!recognises(step)) None
    else
      Some(
        try shapeOf(step)
        catch { case NonFatal(e) => SchemaShape.Unreadable(s"the fields of ${BeamIO.simpleName(step)} could not be read on this Beam release: $e") }
      )

  private def shapeOf(step: BeamStep): SchemaShape = {
    val inputSchema = step.inputs.headOption.flatMap(_.rowSchema)
    step.transform.orNull match {
      case f: Filter.Inner[_] =>
        val filters = field(f, "filters").asInstanceOf[java.util.List[AnyRef]].asScala.toList
        SchemaShape.Filtered(filters.map(d => columns(call(d, "getFieldAccessDescriptor"), inputSchema)))
      case g: Group.CombineFieldsByFields[_] => SchemaShape.Grouped(columns(call(call(g, "getByFields"), "getFieldAccessDescriptor"), inputSchema))
      case g: Group.ByFields[_]              => SchemaShape.Grouped(columns(call(g, "getFieldAccessDescriptor"), inputSchema))
      case j: Join.Impl[_, _]                => joined(j, step)
      case c: CoGroup.Impl                   => coGrouped(c, step)
      case c: CoGroup.ExpandCrossProduct     => coGrouped(c, step)
      // Group.globally() and its aggregations: one group of everything
      case _ => SchemaShape.Grouped(Nil)
    }
  }

  private def joined(join: Join.Impl[_, _], step: BeamStep): SchemaShape = {
    val left = step.inputs.headOption.getOrElse(throw new IllegalStateException("a join with no input"))
    val right = field(join, "rhs").asInstanceOf[PCollection[_]]
    // a join needs a schema on both sides; without one this throws, and the join is Unreadable
    val rightSchema = Some(right.getSchema)
    // a join is built with a condition; without one there is nothing to match the two sides on
    val (leftKeys, rightKeys) = Option(field(join, "predicate")) match {
      case Some(p) => (columns(field(p, "lhs"), left.rowSchema), columns(field(p, "rhs"), rightSchema))
      case None    => (Nil, Nil)
    }
    SchemaShape.Joined(joinTypeOf(field(join, "joinType").toString), List(JoinSide(left.name, "lhs", leftKeys), JoinSide(right.getName, "rhs", rightKeys)))
  }

  /** Beam's join types: the broadcast forms differ only in how the right side reaches the workers. */
  private def joinTypeOf(name: String): JoinType = name match {
    case "INNER" | "INNER_BROADCAST"           => JoinType.Inner
    case "LEFT_OUTER" | "LEFT_OUTER_BROADCAST" => JoinType.LeftOuter
    case "RIGHT_OUTER"                         => JoinType.RightOuter
    case "OUTER"                               => JoinType.FullOuter
    case other                                 => throw new IllegalStateException(s"a join type it does not know: $other")
  }

  /** A `CoGroup` of several collections, each by its own fields (`join(tag, By...)`) or all by the same ones (`join(By...)`). An input
    * whose participation is optional makes it an outer join; otherwise every input must match (an inner join). */
  private def coGrouped(coGroup: AnyRef, step: BeamStep): SchemaShape = {
    val args = field(coGroup, "joinArgs")
    val all = Option(field(args, "allInputsJoinArgs"))
    val byTag = Option(field(args, "joinArgsMap")).map(_.asInstanceOf[java.util.Map[String, AnyRef]].asScala.toMap).getOrElse(Map.empty)
    val sides = step.inputs.map { input =>
      val tag = step.inputTags.getOrElse(input.name, throw new IllegalStateException(s"no tag for the input ${input.name}"))
      val by = byTag.get(tag).orElse(all).getOrElse(throw new IllegalStateException(s"no fields to join the input '$tag' on"))
      (JoinSide(input.name, tag, columns(call(by, "getFieldAccessDescriptor"), input.rowSchema)), call(by, "getOptionalParticipation") == java.lang.Boolean.TRUE)
    }
    SchemaShape.Joined(if (sides.exists(_._2)) JoinType.FullOuter else JoinType.Inner, sides.map(_._1))
  }

  /** The columns `descriptor` names, resolved against `schema`: a nested field is its top-level column (as a struct field read is
    * elsewhere), `*` is every column. */
  private[beamadapter] def columns(descriptor: AnyRef, schema: Option[BeamSchema]): List[String] = {
    val fields = descriptor.asInstanceOf[FieldAccessDescriptor]
    val resolved = schema.fold(fields)(fields.resolve)
    if (resolved.getAllFields)
      schema.getOrElse(throw new IllegalStateException("every field of a collection with no schema")).getFieldNames.asScala.toList
    else
      (resolved.getFieldsAccessed.asScala.toList ++ resolved.getNestedFieldsAccessed.keySet.asScala.toList)
        .map(d => Option(d.getFieldName).getOrElse(throw new IllegalStateException(s"field ${d.getFieldId} of a collection with no schema")))
        .distinct
  }

  // --- reflection: a member declared on the object's class or one it extends, `null` when it holds none ---

  private def classes(o: AnyRef): Iterator[Class[_]] = Iterator.iterate[Class[_]](o.getClass)(_.getSuperclass).takeWhile(_ != null)

  private def field(o: AnyRef, name: String): AnyRef = {
    val f = classes(o).flatMap(_.getDeclaredFields.find(_.getName == name)).toSeq.headOption
      .getOrElse(throw new NoSuchFieldException(s"${o.getClass.getName}.$name"))
    f.setAccessible(true)
    f.get(o)
  }

  private def call(o: AnyRef, name: String): AnyRef = {
    val m = classes(o).flatMap(_.getDeclaredMethods.find(m => m.getName == name && m.getParameterCount == 0)).toSeq.headOption
      .getOrElse(throw new NoSuchMethodException(s"${o.getClass.getName}.$name()"))
    m.setAccessible(true)
    m.invoke(o)
  }
}
