// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.beamadapter

import com.invaract.contract.ContractParser
import com.invaract.ir.{Aggregate, BooleanExpr, ColumnRef, ColumnReference, Comparison, DatasetRef, Filter => IrFilter, Join => IrJoin, JoinType, Plan, Read, UDF, UnknownPlan}
import com.invaract.verification.{ContractViolationException, VerificationOptions, ViolationType}
import com.invaract.verification.notification.{ContractInferenceEvent, InferenceStatus, NotificationEvent, NotificationSink}
import org.apache.avro.generic.GenericRecord
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.coders.{KvCoder, StringUtf8Coder}
import org.apache.beam.sdk.io.FileIO
import org.apache.beam.sdk.io.parquet.ParquetIO
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.schemas.{FieldAccessDescriptor, Schema => BeamSchema}
import org.apache.beam.sdk.extensions.sql.SqlTransform
import org.apache.beam.sdk.schemas.transforms.{CoGroup, Group, Join, RenameFields, Filter => SchemaFilter}
import org.apache.beam.sdk.transforms.join.{CoGroupByKey, KeyedPCollectionTuple}
import org.apache.beam.sdk.transforms.{Count, Flatten, MapElements, PTransform, ParDo, SerializableFunction, View}
import org.apache.beam.sdk.values.{KV, PCollection, PCollectionList, PCollectionTuple, Row, TupleTag, TypeDescriptor, TypeDescriptors}
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

/** Beam's schema transforms as the filters, groupings and joins the transformation-shape rules check (docs/BEAM_ADAPTER.md, D19):
  * what is read from each, what plan it becomes, and where the adapter refuses rather than guess. Pipelines are only constructed. */
class BeamShapesSpec extends AnyFunSuite {
  import ExampleJob.OrdersSchema

  private def pipeline: Pipeline = Pipeline.create(PipelineOptionsFactory.create())

  /** A read of `folder` whose records carry a Beam schema (`id`, `amount`), as Beam's schema transforms need. */
  private def rows(p: Pipeline, name: String, folder: String): PCollection[GenericRecord] =
    p.apply(name, ParquetIO.read(OrdersSchema).withBeamSchemas(true).from(s"/data/in/$folder/*.parquet"))

  private def step(p: Pipeline, name: String): BeamStep = {
    def find(steps: List[BeamStep]): Option[BeamStep] = steps.iterator.flatMap(s => if (s.name == name) Some(s) else find(s.children)).toSeq.headOption
    find(BeamGraph.of(p)).getOrElse(fail(s"no step $name"))
  }

  private def shape(p: Pipeline, name: String): Option[SchemaShape] = BeamShapes.of(step(p, name))

  private def toParquet[T](data: PCollection[T], dir: String = "/data/out/report"): Unit =
    data.asInstanceOf[PCollection[GenericRecord]].apply(s"Write${dir.replace('/', '_')}", FileIO.write[GenericRecord]().via(ParquetIO.sink(OrdersSchema)).to(dir))

  /** A positive-number test, given to a filter. */
  private final class Positive extends SerializableFunction[java.lang.Long, java.lang.Boolean] {
    override def apply(v: java.lang.Long): java.lang.Boolean = v != null && v > 0
  }

  /** A SQL result row as an orders record (the probes write it as orders). */
  private final class RowAsOrder extends SerializableFunction[Row, GenericRecord] {
    override def apply(r: Row): GenericRecord = {
      val record = new org.apache.avro.generic.GenericData.Record(OrdersSchema)
      record.put("id", r.getValue[AnyRef]("id"))
      record.put("amount", r.getValue[AnyRef]("amount"))
      record
    }
  }

  private final class Both extends SerializableFunction[Row, java.lang.Boolean] {
    override def apply(r: Row): java.lang.Boolean = true
  }

  // --- filters ---

  test("a schema Filter reads as the columns each of its predicates is given") {
    val p = pipeline
    val orders = rows(p, "Read", "orders")
    orders.apply("ByName", SchemaFilter.create[GenericRecord]().whereFieldName[java.lang.Long]("amount", new Positive))
    orders.apply("ByTwo", SchemaFilter.create[GenericRecord]().whereFieldNames(List("id", "amount").asJava, new Both))
    orders.apply("Twice", SchemaFilter.create[GenericRecord]().whereFieldName[java.lang.Long]("id", new Positive).whereFieldName[java.lang.Long]("amount", new Positive))
    orders.apply("ById", SchemaFilter.create[GenericRecord]().whereFieldId[java.lang.Long](1, new Positive))
    assert(shape(p, "ByName").contains(SchemaShape.Filtered(List(List("amount")))))
    assert(shape(p, "ByTwo").contains(SchemaShape.Filtered(List(List("id", "amount")))))
    assert(shape(p, "Twice").contains(SchemaShape.Filtered(List(List("id"), List("amount")))))
    assert(shape(p, "ById").contains(SchemaShape.Filtered(List(List("amount")))), "a field given by position is resolved to its name")
  }

  // --- groupings ---

  test("a schema Group reads as its key columns, with or without an aggregation; a global one has none") {
    val p = pipeline
    val orders = rows(p, "Read", "orders")
    orders.apply("Keys", Group.byFieldNames[GenericRecord]("id"))
    orders.apply("KeysById", Group.byFieldIds[GenericRecord](1))
    orders.apply("Totals", Group.byFieldNames[GenericRecord]("id", "amount").aggregateField("amount", Count.combineFn[AnyRef]().asInstanceOf[org.apache.beam.sdk.transforms.Combine.CombineFn[AnyRef, AnyRef, java.lang.Long]], "n"))
    orders.apply("All", Group.globally[GenericRecord]())
    orders.apply("AllCounted", Group.globally[GenericRecord]().aggregateField("id", Count.combineFn[AnyRef]().asInstanceOf[org.apache.beam.sdk.transforms.Combine.CombineFn[AnyRef, AnyRef, java.lang.Long]], "n"))
    assert(shape(p, "Keys").contains(SchemaShape.Grouped(List("id"))))
    assert(shape(p, "KeysById").contains(SchemaShape.Grouped(List("amount"))))
    assert(shape(p, "Totals").contains(SchemaShape.Grouped(List("id", "amount"))))
    assert(shape(p, "All").contains(SchemaShape.Grouped(Nil)))
    assert(shape(p, "AllCounted").contains(SchemaShape.Grouped(Nil)))
  }

  // --- joins ---

  test("a schema Join reads as its type and each side's key columns, the right side named by its own collection") {
    val p = pipeline
    val orders = rows(p, "ReadOrders", "orders")
    val customers = rows(p, "ReadCustomers", "customers")
    orders.apply("Inner", Join.innerJoin[GenericRecord, GenericRecord](customers).using("id"))
    orders.apply("On", Join.innerJoin[GenericRecord, GenericRecord](customers).on(Join.FieldsEqual.left("amount").right("id")))
    orders.apply("Left", Join.leftOuterJoin[GenericRecord, GenericRecord](customers).using("id"))
    orders.apply("Right", Join.rightOuterJoin[GenericRecord, GenericRecord](customers).using("id"))
    orders.apply("Full", Join.fullOuterJoin[GenericRecord, GenericRecord](customers).using("id"))
    orders.apply("Broadcast", Join.innerBroadcastJoin[GenericRecord, GenericRecord](customers).using("id"))
    orders.apply("LeftBroadcast", Join.leftOuterBroadcastJoin[GenericRecord, GenericRecord](customers).using("id"))
    def sides(l: String, r: String) = List(JoinSide(orders.getName, "lhs", List(l)), JoinSide(customers.getName, "rhs", List(r)))
    assert(shape(p, "Inner").contains(SchemaShape.Joined(JoinType.Inner, sides("id", "id"))))
    assert(shape(p, "On").contains(SchemaShape.Joined(JoinType.Inner, sides("amount", "id"))))
    assert(shape(p, "Left").contains(SchemaShape.Joined(JoinType.LeftOuter, sides("id", "id"))))
    assert(shape(p, "Right").contains(SchemaShape.Joined(JoinType.RightOuter, sides("id", "id"))))
    assert(shape(p, "Full").contains(SchemaShape.Joined(JoinType.FullOuter, sides("id", "id"))))
    assert(shape(p, "Broadcast").contains(SchemaShape.Joined(JoinType.Inner, sides("id", "id"))))
    assert(shape(p, "LeftBroadcast").contains(SchemaShape.Joined(JoinType.LeftOuter, sides("id", "id"))))
  }

  test("a CoGroup reads as a join of its tagged inputs: outer when one may be missing, a cross product when keyed by nothing") {
    val p = pipeline
    val orders = rows(p, "ReadOrders", "orders")
    val customers = rows(p, "ReadCustomers", "customers")
    def both = PCollectionTuple.of("o", orders).and("c", customers)
    both.apply("Same", CoGroup.join(CoGroup.By.fieldNames("id")).crossProductJoin())
    both.apply("PerInput", CoGroup.join("o", CoGroup.By.fieldNames("amount")).join("c", CoGroup.By.fieldNames("id").withOptionalParticipation()).crossProductJoin())
    both.apply("Grouped", CoGroup.join(CoGroup.By.fieldNames("id")).withKeyField("k"))
    both.apply("Cartesian", CoGroup.join(CoGroup.By.fieldNames()).crossProductJoin())
    def sides(o: List[String], c: List[String]): Set[JoinSide] = Set(JoinSide(orders.getName, "o", o), JoinSide(customers.getName, "c", c))
    def read(name: String): (JoinType, Set[JoinSide]) = shape(p, name) match {
      case Some(SchemaShape.Joined(t, s)) => (t, s.toSet)
      case other                          => fail(s"$name read as $other")
    }
    assert(read("Same") == ((JoinType.Inner, sides(List("id"), List("id")))))
    assert(read("PerInput") == ((JoinType.FullOuter, sides(List("amount"), List("id")))))
    assert(read("Grouped") == ((JoinType.Inner, sides(List("id"), List("id")))))
    assert(read("Cartesian") == ((JoinType.Inner, sides(Nil, Nil))))
  }

  // --- what is not a schema transform, and what cannot be read ---

  test("a step that is not a schema transform has no shape, and one whose fields cannot be read is Unreadable, never a guess") {
    val p = pipeline
    val orders = rows(p, "Read", "orders")
    orders.apply("Code", MapElements.into(TypeDescriptor.of(classOf[GenericRecord])).via(new BeamScenarioJobs.IdentityFn))
    val broken = SchemaFilter.create[GenericRecord]().whereFieldName[java.lang.Long]("amount", new Positive)
    orders.apply("Broken", broken)
    assert(!BeamShapes.recognises(step(p, "Code")) && BeamShapes.of(step(p, "Code")).isEmpty)
    // a Beam release that changed what the filter holds: here, its filters taken away after the pipeline was built
    val filters = classOf[SchemaFilter.Inner[_]].getDeclaredField("filters")
    filters.setAccessible(true)
    filters.set(broken, null)
    assert(BeamShapes.recognises(step(p, "Broken")))
    assert(BeamShapes.of(step(p, "Broken")).exists {
      case SchemaShape.Unreadable(why) => why.contains("Filter$Inner") && why.contains("could not be read")
      case _                           => false
    })
  }

  test("a descriptor's columns: a nested field is its top-level column, * is every column, a position needs a schema") {
    val nested = BeamSchema.builder().addInt64Field("id").addRowField("customer", BeamSchema.builder().addStringField("name").build()).build()
    assert(BeamShapes.columns(FieldAccessDescriptor.withFieldNames("customer.name"), Some(nested)) == List("customer"))
    assert(BeamShapes.columns(FieldAccessDescriptor.withFieldNames("id", "customer.name"), Some(nested)) == List("id", "customer"))
    assert(BeamShapes.columns(FieldAccessDescriptor.withAllFields(), Some(nested)) == List("id", "customer"))
    assert(BeamShapes.columns(FieldAccessDescriptor.withFieldNames("id"), None) == List("id"), "a name needs no schema")
    intercept[IllegalStateException](BeamShapes.columns(FieldAccessDescriptor.withFieldIds(0), None))
    intercept[IllegalStateException](BeamShapes.columns(FieldAccessDescriptor.withAllFields(), None))
  }

  // --- as plans ---

  private def writePlan(p: Pipeline, location: String = "/data/out/report"): Plan =
    BeamTranslator.translate(p).writes.find(_.location == location).getOrElse(fail(s"no write to $location")).plan.input

  private def col(name: String, qualifier: Option[String] = None) = ColumnReference(ColumnRef(name, qualifier))

  test("a filter, a grouping and a join become the plan nodes they are, over the reads that feed them") {
    val p = pipeline
    val orders = rows(p, "ReadOrders", "orders")
    val customers = rows(p, "ReadCustomers", "customers")
    val filtered = orders.apply("Positive", SchemaFilter.create[GenericRecord]().whereFieldName[java.lang.Long]("amount", new Positive))
    toParquet(filtered, "/data/out/filtered")
    toParquet(orders.apply("Keys", Group.byFieldNames[GenericRecord]("id")), "/data/out/grouped")
    toParquet(orders.apply("Join", Join.innerJoin[GenericRecord, GenericRecord](customers).using("id")), "/data/out/joined")
    toParquet(orders.apply("LeftJoin", Join.leftOuterJoin[GenericRecord, GenericRecord](customers).using("id")), "/data/out/left")
    val ordersRead = Read(DatasetRef("/data/in/orders"))
    val customersRead = Read(DatasetRef("/data/in/customers"))
    assert(writePlan(p, "/data/out/filtered") == IrFilter(ordersRead, UDF(None, List(col("amount")), Some(BeamTranslator.PredicateEngineType))))
    assert(writePlan(p, "/data/out/grouped") == Aggregate(ordersRead, List(col("id")), Nil))
    assert(writePlan(p, "/data/out/joined") == IrJoin(ordersRead, customersRead, JoinType.Inner, Some(Comparison("=", col("id", Some("lhs")), col("id", Some("rhs"))))))
    // read from the Join itself, not from the CoGroup it is built from (which would make it a full outer join)
    assert(writePlan(p, "/data/out/left") == IrJoin(ordersRead, customersRead, JoinType.LeftOuter, Some(Comparison("=", col("id", Some("lhs")), col("id", Some("rhs"))))))
  }

  test("several predicates are all required; a three-way CoGroup is joined to its first input; keyed by nothing, it has no condition") {
    val p = pipeline
    val orders = rows(p, "ReadOrders", "orders")
    val customers = rows(p, "ReadCustomers", "customers")
    val regions = rows(p, "ReadRegions", "regions")
    toParquet(orders.apply("Twice", SchemaFilter.create[GenericRecord]().whereFieldName[java.lang.Long]("id", new Positive).whereFieldName[java.lang.Long]("amount", new Positive)), "/data/out/filtered")
    toParquet(PCollectionTuple.of("o", orders).and("c", customers).and("r", regions).apply("Three", CoGroup.join(CoGroup.By.fieldNames("id")).crossProductJoin()), "/data/out/three")
    toParquet(PCollectionTuple.of("o", orders).and("c", customers).apply("Cartesian", CoGroup.join(CoGroup.By.fieldNames()).crossProductJoin()), "/data/out/cartesian")
    val predicate = (c: String) => UDF(None, List(col(c)), Some(BeamTranslator.PredicateEngineType))
    assert(writePlan(p, "/data/out/filtered") == IrFilter(Read(DatasetRef("/data/in/orders")), BooleanExpr("AND", List(predicate("id"), predicate("amount")))))
    val joins = writePlan(p, "/data/out/three") match {
      case IrJoin(IrJoin(_, _, JoinType.Inner, Some(first)), _, JoinType.Inner, Some(second)) => List(first, second)
      case other                                                                            => fail(s"not two joins: $other")
    }
    // every condition matches a side with the first input, on id
    assert(joins.forall { case Comparison("=", ColumnReference(l), ColumnReference(r)) => l.name == "id" && r.name == "id" && l.qualifier != r.qualifier; case _ => false })
    assert(writePlan(p, "/data/out/cartesian") match { case IrJoin(_, _, _, None) => true; case _ => false })
  }

  test("the right side of a join is a read of the write it feeds, though the join step lists only its left") {
    val p = pipeline
    val orders = rows(p, "ReadOrders", "orders")
    val customers = rows(p, "ReadCustomers", "customers")
    assert(step(p, "ReadOrders") != null)
    val joined = orders.apply("Join", Join.leftOuterJoin[GenericRecord, GenericRecord](customers).using("id"))
    assert(step(p, "Join").inputs.map(_.name) == List(orders.getName), "Beam lists only the left side on the join step itself")
    toParquet(joined)
    def reads(plan: Plan): List[String] = plan match {
      case Read(DatasetRef(l), _, _) => List(l)
      case other                     => other.children.flatMap(reads)
    }
    assert(reads(writePlan(p)).toSet == Set("/data/in/orders", "/data/in/customers"))
  }

  test("a team's own step that holds a schema transform is looked into, so the filter inside it is seen") {
    val p = pipeline
    val orders = rows(p, "ReadOrders", "orders")
    toParquet(orders.apply("CleanOrders", new CleanOrders))
    assert(writePlan(p) match {
      case UnknownPlan(_, _, List(IrFilter(Read(DatasetRef("/data/in/orders"), _, _), cond))) => cond.references.map(_.name) == Set("amount")
      case other                                                                            => fail(s"the filter inside the team's step is not seen: $other")
    })
  }

  // --- what the rules cannot see past ---

  test("steps that combine collections in code are listed for the write they feed; a union is not, nor is a schema join") {
    val p = pipeline
    val orders = rows(p, "ReadOrders", "orders")
    val customers = rows(p, "ReadCustomers", "customers")
    def keyed(c: PCollection[GenericRecord], name: String): PCollection[KV[String, GenericRecord]] =
      c.apply(name, MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptor.of(classOf[GenericRecord]))).via(new KeyById))
        .setCoder(KvCoder.of(StringUtf8Coder.of(), c.getCoder))
    val coGrouped = KeyedPCollectionTuple.of(new TupleTag[GenericRecord]("o"), keyed(orders, "KeyO")).and(new TupleTag[GenericRecord]("c"), keyed(customers, "KeyC"))
      .apply("CoGroupByKey", CoGroupByKey.create[String]())
    toParquet(coGrouped.apply("Merge", MapElements.into(TypeDescriptor.of(classOf[GenericRecord])).via(new FirstOf)).setCoder(orders.getCoder), "/data/out/cogrouped")
    val lookup = customers.apply("Lookup", View.asList[GenericRecord]())
    toParquet(orders.apply("Enrich", ParDo.of(new Enrich(lookup)).withSideInputs(lookup)).setCoder(orders.getCoder), "/data/out/enriched")
    toParquet(PCollectionList.of(orders).and(customers).apply("Union", Flatten.pCollections[GenericRecord]()), "/data/out/union")
    toParquet(orders.apply("Join", Join.innerJoin[GenericRecord, GenericRecord](customers).using("id")), "/data/out/joined")
    val byLocation = BeamTranslator.translate(p).writes.map(w => w.location -> w).toMap
    assert(byLocation("/data/out/cogrouped").opaqueCombines == List("CoGroupByKey (CoGroupByKey)"))
    assert(byLocation("/data/out/enriched").opaqueCombines == List("Enrich (ParDo$SingleOutput)"), "a function given another collection as a side input")
    assert(byLocation("/data/out/union").opaqueCombines.isEmpty, "a union matches no row with another")
    assert(byLocation("/data/out/joined").opaqueCombines.isEmpty && byLocation("/data/out/joined").unreadableShapes.isEmpty)
  }

  // --- enforcement ---

  /** A contract for the pipelines here: `inputs` read, `out/report` written, and `rules` (none when empty). */
  private def contractWith(rules: String, inputs: List[String] = List("orders", "customers")): com.invaract.contract.Contract =
    ContractParser.parse(
      s"""id: shapes
         |version: "1.0.0"
         |inputs:
         |${inputs.map(i => s"  - { name: $i, location: in/$i, format: parquet, schema: { fields: [ { name: id, type: long, required: true }, { name: amount, type: long } ] } }").mkString("\n")}
         |outputs:
         |  - { name: report, location: out/report, format: parquet }
         |${if (rules.isEmpty) "" else s"rules:\n$rules\n"}""".stripMargin
    )

  private def verdict(contract: com.invaract.contract.Contract, p: Pipeline): Either[List[String], Unit] =
    try Right(InvaractBeam.enforce(contract).verify(p))
    catch { case e: ContractViolationException => Left(e.result.violations.map(_.violationType)) }

  private def codeJoin(p: Pipeline): Unit = {
    val orders = rows(p, "ReadOrders", "orders")
    val customers = rows(p, "ReadCustomers", "customers")
    val lookup = customers.apply("Lookup", View.asList[GenericRecord]())
    toParquet(orders.apply("Enrich", ParDo.of(new Enrich(lookup)).withSideInputs(lookup)).setCoder(orders.getCoder))
  }

  test("forbid_cross_join refuses a write fed by a step that combines collections in code, which could be a cross join") {
    val p = pipeline
    codeJoin(p)
    assert(verdict(contractWith("  - type: forbid_cross_join"), p) == Left(List(ViolationType.UnverifiableWrite)))
    val e = intercept[ContractViolationException](InvaractBeam.enforce(contractWith("  - type: forbid_cross_join")).verify(p))
    assert(e.getMessage.contains("Enrich") && e.getMessage.contains("forbid_cross_join"))
  }

  test("the same pipeline is not refused for it under a contract with no forbid_cross_join") {
    val p = pipeline
    codeJoin(p)
    assert(verdict(contractWith("  - type: required_filter_columns\n    columns: [amount]"), p) == Left(List(ViolationType.RuleRequiredFilterColumnsViolation)))
    assert(verdict(contractWith(""), p) == Right(()))
  }

  test("forbid_cross_join passes a schema join on a column, and blocks a CoGroup keyed by nothing as the cross join it is") {
    val keyed = pipeline
    toParquet(rows(keyed, "ReadOrders", "orders").apply("Join", Join.innerJoin[GenericRecord, GenericRecord](rows(keyed, "ReadCustomers", "customers")).using("id")))
    assert(verdict(contractWith("  - type: forbid_cross_join"), keyed) == Right(()))
    val cartesian = pipeline
    toParquet(PCollectionTuple.of("o", rows(cartesian, "ReadOrders", "orders")).and("c", rows(cartesian, "ReadCustomers", "customers"))
      .apply("Cartesian", CoGroup.join(CoGroup.By.fieldNames()).crossProductJoin()))
    assert(verdict(contractWith("  - type: forbid_cross_join"), cartesian) == Left(List(ViolationType.RuleCrossJoinViolation)))
  }

  test("a schema transform whose fields cannot be read refuses a write under any shape rule, and only then") {
    def broken: Pipeline = {
      val p = pipeline
      val filter = SchemaFilter.create[GenericRecord]().whereFieldName[java.lang.Long]("amount", new Positive)
      toParquet(rows(p, "ReadOrders", "orders").apply("Positive", filter))
      val filters = classOf[SchemaFilter.Inner[_]].getDeclaredField("filters")
      filters.setAccessible(true)
      filters.set(filter, null)
      p
    }
    val shaped = contractWith("  - type: required_group_by\n    columns: [id]", inputs = List("orders"))
    val e = intercept[ContractViolationException](InvaractBeam.enforce(shaped).verify(broken))
    assert(e.result.violations.map(_.violationType) == List(ViolationType.UnverifiableWrite))
    assert(e.getMessage.contains("Positive") && e.getMessage.contains("could not be read"))
    assert(verdict(contractWith("", inputs = List("orders")), broken) == Right(()), "no shape rule: nothing it cannot see is asked about")
  }

  test("required_filter_columns and required_group_by are met by schema transforms, and say which columns were used when not") {
    val p = pipeline
    toParquet(rows(p, "ReadOrders", "orders").apply("Positive", SchemaFilter.create[GenericRecord]().whereFieldName[java.lang.Long]("amount", new Positive)))
    val ordersOnly = (rules: String) => contractWith(rules, inputs = List("orders"))
    assert(verdict(ordersOnly("  - type: required_filter_columns\n    columns: [amount]"), p) == Right(()))
    assert(verdict(ordersOnly("  - type: required_filter_columns\n    columns: [id]"), p) == Left(List(ViolationType.RuleRequiredFilterColumnsViolation)))
    val g = pipeline
    toParquet(rows(g, "ReadOrders", "orders").apply("Keys", Group.byFieldNames[GenericRecord]("id")))
    assert(verdict(ordersOnly("  - type: required_group_by\n    columns: [id]"), g) == Right(()))
    assert(verdict(ordersOnly("  - type: required_group_by\n    columns: [amount]"), g) == Left(List(ViolationType.RuleRequiredGroupByViolation)))
  }

  test("an undeclared read reaching a write only through a join's right side is seen, so strict mode refuses it") {
    val p = pipeline
    toParquet(rows(p, "ReadOrders", "orders").apply("Join", Join.innerJoin[GenericRecord, GenericRecord](rows(p, "ReadCustomers", "customers")).using("id")))
    val strict = VerificationOptions(rejectUndeclaredInputs = true)
    val e = intercept[ContractViolationException](InvaractBeam.enforce(contractWith("", inputs = List("orders")), strict).verify(p))
    assert(e.result.violations.exists(v => v.violationType == ViolationType.UndeclaredInput && v.location.exists(_.endsWith("in/customers"))))
    // and a declared one read there is not missing
    InvaractBeam.enforce(contractWith(""), strict).verify(p)
  }

  test("a dry run drafts a contract for a write fed by schema transforms") {
    val events = ListBuffer.empty[NotificationEvent]
    val sink = new NotificationSink { override def publish(event: NotificationEvent): Unit = events += event }
    val p = pipeline
    val orders = rows(p, "ReadOrders", "orders")
    val filtered = orders.apply("Positive", SchemaFilter.create[GenericRecord]().whereFieldName[java.lang.Long]("amount", new Positive))
    toParquet(filtered.apply("Join", Join.innerJoin[GenericRecord, GenericRecord](rows(p, "ReadCustomers", "customers")).using("id")))
    new BeamCheck(None, VerificationOptions(), Some(sink), InvaractBeam.jobInfoOf(p.getOptions), BeamCapabilities.declared).verify(p)
    val Seq(drafted) = events.toList.collect { case e: ContractInferenceEvent => e }
    assert(drafted.status == InferenceStatus.Inferred, drafted)
    assert(drafted.contractYaml.exists(y => y.contains("in/orders") && y.contains("in/customers")))
  }

  // --- probes made permanent (friction log 27) ---

  /** A SQL query over orders and customers, its rows written as orders: Beam SQL builds a GROUP BY and a JOIN from the schema
    * Group and Join, and compiles WHERE and SELECT into its own Calc step. */
  private def sqlJob(query: String): Pipeline = {
    val p = pipeline
    val both = PCollectionTuple.of("o", rows(p, "ReadOrders", "orders")).and("c", rows(p, "ReadCustomers", "customers"))
    toParquet(both.apply("Sql", SqlTransform.query(query))
      .apply("AsOrders", MapElements.into(TypeDescriptor.of(classOf[GenericRecord])).via(new RowAsOrder))
      .setCoder(org.apache.beam.sdk.extensions.avro.coders.AvroCoder.of(OrdersSchema)))
    p
  }

  test("a SQL query's GROUP BY and JOIN are seen, being Beam's own Group and Join; its WHERE is compiled into code and is not") {
    val p = sqlJob("SELECT o.id, SUM(o.amount) AS amount FROM o JOIN c ON o.id = c.id WHERE o.amount > 0 GROUP BY o.id")
    assert(verdict(contractWith("  - type: required_group_by\n    columns: [id]"), p) == Right(()))
    assert(verdict(contractWith("  - type: required_join_columns\n    columns: [id]"), p) == Right(()))
    assert(verdict(contractWith("  - type: forbid_cross_join"), p) == Right(()))
    assert(verdict(contractWith("  - type: required_group_by\n    columns: [amount]"), p) == Left(List(ViolationType.RuleRequiredGroupByViolation)))
    assert(verdict(contractWith("  - type: required_filter_columns\n    columns: [amount]"), p) == Left(List(ViolationType.RuleRequiredFilterColumnsViolation)))
  }

  test("Beam SQL refuses a cross join itself, when the pipeline is built, so none can come from a query") {
    // if a Beam release ever builds one, this fails, and how it is built has to be looked at for forbid_cross_join
    val e = intercept[UnsupportedOperationException](sqlJob("SELECT o.id, o.amount FROM o CROSS JOIN c"))
    assert(e.getMessage.contains("CROSS JOIN"))
  }

  test("a filter is seen under the name the column has where it filters: after a rename, the new name") {
    val p = pipeline
    val renamed = rows(p, "ReadOrders", "orders").apply("Rename", RenameFields.create[GenericRecord]().rename("amount", "total"))
    assert(renamed.getSchema.getFieldNames.asScala.toList == List("id", "total"))
    renamed.apply("Positive", SchemaFilter.create[Row]().whereFieldName[java.lang.Long]("total", new Positive))
    assert(shape(p, "Positive").contains(SchemaShape.Filtered(List(List("total")))))
  }

  test("a pipeline thousands of steps deep is translated and checked without running out of stack") {
    val p = pipeline
    var data: PCollection[GenericRecord] = rows(p, "ReadOrders", "orders")
    (1 to 3000).foreach(i => data = data.apply(s"Positive$i", SchemaFilter.create[GenericRecord]().whereFieldName[java.lang.Long]("amount", new Positive)))
    toParquet(data)
    val started = System.nanoTime()
    assert(verdict(contractWith("  - type: required_filter_columns\n    columns: [amount]", inputs = List("orders")), p) == Right(()))
    assert((System.nanoTime() - started) / 1000000 < 30000, "and in well under a minute")
  }

  // --- the code the tests above are built from ---

  private final class KeyById extends SerializableFunction[GenericRecord, KV[String, GenericRecord]] {
    override def apply(r: GenericRecord): KV[String, GenericRecord] = KV.of(String.valueOf(r.get("id")), r)
  }

  private final class FirstOf extends SerializableFunction[KV[String, org.apache.beam.sdk.transforms.join.CoGbkResult], GenericRecord] {
    override def apply(kv: KV[String, org.apache.beam.sdk.transforms.join.CoGbkResult]): GenericRecord =
      kv.getValue.getAll(new TupleTag[GenericRecord]("o")).iterator().next()
  }

  private final class Enrich(lookup: org.apache.beam.sdk.values.PCollectionView[java.util.List[GenericRecord]]) extends org.apache.beam.sdk.transforms.DoFn[GenericRecord, GenericRecord] {
    @org.apache.beam.sdk.transforms.DoFn.ProcessElement
    def process(c: org.apache.beam.sdk.transforms.DoFn[GenericRecord, GenericRecord]#ProcessContext): Unit = c.output(c.element())
  }

  /** A team's own step: positive amounts only, then whatever else it does (here nothing). */
  private final class CleanOrders extends PTransform[PCollection[GenericRecord], PCollection[GenericRecord]] {
    override def expand(input: PCollection[GenericRecord]): PCollection[GenericRecord] =
      input.apply("Positive", SchemaFilter.create[GenericRecord]().whereFieldName[java.lang.Long]("amount", new Positive))
        .apply("Tidy", MapElements.into(TypeDescriptor.of(classOf[GenericRecord])).via(new BeamScenarioJobs.IdentityFn)).setCoder(input.getCoder)
  }
}
