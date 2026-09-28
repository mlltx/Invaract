// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import com.invaract.fingerprint.TransformationFingerprinter
import com.invaract.ir
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.{Files, Path}

/** Real-Spark tests for `CheckpointRegistry`: everything runs through a real
  * `.checkpoint()`, since the whole mechanism rests on how Spark builds a
  * checkpointed Dataset's plan (its `LogicalRDD` keeps the origin's output
  * attribute ids).
  */
class CheckpointRegistrySpec extends AnyFunSuite with BeforeAndAfterAll {
  private var spark: SparkSession = _
  private var dir: Path = _
  private var ordersPath: String = _
  private var customersPath: String = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder().master("local[*]").appName("CheckpointRegistrySpec")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    dir = Files.createTempDirectory("invaract-checkpoint-registry")
    spark.sparkContext.setCheckpointDir(dir.resolve("ckpt").toString)
    ordersPath = dir.resolve("orders.csv").toString
    customersPath = dir.resolve("customers.csv").toString
    Files.write(dir.resolve("orders.csv"), "order_id,customer_id,amount\n1,a,100\n2,b,200\n".getBytes)
    Files.write(dir.resolve("customers.csv"), "customer_id,name,state\na,Alice,NY\nb,Bob,CA\n".getBytes)
  }

  override def afterAll(): Unit = spark.stop()

  private def read(path: String): DataFrame = spark.read.option("header", "true").option("inferSchema", "true").csv(path)

  /** What `ContractEnforcementRule` does for every analyzed plan it sees. */
  private def see(registry: CheckpointRegistry, df: DataFrame): DataFrame = {
    val plan = df.queryExecution.analyzed
    registry.bind(plan)
    val substituted = registry.substitute(plan).plan
    registry.record(substituted, SparkPlanAdapter.translate(substituted).plan)
    df
  }

  /** `df.checkpoint()` as the rule experiences it: the origin, then the checkpointed Dataset. */
  private def checkpointed(registry: CheckpointRegistry, df: DataFrame, local: Boolean = false): DataFrame =
    see(registry, if (local) see(registry, df).localCheckpoint(true) else see(registry, df).checkpoint(true))

  private def translate(registry: CheckpointRegistry, df: DataFrame) = {
    val substitution = registry.substitute(df.queryExecution.analyzed)
    val result = SparkPlanAdapter.translate(substitution.plan)
    result.copy(diagnostics = result.diagnostics ++ substitution.diagnostics)
  }

  private def readsOf(plan: ir.Plan): Set[String] = StructuralVerifier.collectReads(plan).map(_.dataset.location).toSet
  private def unknownsOf(plan: ir.Plan): List[ir.UnknownPlan] = StructuralVerifier.collectUnknownPlans(plan)

  private def joinedSelect(): DataFrame = {
    val o = read(ordersPath)
    val c = read(customersPath)
    o.join(c, "customer_id").select(o("order_id"), c("name")) // drops customer_id, amount, state
  }

  test("a checkpoint after a multi-input join + column-dropping select resolves to BOTH real reads, with no unknown node") {
    val registry = new CheckpointRegistry
    val ck = checkpointed(registry, joinedSelect())

    val result = translate(registry, ck)

    assert(readsOf(result.plan).map(_.split('/').last) == Set("orders.csv", "customers.csv"))
    assert(unknownsOf(result.plan).isEmpty, ir.PlanPrinter.render(result.plan))
    assert(result.diagnostics.isEmpty, result.diagnostics.toString)
  }

  test("resolution works for .localCheckpoint() too") {
    val registry = new CheckpointRegistry
    val result = translate(registry, checkpointed(registry, joinedSelect(), local = true))
    assert(readsOf(result.plan).size == 2)
  }

  test("a checkpoint whose origin the registry never saw stays an opaque LogicalRDD, with a diagnostic saying so") {
    val registry = new CheckpointRegistry
    val ck = see(registry, joinedSelect().checkpoint(true)) // origin never shown to the registry

    val result = translate(registry, ck)

    assert(unknownsOf(result.plan).map(_.sourceType) == List("LogicalRDD"))
    assert(readsOf(result.plan).isEmpty)
    assert(result.diagnostics.exists(_.message.contains("cannot see past")), result.diagnostics.toString)
  }

  test("a checkpoint the registry never saw CREATED (never bound) is unresolved even if its origin was recorded") {
    val registry = new CheckpointRegistry
    val ck = see(registry, joinedSelect()).checkpoint(true) // origin seen; the checkpointed Dataset never shown to bind()

    assert(unknownsOf(translate(registry, ck).plan).map(_.sourceType) == List("LogicalRDD"))
  }

  test("without any registry, translation is exactly as before: an opaque LogicalRDD") {
    val ck = joinedSelect().checkpoint(true)
    val result = SparkPlanAdapter.translate(ck.queryExecution.analyzed)
    assert(unknownsOf(result.plan).map(_.sourceType) == List("LogicalRDD"))
  }

  test("a plan built on top of a checkpoint sees the real reads through it (the same leaf flows into filter/select/SQL)") {
    val registry = new CheckpointRegistry
    val ck = checkpointed(registry, joinedSelect())
    val downstream = ck.filter(col("order_id") > 0).select(col("name"))
    ck.createOrReplaceTempView("registry_spec_ck")
    val viaSql = spark.sql("select name from registry_spec_ck where order_id > 0")

    assert(readsOf(translate(registry, downstream).plan).size == 2)
    assert(readsOf(translate(registry, viaSql).plan).size == 2)
  }

  test("chained checkpoints resolve through each other") {
    val registry = new CheckpointRegistry
    val first = checkpointed(registry, joinedSelect())
    val second = checkpointed(registry, first.select(col("name")))

    val result = translate(registry, second)

    assert(readsOf(result.plan).size == 2, ir.PlanPrinter.render(result.plan))
    assert(unknownsOf(result.plan).isEmpty)
  }

  test("a plan DERIVED from a checkpoint that shares its ids (a filter) never changes what that checkpoint resolves to") {
    val registry = new CheckpointRegistry
    val ck = checkpointed(registry, joinedSelect())
    // The rule sees this too; a filter preserves its input's output attribute ids.
    val derived = see(registry, ck.filter(col("order_id") > 0))

    val throughCheckpoint = translate(registry, ck)
    val throughDerived = translate(registry, derived)

    assert(readsOf(throughCheckpoint.plan).size == 2 && readsOf(throughDerived.plan).size == 2)
    assert(unknownsOf(throughCheckpoint.plan).isEmpty && unknownsOf(throughDerived.plan).isEmpty)
    def hasFilter(p: ir.Plan): Boolean = p.isInstanceOf[ir.Filter] || p.children.exists(hasFilter)
    assert(!hasFilter(throughCheckpoint.plan), "ck itself was bound to the origin at creation - the later filter must not leak into it")
    assert(hasFilter(throughDerived.plan))
    assert(throughCheckpoint.diagnostics.isEmpty, throughCheckpoint.diagnostics.toString)
  }

  test("iterative pattern: re-checkpointing a filter of a checkpoint (same ids) resolves to THAT filter, and discloses the shared ids") {
    val registry = new CheckpointRegistry
    val first = checkpointed(registry, joinedSelect())
    val filtered = see(registry, first.filter(col("order_id") > 0))
    val second = see(registry, filtered.checkpoint(true)) // same output ids as `first`

    val viaSecond = translate(registry, second)
    val viaFirst = translate(registry, first)

    def hasFilter(p: ir.Plan): Boolean = p.isInstanceOf[ir.Filter] || p.children.exists(hasFilter)
    assert(readsOf(viaSecond.plan).size == 2 && unknownsOf(viaSecond.plan).isEmpty)
    assert(hasFilter(viaSecond.plan), "second was made from the filter, so its origin includes it")
    assert(!hasFilter(viaFirst.plan), "first is unaffected by second's existence")
    assert(viaSecond.diagnostics.exists(_.nodeType == CheckpointRegistry.ResolutionDiagnosticType), viaSecond.diagnostics.toString)
  }

  test("two plans with the same output columns reading DIFFERENT sources make the origin ambiguous - left unresolved, never guessed") {
    val registry = new CheckpointRegistry
    val orders = see(registry, read(ordersPath))
    val customers = read(customersPath)
    // Same output attribute ids as `orders`, but this plan also reads customers.
    see(registry, orders.join(customers, "customer_id").select(orders("order_id"), orders("customer_id"), orders("amount")))

    val result = translate(registry, see(registry, orders.checkpoint(true)))

    assert(unknownsOf(result.plan).map(_.sourceType) == List("LogicalRDD"))
    assert(result.diagnostics.exists(_.message.contains("different sources")), result.diagnostics.toString)
  }

  test("two plans with the same output columns reading the SAME sources resolve to the most recent, flagged for disclosure") {
    val registry = new CheckpointRegistry
    val base = see(registry, read(ordersPath))
    val filtered = see(registry, base.filter(col("amount") > 0)) // a filter preserves output ids

    val result = translate(registry, see(registry, filtered.checkpoint(true)))

    assert(readsOf(result.plan).size == 1)
    assert(unknownsOf(result.plan).isEmpty)
    def hasFilter(p: ir.Plan): Boolean = p.isInstanceOf[ir.Filter] || p.children.exists(hasFilter)
    assert(hasFilter(result.plan), "the most recently recorded plan (the filter) is the one spliced in")
    assert(result.diagnostics.exists(_.nodeType == CheckpointRegistry.ResolutionDiagnosticType), result.diagnostics.toString)
  }

  test("seeing the identical plan twice is not ambiguity") {
    val registry = new CheckpointRegistry
    val df = read(ordersPath)
    see(registry, df)
    see(registry, df)

    val result = translate(registry, see(registry, df.checkpoint(true)))

    assert(readsOf(result.plan).size == 1)
    assert(result.diagnostics.isEmpty, result.diagnostics.toString)
  }

  test("the registry is bounded: an origin evicted before its checkpoint is created falls back to an opaque LogicalRDD") {
    val registry = new CheckpointRegistry(maxEntries = 2)
    val first = see(registry, read(ordersPath))
    see(registry, read(customersPath))
    see(registry, read(ordersPath)) // a third distinct plan evicts the eldest (`first`)

    val result = translate(registry, see(registry, first.checkpoint(true)))

    assert(unknownsOf(result.plan).map(_.sourceType) == List("LogicalRDD"))
  }

  test("recently USED entries survive eviction (access-ordered LRU)") {
    val registry = new CheckpointRegistry(maxEntries = 2)
    val first = see(registry, read(ordersPath))
    see(registry, read(customersPath))
    see(registry, first.checkpoint(true)) // bind() touches `first`'s entry, leaving the customers entry eldest
    see(registry, read(ordersPath)) // a third distinct plan: evicts customers, not `first`

    assert(unknownsOf(translate(registry, see(registry, first.checkpoint(true))).plan).isEmpty)
  }

  test("a binding survives its origin being evicted afterwards (the snapshot is taken at creation)") {
    val registry = new CheckpointRegistry(maxEntries = 1)
    val ck = checkpointed(registry, read(ordersPath))
    see(registry, read(customersPath)) // evicts the orders entry

    assert(readsOf(translate(registry, ck).plan).size == 1)
  }

  test("a plan with no output attributes is not recorded and does not disturb the registry") {
    val registry = new CheckpointRegistry
    val df = see(registry, read(ordersPath))
    registry.record(org.apache.spark.sql.catalyst.plans.logical.LocalRelation(Seq.empty), ir.UnknownPlan("x", "X"))

    assert(readsOf(translate(registry, see(registry, df.checkpoint(true))).plan).size == 1)
  }

  test("FINGERPRINT: a plan through a resolved checkpoint fingerprints identically to the same plan with no checkpoint at all") {
    val registry = new CheckpointRegistry
    val origin = see(registry, joinedSelect())
    val viaCheckpoint = see(registry, origin.checkpoint(true)).select(col("name"), col("order_id"))
    val direct = origin.select(col("name"), col("order_id"))

    def fingerprint(df: DataFrame) = {
      val write = ir.Write(ir.DatasetRef("out"), translate(registry, df).plan)
      TransformationFingerprinter.fingerprint(write, None)
    }

    assert(fingerprint(viaCheckpoint).overall == fingerprint(direct).overall)
    assert(fingerprint(viaCheckpoint).outputs == fingerprint(direct).outputs)
  }

  test("FINGERPRINT: an UNRESOLVED checkpoint fingerprints differently from the resolved one, and stably") {
    val registry = new CheckpointRegistry
    val origin = see(registry, joinedSelect())
    val viaCheckpoint = see(registry, origin.checkpoint(true)).select(col("name"))
    val resolved = TransformationFingerprinter.fingerprint(ir.Write(ir.DatasetRef("out"), translate(registry, viaCheckpoint).plan), None)

    val emptyRegistry = new CheckpointRegistry
    def unresolved() =
      TransformationFingerprinter.fingerprint(ir.Write(ir.DatasetRef("out"), translate(emptyRegistry, viaCheckpoint).plan), None)

    assert(unresolved().overall == unresolved().overall, "deterministic even when the boundary can't be seen through")
    assert(unresolved().overall != resolved.overall, "resolving must actually change what is fingerprinted")
  }
}
