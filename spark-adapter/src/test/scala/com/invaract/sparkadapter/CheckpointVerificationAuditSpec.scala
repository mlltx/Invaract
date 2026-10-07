// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter


import com.invaract.verification.{ContractViolationException, DataQualityVerdict, RoleConformanceVerdict, SensitivityLineage, VerificationOptions, ViolationType}
import com.invaract.contract.{Contract, ContractParser}
import com.invaract.ir
import com.invaract.verification.notification.{ContractValidationEvent, NotificationSink, TestNotificationSink}

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.functions._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.{Files, Path}
import scala.collection.mutable.ListBuffer

/** Audit of every verifier that reads `Read` nodes, input schemas or lineage,
  * now that `CheckpointRegistry` splices a checkpoint's origin back into the plan
  * before translation. Before it did, a `.checkpoint()` made the origin's reads,
  * joins, filters and lineage invisible, so every input-side check silently did not
  * run (or, for the plan-shape rules, ran against a plan missing what was upstream
  * of the checkpoint). Each test runs ONE job three ways and compares:
  *
  *  - `ck`: the job with its `.checkpoint()`, under the real rule (registry on);
  *  - `twin`: the identical job with no checkpoint at all;
  *  - `legacy`: the checkpointed job's own write plan verified by a rule with no
  *    registry - what happened before checkpoints were resolved.
  *
  * The claim under test is that `ck` is verified exactly as `twin` is (the IR is
  * identical - see the fingerprint parity tests), and that `legacy` differed in the
  * documented way. Real `local[*]` Spark throughout.
  */
class CheckpointVerificationAuditSpec extends AnyFunSuite with BeforeAndAfterAll {
  import CheckpointVerificationAuditSpec._

  private var spark: SparkSession = _
  private var dir: Path = _
  private var ordersPath: String = _
  private var customersPath: String = _
  private var calendarPath: String = _
  private var dataPath: String = _

  @volatile private var activeContract: Option[Contract] = None
  @volatile private var activeOptions: VerificationOptions = VerificationOptions()
  @volatile private var activeSink: Option[NotificationSink] = None
  private val capturedPlans = ListBuffer.empty[LogicalPlan]
  private val registry = new CheckpointRegistry

  override def beforeAll(): Unit = {
    dir = Files.createTempDirectory("invaract-checkpoint-audit")
    spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("CheckpointVerificationAuditSpec")
      .config("spark.sql.warehouse.dir", dir.resolve("warehouse").toString)
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.ui.enabled", "false")
      .withExtensions { ext =>
        ext.injectCheckRule { _ => (plan: LogicalPlan) =>
          capturedPlans += plan
          activeContract.foreach(c =>
            ContractEnforcementRule.verifyOrThrow(c, plan, activeOptions, activeSink, checkpointRegistry = Some(registry))
          )
        }
      }
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    spark.sparkContext.setCheckpointDir(dir.resolve("ckpt").toString)
    ordersPath = write("orders.csv", "order_id,customer_id,amount\n1,a,100\n2,b,200\n")
    customersPath = write("customers.csv", "customer_id,name,state\na,Alice,NY\nb,Bob,CA\n")
    calendarPath = write("calendar.csv", "gate\n1\n")
    dataPath = write("data.csv", "id\n1\n2\n3\n")
  }

  override def afterAll(): Unit = spark.stop()

  private def write(name: String, content: String): String = {
    val p = dir.resolve(name)
    Files.write(p, content.getBytes)
    p.toString
  }

  private def csv(path: String): DataFrame = spark.read.option("header", "true").option("inferSchema", "true").csv(path)
  private def orders = csv(ordersPath)
  private def customers = csv(customersPath)
  private def outPath(name: String): String = dir.resolve(name).toString

  // ---- harness ----------------------------------------------------------

  private def outcomeOf(thunk: => Unit): Outcome =
    try { thunk; Passed }
    catch { case e: ContractViolationException => Rejected(e.result.violations.map(_.violationType).toSet) }

  private def lastEvent(sink: TestNotificationSink): Option[ContractValidationEvent] =
    sink.events.collect { case e: ContractValidationEvent => e }.lastOption

  /** Runs `build` (given the job's checkpoint function) writing to `out`, under `yaml`. */
  private def attempt(contract: Contract, options: VerificationOptions, out: String)(
      build: (DataFrame => DataFrame) => DataFrame,
      ck: DataFrame => DataFrame
  ): Run = {
    val sink = new TestNotificationSink
    activeContract = Some(contract)
    activeOptions = options
    activeSink = Some(sink)
    try {
      val outcome = outcomeOf(build(ck).write.mode("overwrite").parquet(out))
      Run(outcome, lastEvent(sink))
    } finally { activeContract = None; activeSink = None }
  }

  private def writePlanOf(plans: Seq[LogicalPlan]): LogicalPlan =
    plans.filter(p => WriteCommandSupport.combined.isDefinedAt(p)).last

  private def verdicts(yaml: String, out: String, options: VerificationOptions = VerificationOptions(), withTwin: Boolean = true)(
      build: (DataFrame => DataFrame) => DataFrame
  ): Verdicts = {
    val contract = ContractParser.parse(yaml)
    capturedPlans.clear()
    val withCheckpoint = attempt(contract, options, out)(build, _.checkpoint(true))
    val writePlan = writePlanOf(capturedPlans.toList)
    // A job that overwrites the path it reads has no un-checkpointed twin: Spark itself refuses it.
    val twin = if (withTwin) attempt(contract, options, out)(build, identity) else withCheckpoint
    val legacySink = new TestNotificationSink
    val legacyOutcome = outcomeOf(
      ContractEnforcementRule.verifyOrThrow(contract, writePlan, options, Some(legacySink), checkpointRegistry = None)
    )
    Verdicts(withCheckpoint, twin, Run(legacyOutcome, lastEvent(legacySink)))
  }

  // ---- contract yaml ----------------------------------------------------

  private def schema(fields: (String, String)*): String =
    fields.map { case (n, t) => s"{name: $n, type: $t, nullable: true}" }.mkString("{fields: [", ", ", "]}")

  private def contractYaml(inputs: Seq[(String, String, String)], out: String, outSchema: String, extra: String = ""): String = {
    val in = if (inputs.isEmpty) "" else
      inputs.map { case (n, p, s) => s"  - name: $n\n    location: $p\n    schema: $s" }.mkString("inputs:\n", "\n", "\n")
    s"""id: checkpoint_audit
       |version: "1.0.0"
       |$in
       |outputs:
       |  - name: out
       |    location: $out
       |    schema: $outSchema
       |$extra
       |""".stripMargin
  }

  private lazy val ordersSchema = schema("order_id" -> "integer", "customer_id" -> "string", "amount" -> "integer")
  private lazy val customersSchema = schema("customer_id" -> "string", "name" -> "string", "state" -> "string")
  private val joinedOut = schema("order_id" -> "integer", "name" -> "string")

  private def joinedSelect(ck: DataFrame => DataFrame): DataFrame = {
    val o = orders
    val c = customers
    ck(o.join(c, "customer_id").select(o("order_id"), c("name")))
  }

  private def bothInputs = Seq(("orders", ordersPath, ordersSchema), ("customers", customersPath, customersSchema))

  private def assertSameAsTwin(v: Verdicts): Unit = assert(v.ck.outcome == v.twin.outcome, s"checkpointed ${v.ck.outcome} vs twin ${v.twin.outcome}")

  // ---- StructuralVerifier: input existence / declaration -----------------

  test("UNDECLARED_INPUT: a read hidden behind a checkpoint is rejected under rejectUndeclaredInputs, exactly as without it") {
    val out = outPath("undeclared.parquet")
    val v = verdicts(contractYaml(Seq(("orders", ordersPath, ordersSchema)), out, joinedOut), out, VerificationOptions(rejectUndeclaredInputs = true))(joinedSelect)

    assert(v.ck.outcome == Rejected(Set(ViolationType.UndeclaredInput)))
    assertSameAsTwin(v)
    assert(v.legacy.outcome == Passed, "customers was read upstream of the checkpoint and simply invisible")
  }

  test("UNDECLARED_INPUT: declaring every input passes with the checkpoint, and nothing is left 'unverifiable' any more") {
    val out = outPath("undeclared_ok.parquet")
    val v = verdicts(contractYaml(bothInputs, out, joinedOut), out, VerificationOptions(rejectUndeclaredInputs = true))(joinedSelect)

    assert(v.ck.outcome == Passed && v.twin.outcome == Passed)
    assert(v.ck.event.exists(_.unverifiableInputs.isEmpty))
    // ... whereas the legacy verdict could only say "cannot see whether these were read".
    assert(v.legacy.event.exists(_.unverifiableInputs.map(_.inputName).toSet == Set("orders", "customers")))
  }

  // ---- StructuralVerifier: input schema ------------------------------------

  test("input schema checks run through a checkpoint: a wrong declared type and a missing declared field are rejected") {
    val out = outPath("input_schema.parquet")
    val wrong = "{fields: [{name: order_id, type: integer, nullable: true}, {name: customer_id, type: string, nullable: true}, " +
      "{name: amount, type: string, nullable: true}, {name: discount, type: integer, required: true, nullable: true}]}"
    val v = verdicts(contractYaml(Seq(("orders", ordersPath, wrong), ("customers", customersPath, customersSchema)), out, joinedOut), out)(joinedSelect)

    assert(v.ck.outcome == Rejected(Set(ViolationType.InputFieldTypeMismatch, ViolationType.MissingInputField)))
    assertSameAsTwin(v)
    assert(v.legacy.outcome == Passed, "the input schemas were never compared: the reads were invisible")
  }

  test("input schema checks: the correct declared schema passes with the checkpoint") {
    val out = outPath("input_schema_ok.parquet")
    val v = verdicts(contractYaml(bothInputs, out, joinedOut), out)(joinedSelect)
    assert(v.ck.outcome == Passed && v.twin.outcome == Passed)
  }

  test("rejectUndeclaredFields: an input column the contract does not declare is rejected through a checkpoint") {
    val out = outPath("undeclared_column.parquet")
    val fewer = schema("order_id" -> "integer", "customer_id" -> "string") // no `amount`
    val v = verdicts(
      contractYaml(Seq(("orders", ordersPath, fewer), ("customers", customersPath, customersSchema)), out, joinedOut), out,
      VerificationOptions(rejectUndeclaredFields = true)
    )(joinedSelect)

    assert(v.ck.outcome == Rejected(Set(ViolationType.UndeclaredInputColumn)))
    assertSameAsTwin(v)
    assert(v.legacy.outcome == Passed)
  }

  // ---- StructuralVerifier: catalog identity of an input ---------------------

  test("input catalog requirements are checked through a checkpoint: a path read where a registered table is required is rejected") {
    val out = outPath("input_catalog.parquet")
    val tablePath = outPath("audit_orders_tbl")
    orders.write.option("path", tablePath).mode("overwrite").saveAsTable("audit_orders_tbl")
    def yaml(table: String) =
      s"""id: checkpoint_audit
         |version: "1.0.0"
         |inputs:
         |  - name: orders
         |    location: $tablePath
         |    schema: $ordersSchema
         |    catalog:
         |      required: true
         |      catalogName: spark_catalog
         |      table: $table
         |outputs:
         |  - name: out
         |    location: $out
         |    schema: ${schema("order_id" -> "integer", "amount" -> "integer")}
         |""".stripMargin
    def job(read: () => DataFrame)(ck: DataFrame => DataFrame) = ck(read().select("order_id", "amount"))

    val registered = verdicts(yaml("audit_orders_tbl"), out)(job(() => spark.table("audit_orders_tbl")))
    val mismatched = verdicts(yaml("some_other_table"), out)(job(() => spark.table("audit_orders_tbl")))
    val byPath = verdicts(yaml("audit_orders_tbl"), out)(job(() => spark.read.parquet(tablePath)))

    assert(registered.ck.outcome == Passed && registered.twin.outcome == Passed)
    assert(mismatched.ck.outcome == Rejected(Set(ViolationType.InputCatalogMismatch)))
    assertSameAsTwin(mismatched)
    assert(byPath.ck.outcome == Rejected(Set(ViolationType.MissingInputCatalogRegistration)))
    assertSameAsTwin(byPath)
    assert(mismatched.legacy.outcome == Passed && byPath.legacy.outcome == Passed, "the read's catalog identity was never visible")
  }

  // ---- PlanRuleVerifier -----------------------------------------------------

  private def rule(text: String) = s"rules:\n  - type: $text"

  test("forbid_cross_join: a cross join upstream of a checkpoint is now caught (it used to be invisible)") {
    val out = outPath("cross_join.parquet")
    val yaml = contractYaml(Nil, out, joinedOut, rule("forbid_cross_join"))
    val v = verdicts(yaml, out) { ck =>
      val o = orders; val c = customers
      ck(o.crossJoin(c).select(o("order_id"), c("name")))
    }

    assert(v.ck.outcome == Rejected(Set(ViolationType.RuleCrossJoinViolation)))
    assertSameAsTwin(v)
    assert(v.legacy.outcome == Passed)
  }

  test("forbid_cross_join: a keyed join upstream of a checkpoint passes") {
    val out = outPath("keyed_join.parquet")
    val v = verdicts(contractYaml(Nil, out, joinedOut, rule("forbid_cross_join")), out)(joinedSelect)
    assert(v.ck.outcome == Passed && v.twin.outcome == Passed && v.legacy.outcome == Passed)
  }

  test("required_filter_columns: a filter upstream of the checkpoint now satisfies it (it used to be a FALSE violation)") {
    val out = outPath("filter_rule.parquet")
    val yaml = contractYaml(Nil, out, schema("order_id" -> "integer", "amount" -> "integer"), s"rules:\n  - type: required_filter_columns\n    columns: [amount]")
    val filtered = verdicts(yaml, out)(ck => ck(orders.filter(col("amount") > 0).select("order_id", "amount")))
    val unfiltered = verdicts(yaml, out)(ck => ck(orders.select("order_id", "amount")))

    assert(filtered.ck.outcome == Passed)
    assertSameAsTwin(filtered)
    assert(filtered.legacy.outcome == Rejected(Set(ViolationType.RuleRequiredFilterColumnsViolation)), "the filter was invisible, so a compliant job was blocked")
    // ... and a job that genuinely never filters is still rejected, checkpoint or not.
    assert(unfiltered.ck.outcome == Rejected(Set(ViolationType.RuleRequiredFilterColumnsViolation)))
    assertSameAsTwin(unfiltered)
  }

  test("required_group_by: a grouping upstream of the checkpoint now satisfies it (it used to be a FALSE violation)") {
    val out = outPath("group_rule.parquet")
    val yaml = contractYaml(Nil, out, schema("customer_id" -> "string", "total" -> "long"), s"rules:\n  - type: required_group_by\n    columns: [customer_id]")
    val grouped = verdicts(yaml, out)(ck => ck(orders.groupBy("customer_id").agg(sum("amount").as("total"))))
    val plain = verdicts(yaml, out)(ck => ck(orders.select(col("customer_id"), col("amount").cast("long").as("total"))))

    assert(grouped.ck.outcome == Passed)
    assertSameAsTwin(grouped)
    assert(grouped.legacy.outcome == Rejected(Set(ViolationType.RuleRequiredGroupByViolation)))
    assert(plain.ck.outcome == Rejected(Set(ViolationType.RuleRequiredGroupByViolation)))
    assertSameAsTwin(plain)
  }

  test("required_join_columns: a join upstream of the checkpoint is checked (satisfied and violated)") {
    val out = outPath("join_rule.parquet")
    def yaml(cols: String) = contractYaml(Nil, out, joinedOut, s"rules:\n  - type: required_join_columns\n    columns: [$cols]")
    val ok = verdicts(yaml("customer_id"), out)(joinedSelect)
    val bad = verdicts(yaml("region"), out)(joinedSelect)

    assert(ok.ck.outcome == Passed)
    assertSameAsTwin(ok)
    assert(ok.legacy.outcome == Rejected(Set(ViolationType.RuleRequiredJoinColumnsViolation)), "the join was invisible")
    assert(bad.ck.outcome == Rejected(Set(ViolationType.RuleRequiredJoinColumnsViolation)))
    assertSameAsTwin(bad)
  }

  // ---- RoleConsistencyVerifier ---------------------------------------------

  private def roleYaml(out: String) =
    s"""id: checkpoint_audit
       |version: "1.0.0"
       |inputs:
       |  - name: calendar
       |    location: $calendarPath
       |    type: CONTROL
       |    schema: ${schema("gate" -> "integer")}
       |  - name: data
       |    location: $dataPath
       |    schema: ${schema("id" -> "integer")}
       |outputs:
       |  - name: out
       |    location: $out
       |    type: DATA_ASSET
       |    schema: ${schema("id" -> "integer")}
       |""".stripMargin

  test("roleConsistency: a CONTROL input whose data reaches an output column, upstream of a checkpoint, is now a Contradicts") {
    val out = outPath("role_contradicts.parquet")
    val v = verdicts(roleYaml(out), out, VerificationOptions(roleConsistency = true)) { ck =>
      val cal = csv(calendarPath); val data = csv(dataPath)
      ck(data.crossJoin(cal).select(cal("gate").as("id")))
    }

    assert(v.ck.outcome == Rejected(Set(ViolationType.RoleConsistencyViolation)))
    assertSameAsTwin(v)
    assert(v.legacy.outcome == Passed)
    assert(v.legacy.event.exists(_.roleConformance.isEmpty), "no role verdict could be reached at all")
  }

  private def gatingJob(selectId: DataFrame => org.apache.spark.sql.Column)(ck: DataFrame => DataFrame): DataFrame = {
    val cal = csv(calendarPath); val data = csv(dataPath)
    ck(data.join(cal, data("id") === cal("gate")).select(selectId(data)))
  }

  test("roleConsistency: a CONTROL input used only to gate rows conforms through a checkpoint") {
    val out = outPath("role_conforms.parquet")
    // `.as("id")` gives the output column a fresh attribute id - see the ambiguity test below for why that matters.
    val v = verdicts(roleYaml(out), out, VerificationOptions(roleConsistency = true))(gatingJob(_("id").as("id")))

    assert(v.ck.outcome == Passed && v.twin.outcome == Passed)
    def calendarVerdict(r: Run) = r.event.flatMap(_.roleConformance.find(_.dataset == "calendar")).map(_.verdict)
    assert(calendarVerdict(v.ck) == Some(RoleConformanceVerdict.Conforms))
    assert(calendarVerdict(v.twin) == Some(RoleConformanceVerdict.Conforms))
    assert(calendarVerdict(v.legacy).isEmpty)
  }

  // The most ordinary shape a checkpoint's origin can have: join with a small lookup, then keep ONE side's
  // columns as they are. The output attribute ids are then the plain read's own, so two recorded plans share them
  // while reading different sources: CheckpointRegistry refuses to guess (see its class doc) and the checkpoint
  // stays opaque. Nothing is falsely rejected - the inputs are reported UnverifiableInput - but no input-side
  // verdict (role, plan-shape rules, lineage) can be reached for this job.
  test("KNOWN LIMIT: joining with a lookup and selecting one side's columns unaliased leaves the checkpoint unresolved (aliasing resolves it)") {
    val out = outPath("role_ambiguous.parquet")
    val ambiguous = verdicts(roleYaml(out), out, VerificationOptions(roleConsistency = true))(gatingJob(_("id")))
    val aliased = verdicts(roleYaml(out), out, VerificationOptions(roleConsistency = true))(gatingJob(_("id").as("id")))

    assert(ambiguous.ck.outcome == Passed && ambiguous.twin.outcome == Passed)
    assert(ambiguous.ck.event.exists(_.unverifiableInputs.map(_.inputName).toSet == Set("calendar", "data")), "disclosed, not silent")
    assert(ambiguous.ck.event.exists(_.roleConformance.isEmpty), "no role verdict could be reached")
    assert(ambiguous.twin.event.exists(_.roleConformance.nonEmpty), "the un-checkpointed twin does get one")
    assert(aliased.ck.event.exists(_.unverifiableInputs.isEmpty) && aliased.ck.event.exists(_.roleConformance.nonEmpty))
  }

  // A lineage qualifier is the SCOPE Spark reports for a read (its alias, or `location#n` for a repeated unaliased
  // read), not the location a contract declares. Role consistency, sensitivity and dry-run inference used to compare
  // the declared location against the raw qualifier, so every aliased or self-joined input went unchecked - with or
  // without a checkpoint. These run through a checkpoint because that is where the audit found it.

  test("roleConsistency: a CONTROL input read under an alias, contributing to the output, is Contradicts (checkpointed or not)") {
    val out = outPath("role_alias.parquet")
    val v = verdicts(roleYaml(out), out, VerificationOptions(roleConsistency = true)) { ck =>
      val cal = csv(calendarPath).as("c"); val data = csv(dataPath).as("d")
      ck(data.crossJoin(cal).select(col("c.gate").as("id")))
    }

    assert(v.ck.outcome == Rejected(Set(ViolationType.RoleConsistencyViolation)))
    assertSameAsTwin(v)
    assert(v.legacy.outcome == Passed)
  }

  test("roleConsistency: a CONTROL input self-joined WITHOUT aliases (location#n qualifiers), contributing to the output, is Contradicts") {
    val out = outPath("role_selfjoin.parquet")
    val v = verdicts(roleYaml(out), out, VerificationOptions(roleConsistency = true)) { ck =>
      val c1 = csv(calendarPath); val c2 = csv(calendarPath); val data = csv(dataPath)
      ck(data.crossJoin(c1).join(c2, c1("gate") === c2("gate")).select(c1("gate").as("id")))
    }

    assert(v.ck.outcome == Rejected(Set(ViolationType.RoleConsistencyViolation)))
    assertSameAsTwin(v)
  }

  test("sensitivity propagation: tags reach an output column read through an alias, through a checkpoint") {
    val out = outPath("sensitivity_alias.parquet")
    val tagged = "{fields: [{name: order_id, type: integer, nullable: true}, {name: customer_id, type: string, nullable: true, sensitivityTags: [pii]}, {name: amount, type: integer, nullable: true}]}"
    val contract = ContractParser.parse(contractYaml(Seq(("orders", ordersPath, tagged)), out, schema("order_id" -> "integer", "customer_id" -> "string")))
    capturedPlans.clear()

    val run = attempt(contract, VerificationOptions(), out)(ck => ck(orders.as("o").select(col("o.order_id"), col("o.customer_id"))), _.checkpoint(true))
    val plan = writePlanOf(capturedPlans.toList)

    assert(run.outcome == Passed)
    val tags = SensitivityLineage.propagate(SparkAdapterListener.translationFor(plan).plan, contract).map(s => s.lineage.output.name -> s.sensitivityTags).toMap
    assert(tags == Map("order_id" -> Set.empty[String], "customer_id" -> Set("pii")))
  }

  test("dry-run inference: the observed usage of an input read under an alias is inferred, through a checkpoint") {
    val out = outPath("infer_alias.parquet")
    val contract = ContractParser.parse(contractYaml(bothInputs, out, joinedOut))
    capturedPlans.clear()
    attempt(contract, VerificationOptions(), out)({ ck =>
      val o = orders.as("o"); val c = customers.as("c")
      ck(o.join(c, col("o.customer_id") === col("c.customer_id")).select(col("o.order_id"), col("c.name")))
    }, _.checkpoint(true))
    val plan = writePlanOf(capturedPlans.toList)

    val inferred = ListBuffer.empty[Contract]
    ContractEnforcementRule.inferOrIgnore(plan, c => inferred += c, Some(registry))

    val usage = inferred.head.inputs.map(i => i.schema.fields.map(_.name).toSet -> i.description.getOrElse(""))
    assert(usage.size == 2 && usage.forall(_._2.startsWith("Observed: contributes")), usage.toString)
  }

  // ---- StaticDataQualityVerifier -------------------------------------------

  private def dqYaml(out: String) =
    s"""id: checkpoint_audit
       |version: "1.0.0"
       |outputs:
       |  - name: out
       |    location: $out
       |    schema:
       |      fields:
       |        - name: id
       |          type: long
       |          required: true
       |        - name: currency
       |          type: string
       |          required: true
       |          constraints:
       |            - type: equals
       |              value: GBP
       |""".stripMargin

  test("staticDataQuality: a value the transformation provably gets wrong, upstream of a checkpoint, is now a Violated") {
    val out = outPath("dq_violated.parquet")
    val v = verdicts(dqYaml(out), out, VerificationOptions(staticDataQuality = true))(ck => ck(spark.range(3).withColumn("currency", lit("USD"))))

    assert(v.ck.outcome == Rejected(Set(ViolationType.DataQualityViolation)))
    assertSameAsTwin(v)
    assert(v.legacy.outcome == Passed, "an opaque checkpoint can only ever be NotGuaranteed, which never blocks")
  }

  test("staticDataQuality: a proven-satisfied constraint is Guaranteed through a checkpoint (it used to be NotGuaranteed)") {
    val out = outPath("dq_guaranteed.parquet")
    val v = verdicts(dqYaml(out), out, VerificationOptions(staticDataQuality = true))(ck => ck(spark.range(3).withColumn("currency", lit("GBP"))))

    assert(v.ck.outcome == Passed && v.twin.outcome == Passed)
    def verdictFor(r: Run) = r.event.flatMap(_.dataQuality.find(_.field == "currency")).map(_.verdict)
    assert(verdictFor(v.ck) == Some(DataQualityVerdict.Guaranteed))
    assert(verdictFor(v.twin) == Some(DataQualityVerdict.Guaranteed))
    assert(verdictFor(v.legacy) == Some(DataQualityVerdict.NotGuaranteed))
  }

  // ---- SensitivityLineage (report-only) ------------------------------------

  test("sensitivity propagation: a tagged input column's tags reach the output column through a checkpoint") {
    val out = outPath("sensitivity.parquet")
    val tagged = "{fields: [{name: order_id, type: integer, nullable: true}, {name: customer_id, type: string, nullable: true, sensitivityTags: [pii]}, {name: amount, type: integer, nullable: true}]}"
    val yaml = contractYaml(Seq(("orders", ordersPath, tagged)), out, schema("order_id" -> "integer", "customer_id" -> "string"))
    val contract = ContractParser.parse(yaml)
    capturedPlans.clear()

    val run = attempt(contract, VerificationOptions(), out)(ck => ck(orders.select("order_id", "customer_id")), _.checkpoint(true))
    val plan = writePlanOf(capturedPlans.toList)

    assert(run.outcome == Passed)
    def tagsOf(t: TranslationResult): Map[String, Set[String]] =
      SensitivityLineage.propagate(t.plan, contract).map(s => s.lineage.output.name -> s.sensitivityTags).toMap
    // What lastWrite (and so report.json) reports: the resolved plan the rule stashed.
    assert(tagsOf(SparkAdapterListener.translationFor(plan)) == Map("order_id" -> Set.empty[String], "customer_id" -> Set("pii")))
    // Before checkpoints were resolved, the lineage stopped at the checkpoint: nothing was tagged.
    assert(tagsOf(SparkPlanAdapter.translate(plan)).values.forall(_.isEmpty))
  }

  // ---- OrgPolicy: contract-level; its injected rules ride the same verifiers --

  test("org policy: a policy-injected forbid_cross_join is enforced through a checkpoint (org policy itself never reads the plan)") {
    val out = outPath("org_policy.parquet")
    val contract = ContractParser.parse(contractYaml(Nil, out, joinedOut)) // the contract itself declares no rules
    val policy = write("org_policy.yaml", "version: \"1.0\"\ninject:\n  rules:\n    - type: forbid_cross_join\n")
    spark.conf.set(ContractEnforcementRule.OrgPolicyConfKey, policy)
    val (governed, options) =
      try ContractEnforcementRule.enforceOrgPolicy(contract, VerificationOptions(), spark, None, None)
      finally spark.conf.unset(ContractEnforcementRule.OrgPolicyConfKey)
    capturedPlans.clear()

    val run = attempt(contract, VerificationOptions(), out)({ ck =>
      val o = orders; val c = customers
      ck(o.crossJoin(c).select(o("order_id"), c("name")))
    }, _.checkpoint(true))
    val plan = writePlanOf(capturedPlans.toList)

    assert(run.outcome == Passed, "the un-governed contract has no such rule")
    assert(outcomeOf(ContractEnforcementRule.verifyOrThrow(governed, plan, options, None, checkpointRegistry = Some(registry))) ==
      Rejected(Set(ViolationType.RuleCrossJoinViolation)))
    assert(outcomeOf(ContractEnforcementRule.verifyOrThrow(governed, plan, options, None, checkpointRegistry = None)) == Passed)
  }

  // ---- fingerprints and dry-run inference -----------------------------------

  test("fingerprint: a multi-input checkpointed job fingerprints like its un-checkpointed twin; the legacy plan did not") {
    val out = outPath("fingerprint.parquet")
    val v = verdicts(contractYaml(bothInputs, out, joinedOut), out, VerificationOptions(computeFingerprint = true))(joinedSelect)

    assert(v.ck.outcome == Passed && v.twin.outcome == Passed)
    val (ck, twin, legacy) = (v.ck.event.flatMap(_.fingerprints), v.twin.event.flatMap(_.fingerprints), v.legacy.event.flatMap(_.fingerprints))
    assert(ck.isDefined && ck.map(_.overall) == twin.map(_.overall))
    assert(legacy.isDefined && legacy.map(_.overall) != twin.map(_.overall), "the opaque checkpoint fingerprinted as a different transformation")
  }

  test("dry-run inference: the inputs read before a checkpoint are inferred with their real schemas and observed usage") {
    val out = outPath("infer.parquet")
    val contract = ContractParser.parse(contractYaml(bothInputs, out, joinedOut))
    capturedPlans.clear()
    attempt(contract, VerificationOptions(), out)(joinedSelect, _.checkpoint(true))
    val plan = writePlanOf(capturedPlans.toList)

    val resolved = ListBuffer.empty[Contract]
    ContractEnforcementRule.inferOrIgnore(plan, c => resolved += c, Some(registry))
    val legacy = ListBuffer.empty[Contract]
    ContractEnforcementRule.inferOrIgnore(plan, c => legacy += c, None)

    val inputs = resolved.head.inputs
    assert(inputs.map(_.schema.fields.map(_.name).toSet).toSet == Set(Set("order_id", "customer_id", "amount"), Set("customer_id", "name", "state")))
    assert(inputs.forall(_.description.exists(_.startsWith("Observed:"))), inputs.map(_.description).toString)
    assert(legacy.head.inputs.isEmpty)
  }

  // ---- output schema of a checkpointed Dataset ------------------------------

  test("output schema: nullability and types of a checkpointed Dataset are compared exactly like its twin's") {
    val out = outPath("output_schema.parquet")
    def yaml(nullable: Boolean) =
      contractYaml(Nil, out, s"{fields: [{name: order_id, type: integer, nullable: true}, {name: one, type: integer, nullable: $nullable}]}")
    def job(ck: DataFrame => DataFrame) = ck(orders.select(col("order_id"), lit(1).as("one")))
    val declaredNonNull = verdicts(yaml(nullable = false), out)(job)
    val declaredNullable = verdicts(yaml(nullable = true), out)(job)

    assertSameAsTwin(declaredNonNull)
    assertSameAsTwin(declaredNullable)
    assert(declaredNullable.ck.outcome == Passed)
  }

  // ---- the read-then-overwrite-the-same-path pattern -------------------------

  test("a job that reads a path, checkpoints (to be allowed to overwrite it) and overwrites it: the read is now visible as an input") {
    val p = outPath("roundtrip.parquet")
    orders.write.mode("overwrite").parquet(p)
    def yaml(declareInput: Boolean) =
      contractYaml(if (declareInput) Seq(("orders", p, ordersSchema)) else Nil, p, schema("order_id" -> "integer", "amount" -> "integer"))
    def job(ck: DataFrame => DataFrame) = ck(spark.read.parquet(p).select("order_id", "amount"))
    val declared = verdicts(yaml(declareInput = true), p, withTwin = false)(job)
    val undeclared = verdicts(yaml(declareInput = false), p, VerificationOptions(rejectUndeclaredInputs = true), withTwin = false)(job)

    assert(declared.ck.outcome == Passed)
    assert(undeclared.ck.outcome == Rejected(Set(ViolationType.UndeclaredInput)))
    assert(undeclared.legacy.outcome == Passed, "the read of the very path being overwritten was invisible")
  }
}

object CheckpointVerificationAuditSpec {
  sealed trait Outcome
  case object Passed extends Outcome
  final case class Rejected(types: Set[String]) extends Outcome

  final case class Run(outcome: Outcome, event: Option[ContractValidationEvent])
  final case class Verdicts(ck: Run, twin: Run, legacy: Run)
}
