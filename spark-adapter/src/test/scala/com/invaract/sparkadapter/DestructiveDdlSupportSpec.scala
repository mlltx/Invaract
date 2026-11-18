// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import com.invaract.verification.{ContractViolationException, ViolationType}
import com.invaract.verification.notification.{ContractValidationEvent, InferenceStatus, TestNotificationSink}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.{ResolvedTable, UnresolvedPartitionSpec}
import org.apache.spark.sql.catalyst.plans.logical.{DropPartitions, TruncatePartition}
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}

import java.nio.file.{Files, Path}

/** `DROP TABLE`, `TRUNCATE TABLE` and `ALTER TABLE ... DROP PARTITION` (docs/SPARK_ADAPTER_HARDENING_PLAN.md, H-10):
  * removing data from a declared output is allowed when the contract lets that output be overwritten; removing data
  * from anything else is refused, with a message a user can act on. They used to be refused whatever the contract said.
  */
class DestructiveDdlSupportSpec extends ConnectorSpecBase {
  private var dir: Path = _

  override def beforeAll(): Unit = {
    dir = Files.createTempDirectory("invaract-destructive-ddl")
    spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("DestructiveDdlSupportSpec")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.warehouse.dir", dir.resolve("warehouse").toString)
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.ui.enabled", "false")
      .withExtensions(injectContractCheck)
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
  }

  /** A partitioned parquet table with one row in each of two partitions, made before any contract is active. */
  private def freshTable(name: String): Unit = {
    spark.sql(s"DROP TABLE IF EXISTS $name")
    spark.sql(s"CREATE TABLE $name (id BIGINT, dt STRING) USING parquet PARTITIONED BY (dt)")
    spark.sql(s"INSERT INTO $name VALUES (1, 'a'), (2, 'b')")
  }

  private def byName(table: String) = s"spark_catalog/default/$table"
  private def byPath(table: String) = dir.resolve("warehouse").resolve(table).toString

  private def contract(location: String, saveMode: Option[String] = None, tail: String = ""): String =
    (List(
      "id: removals",
      "version: \"1.0.0\"",
      "outputs:",
      "  - name: out",
      s"    location: $location"
    ) ++ saveMode.map(m => s"    saveMode: $m") ++ List(
      "    schema:",
      "      fields:",
      "        - { name: id, type: long }",
      tail
    )).mkString("\n")

  private def run(yaml: String)(sql: String): List[String] =
    try { withContract(yaml)(spark.sql(sql).collect()); Nil }
    catch { case e: ContractViolationException => e.result.violations.map(_.violationType) }

  private def refusal(yaml: String)(sql: String): ContractViolationException =
    intercept[ContractViolationException](withContract(yaml)(spark.sql(sql).collect()))

  private def rows(table: String): Long = spark.table(table).count()

  private val forbidUnconditionalDelete = "rules:\n  - type: forbid_unconditional_delete"

  // -- A declared output that may be overwritten

  test("DROP TABLE of a declared output passes and the table is gone, the output declared by name or by its path") {
    List(byName("t1"), byPath("t1")).foreach { location =>
      freshTable("t1")
      assert(run(contract(location))("DROP TABLE IF EXISTS t1") == Nil, location)
      assert(!spark.catalog.tableExists("t1"), location)
    }
  }

  test("DROP TABLE IF EXISTS of a declared output that does not exist yet passes, declared by its warehouse path or by name") {
    spark.sql("DROP TABLE IF EXISTS not_yet")
    assert(run(contract(byPath("not_yet")))("DROP TABLE IF EXISTS not_yet") == Nil, "the newcomer's case: a draft declares the path")
    assert(run(contract(byName("not_yet")))("DROP TABLE IF EXISTS not_yet") == Nil)
  }

  test("TRUNCATE TABLE of a declared output passes and the table is empty") {
    freshTable("t2")
    assert(run(contract(byName("t2")))("TRUNCATE TABLE t2") == Nil)
    assert(rows("t2") == 0)
  }

  test("ALTER TABLE ... DROP PARTITION of a declared output passes, and only that partition is gone") {
    freshTable("t3")
    assert(run(contract(byName("t3")))("ALTER TABLE t3 DROP IF EXISTS PARTITION (dt='a')") == Nil)
    assert(spark.table("t3").collect().map(_.getString(1)).toList == List("b"))
  }

  test("saveMode: overwrite, in any case, lets the statements through") {
    freshTable("t4")
    assert(run(contract(byName("t4"), Some("Overwrite")))("TRUNCATE TABLE t4 PARTITION (dt='a')") == Nil)
    assert(run(contract(byName("t4"), Some("overwrite")))("DROP TABLE t4") == Nil)
    assert(!spark.catalog.tableExists("t4"))
  }

  test("each allowed statement publishes one PASSED validation, with no violations") {
    freshTable("t5")
    val sink = new TestNotificationSink
    withSink(sink) {
      assert(run(contract(byName("t5")))("ALTER TABLE t5 DROP PARTITION (dt='a')") == Nil)
      assert(run(contract(byName("t5")))("TRUNCATE TABLE t5") == Nil)
      assert(run(contract(byName("t5")))("DROP TABLE t5") == Nil)
    }
    val validations = sink.events.collect { case v: ContractValidationEvent => v }
    assert(validations.map(_.status) == List("PASSED", "PASSED", "PASSED"))
    assert(validations.forall(_.violations.isEmpty))
  }

  test("DROP TABLE of a declared Delta table passes") {
    spark.sql("DROP TABLE IF EXISTS d1")
    spark.sql("CREATE TABLE d1 (id BIGINT) USING delta")
    spark.sql("INSERT INTO d1 VALUES (1)")
    assert(run(contract(byName("d1")))("DROP TABLE d1") == Nil)
    assert(!spark.catalog.tableExists("d1"))
  }

  // -- A declared output that may not be overwritten

  test("saveMode: append: each statement is OUTPUT_SAVE_MODE_MISMATCH, and the data is intact") {
    freshTable("t6")
    val append = contract(byName("t6"), Some("append"))
    List("DROP TABLE t6", "TRUNCATE TABLE t6", "ALTER TABLE t6 DROP PARTITION (dt='a')", "TRUNCATE TABLE t6 PARTITION (dt='a')").foreach { sql =>
      val e = refusal(append)(sql)
      assert(e.result.violations.map(_.violationType) == List(ViolationType.OutputSaveModeMismatch), sql)
      val v = e.result.violations.head
      assert(v.expected.contains("append") && v.actual.contains("overwrite"), sql)
      assert(v.location.exists(_.endsWith("/warehouse/t6")), s"$sql: ${v.location}")
      assert(rows("t6") == 2, sql)
    }
  }

  test("forbid_unconditional_delete: DROP TABLE and TRUNCATE TABLE are refused; a partition's removal passes") {
    freshTable("t7")
    val yaml = contract(byName("t7"), tail = forbidUnconditionalDelete)
    List("DROP TABLE t7", "TRUNCATE TABLE t7").foreach { sql =>
      val e = refusal(yaml)(sql)
      assert(e.result.violations.map(_.violationType) == List(ViolationType.RuleUnconditionalDelete), sql)
      assert(e.result.violations.head.location.exists(_.endsWith("/warehouse/t7")), sql)
      assert(e.result.violations.head.rule.contains("forbid_unconditional_delete"), sql)
      assert(rows("t7") == 2, sql)
    }
    assert(run(yaml)("TRUNCATE TABLE t7 PARTITION (dt='a')") == Nil)
    assert(run(yaml)("ALTER TABLE t7 DROP PARTITION (dt='b')") == Nil)
    assert(rows("t7") == 0)
  }

  test("a custom rule about deletes, which cannot judge a whole-table removal, refuses it; one about updates does not") {
    freshTable("t8")
    def withCustom(className: String) = contract(
      byName("t8"),
      tail = s"customRuleTypes:\n  custom_rule: $className\nrules:\n  - type: custom_rule"
    )
    val e = refusal(withCustom(classOf[AlwaysViolatesRuleVerifier].getName))("TRUNCATE TABLE t8")
    assert(e.result.violations.map(_.violationType) == List(ViolationType.RuleUnverifiableDml))
    assert(e.result.violations.head.actual.contains("DELETE"))
    assert(rows("t8") == 2)
    assert(run(withCustom(classOf[AlwaysViolatesRuleVerifier].getName))("ALTER TABLE t8 DROP PARTITION (dt='a')") == Nil)
    assert(run(withCustom(classOf[ForbidPasswordColumnUpdateVerifier].getName))("TRUNCATE TABLE t8") == Nil)
    assert(rows("t8") == 0)
  }

  test("an invalid contract is reported as such, not judged") {
    freshTable("t9")
    val yaml = contract(byName("t9"), tail = "customRuleTypes:\n  broken: com.invaract.nowhere.Missing\nrules:\n  - type: broken")
    val e = refusal(yaml)("DROP TABLE t9")
    assert(e.result.violations.map(_.violationType) == List(ViolationType.InvalidContract))
    assert(spark.catalog.tableExists("t9"))
  }

  // -- Not a declared output

  test("not a declared output: each statement is UNVERIFIABLE_WRITE, naming the table and what to do, and the data is intact") {
    freshTable("t10")
    val other = contract(byName("somewhere_else"))
    List(
      "DROP TABLE t10",
      "TRUNCATE TABLE t10",
      "TRUNCATE TABLE t10 PARTITION (dt='a')",
      "ALTER TABLE t10 DROP PARTITION (dt='a')"
    ).foreach { sql =>
      val e = refusal(other)(sql)
      assert(e.result.violations.map(_.violationType) == List(ViolationType.UnverifiableWrite), sql)
      val message = e.result.violations.head.message + " " + e.result.violations.head.remediation
      assert(message.contains("spark_catalog/default/t10") && message.contains("Declare"), s"$sql: $message")
      assert(!message.contains("FailClosedCommands"), s"$sql: a user is not told to change Invaract: $message")
      assert(rows("t10") == 2, sql)
    }
  }

  test("DROP TABLE IF EXISTS of a table that does not exist, and is not declared, is refused too") {
    spark.sql("DROP TABLE IF EXISTS no_such_table")
    val e = refusal(contract(byName("somewhere_else")))("DROP TABLE IF EXISTS no_such_table")
    assert(e.result.violations.map(_.violationType) == List(ViolationType.UnverifiableWrite))
    assert(e.getMessage.contains("spark_catalog/default/no_such_table"))
  }

  test("TRUNCATE TABLE of a Delta table: judged by Invaract first - refused when undeclared, and left to Delta when declared") {
    spark.sql("DROP TABLE IF EXISTS d2")
    spark.sql("CREATE TABLE d2 (id BIGINT) USING delta")
    spark.sql("INSERT INTO d2 VALUES (1)")
    val e = refusal(contract(byName("somewhere_else")))("TRUNCATE TABLE d2")
    assert(e.result.violations.map(_.violationType) == List(ViolationType.UnverifiableWrite))
    assert(e.getMessage.contains("spark_catalog/default/d2"))
    // Declared: Invaract lets it through, and what happens next is Delta's own answer (it does not support TRUNCATE).
    val delta = scala.util.Try(withContract(contract(byName("d2")))(spark.sql("TRUNCATE TABLE d2").collect()))
    assert(!delta.failed.toOption.exists(_.isInstanceOf[ContractViolationException]), s"$delta")
    assert(rows("d2") == 1 || delta.isSuccess)
  }

  test("under spark.sql.legacy.useV1Command, DROP TABLE arrives as DropTableCommand and is judged the same way") {
    freshTable("t11")
    spark.conf.set("spark.sql.legacy.useV1Command", "true")
    try {
      val e = refusal(contract(byName("somewhere_else")))("DROP TABLE t11")
      assert(e.result.violations.map(_.violationType) == List(ViolationType.UnverifiableWrite))
      assert(spark.catalog.tableExists("t11"))
      assert(run(contract(byName("t11")))("DROP TABLE t11") == Nil)
      assert(!spark.catalog.tableExists("t11"))
    } finally spark.conf.unset("spark.sql.legacy.useV1Command")
  }

  // -- Statements that delete nothing

  test("DROP VIEW of a persistent view passes under any contract, and the view is gone; CLEAR CACHE passes") {
    freshTable("t12")
    spark.sql("CREATE OR REPLACE VIEW v12 AS SELECT id FROM t12")
    val other = contract(byName("somewhere_else"))
    assert(run(other)("DROP VIEW v12") == Nil)
    assert(!spark.catalog.tableExists("v12"))
    assert(run(other)("CLEAR CACHE") == Nil)
    assert(rows("t12") == 2)
  }

  // -- What the statements are taken to be

  /** What `extract` makes of the plans the rule sees for `sql`: the statement's own, and nothing for any other. */
  private def extracted(sql: String): List[DestructiveDdlSupport.DestructiveDdl] = {
    capturedPlans.clear()
    spark.sql(sql).collect()
    capturedPlans.toList.flatMap(DestructiveDdlSupport.extract)
  }

  test("extract: the statement, whether it removes everything, and the table's location and name") {
    freshTable("t13")
    spark.sql("DROP TABLE IF EXISTS never_made_13")
    val List(drop) = extracted("DROP TABLE IF EXISTS never_made_13")
    assert(drop.statement == "DROP TABLE" && drop.removesEverything)
    assert(drop.tableNames == List(byName("never_made_13")))
    assert(drop.location.endsWith("/warehouse/never_made_13"), drop.location)
    val List(truncate) = extracted("TRUNCATE TABLE t13 PARTITION (dt='a')")
    assert(truncate.statement == "TRUNCATE TABLE ... PARTITION" && !truncate.removesEverything)
    val List(dropPartition) = extracted("ALTER TABLE t13 DROP IF EXISTS PARTITION (dt='zzz')")
    assert(dropPartition.statement == "ALTER TABLE ... DROP PARTITION" && !dropPartition.removesEverything)
    assert(dropPartition.location.endsWith("/warehouse/t13") && dropPartition.tableNames == List(byName("t13")))
    assert(dropPartition.table == byName("t13"))
    val List(whole) = extracted("TRUNCATE TABLE t13")
    assert(whole.statement == "TRUNCATE TABLE" && whole.removesEverything)
    assert(extracted("SELECT id FROM t13") == Nil)
    spark.sql("CREATE OR REPLACE VIEW v13 AS SELECT id FROM t13")
    assert(extracted("DROP VIEW v13") == Nil, "a view holds no data")
  }

  test("extract: the partition statements of a table in a catalog that manages partitions itself") {
    // Neither Delta nor Iceberg manages partitions through these statements (Spark refuses them before Invaract sees
    // them), so the plans are built here from a real Delta table, as a catalog that does manage them would produce.
    spark.sql("DROP TABLE IF EXISTS d3")
    spark.sql("CREATE TABLE d3 (id BIGINT, dt STRING) USING delta PARTITIONED BY (dt)")
    val catalog = spark.sessionState.catalogManager.catalog("spark_catalog").asInstanceOf[TableCatalog]
    val ident = Identifier.of(Array("default"), "d3")
    val resolved = ResolvedTable.create(catalog, ident, catalog.loadTable(ident))
    val spec = UnresolvedPartitionSpec(Map("dt" -> "a"))
    val Some(dropped) = DestructiveDdlSupport.extract(DropPartitions(resolved, Seq(spec), ifExists = true, purge = false))
    val Some(truncated) = DestructiveDdlSupport.extract(TruncatePartition(resolved, spec))
    assert(dropped.statement == "ALTER TABLE ... DROP PARTITION" && !dropped.removesEverything)
    assert(truncated.statement == "TRUNCATE TABLE ... PARTITION" && !truncated.removesEverything)
    List(dropped, truncated).foreach { ddl =>
      assert(ddl.tableNames == List(byName("d3")))
      assert(ddl.location.endsWith("/warehouse/d3"), ddl.location)
    }
  }

  test("dry-run reports a removal as SKIPPED_UNSUPPORTED, saying when enforcement allows it, rather than as a blocker") {
    freshTable("t14")
    capturedPlans.clear()
    spark.sql("TRUNCATE TABLE t14").collect()
    val outcome = ContractEnforcementRule.inferOutcome(capturedPlans.head)
    outcome match {
      case Some(removal @ ContractEnforcementRule.InferenceOutcome.SkippedRemoval(reason, removes)) =>
        assert(removal.status == InferenceStatus.SkippedUnsupported)
        assert(removes.contains(byName("t14")) && removes.exists(_.endsWith("/warehouse/t14")), removes)
        assert(reason.startsWith("TRUNCATE TABLE") && reason.contains("spark_catalog/default/t14"), reason)
        assert(reason.contains("declared output that may be overwritten"), reason)
      case other => fail(s"expected a skipped outcome, got $other")
    }
  }

  test("the newcomer's job: DROP TABLE IF EXISTS, then saveAsTable - its dry-run draft passes the same job when enforced") {
    spark.sql("DROP TABLE IF EXISTS top_customers")
    def job(): Unit = {
      spark.sql("DROP TABLE IF EXISTS top_customers")
      spark.range(5).withColumnRenamed("id", "customer_id").write.saveAsTable("top_customers")
    }
    val sink = new TestNotificationSink
    val reporter = new DryRunReporter(
      sink,
      com.invaract.verification.notification.JobInfo(name = Some("newcomer")),
      com.invaract.verification.VerificationOptions(),
      () => 1L,
      (plan, registry) => ContractEnforcementRule.inferOutcome(plan, Some(registry)),
      Map.empty
    )
    capturedPlans.clear()
    job()
    capturedPlans.toList.foreach(reporter.check)
    reporter.finish()
    val summary = sink.events.collect { case e: com.invaract.verification.notification.DryRunSummaryEvent => e }.last
    val draft = parseContract(summary.mergedContractYaml.getOrElse(fail(s"no merged draft: ${summary.mergeConflicts}")))
    assert(draft.outputs.map(_.saveMode) == List(None), "the write's default mode, error, would refuse the drop")
    withContract(summary.mergedContractYaml.get)(job()) // must not throw
    assert(rows("top_customers") == 5)
  }

  test("withoutSaveModeOf: only an output the job removed data from loses its save mode") {
    val draft = parseContract(contract(byName("kept"), Some("append")).replace("outputs:\n", "outputs:\n  - { name: gone, location: " + byPath("gone") + ", saveMode: error, schema: { fields: [ { name: id, type: long } ] } }\n"))
    assert(DryRunReporter.withoutSaveModeOf(Nil)(draft) eq draft)
    val cleared = DryRunReporter.withoutSaveModeOf(List("file:" + byPath("gone"), byName("gone")))(draft)
    assert(cleared.outputs.map(o => o.name -> o.saveMode) == List("gone" -> None, "out" -> Some("append")))
    assert(DryRunReporter.withoutSaveModeOf(List(byName("elsewhere")))(draft).outputs.map(_.saveMode) == List(Some("error"), Some("append")))
  }

  test("TableTargets: a table of another catalog, or of a nested namespace, is named as it is and not looked up in the session's") {
    import org.apache.spark.sql.catalyst.analysis.ResolvedIdentifier
    import org.apache.spark.sql.connector.catalog.{CatalogPlugin, Identifier}
    import org.apache.spark.sql.util.CaseInsensitiveStringMap
    val other = new CatalogPlugin {
      override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = ()
      override def name(): String = "other_catalog"
    }
    val elsewhere = TableTargets.ofResolvedIdentifier(ResolvedIdentifier(other, Identifier.of(Array("default"), "t15")))
    assert(elsewhere.names == List("other_catalog/default/t15"))
    assert(elsewhere.location == "other_catalog/default/t15", "not the session catalog's default/t15")
    val session = spark.sessionState.catalogManager.catalog("spark_catalog")
    val nested = TableTargets.ofResolvedIdentifier(ResolvedIdentifier(session, Identifier.of(Array("a", "b"), "t15")))
    assert(nested.names == List("spark_catalog/a/b/t15"))
    assert(nested.location == "spark_catalog/a/b/t15", "a namespace the session catalog cannot hold is not looked up in it")
    val inSession = TableTargets.ofResolvedIdentifier(ResolvedIdentifier(session, Identifier.of(Array("default"), "t15")))
    assert(inSession.location.endsWith("/warehouse/t15"), inSession.location)
  }

  test("forbid_unconditional_delete reaches every whole-table form: the legacy DropTableCommand, a Delta TRUNCATE") {
    freshTable("t16")
    spark.conf.set("spark.sql.legacy.useV1Command", "true")
    try {
      val e = refusal(contract(byName("t16"), tail = forbidUnconditionalDelete))("DROP TABLE t16")
      assert(e.result.violations.map(_.violationType) == List(ViolationType.RuleUnconditionalDelete))
      assert(spark.catalog.tableExists("t16"))
    } finally spark.conf.unset("spark.sql.legacy.useV1Command")
    spark.sql("DROP TABLE IF EXISTS d4")
    spark.sql("CREATE TABLE d4 (id BIGINT) USING delta")
    spark.sql("INSERT INTO d4 VALUES (1)")
    val e = refusal(contract(byName("d4"), tail = forbidUnconditionalDelete))("TRUNCATE TABLE d4")
    assert(e.result.violations.map(_.violationType) == List(ViolationType.RuleUnconditionalDelete), "Invaract's refusal, before Delta's own")
  }

  test("a contract with forbid_unconditional_delete and a custom delete rule: the built-in rule's violation") {
    freshTable("t17")
    val both = contract(
      byName("t17"),
      tail = s"customRuleTypes:\n  custom_rule: ${classOf[AlwaysViolatesRuleVerifier].getName}\nrules:\n  - type: custom_rule\n  - type: forbid_unconditional_delete"
    )
    val e = refusal(both)("TRUNCATE TABLE t17")
    assert(e.result.violations.map(_.violationType) == List(ViolationType.RuleUnconditionalDelete))
    assert(rows("t17") == 2)
  }

  test("log-only dry run (no sink) passes over a removal: no draft, no error") {
    freshTable("t18")
    capturedPlans.clear()
    spark.sql("TRUNCATE TABLE t18").collect()
    val drafts = scala.collection.mutable.ListBuffer.empty[com.invaract.contract.Contract]
    capturedPlans.toList.foreach(plan => ContractEnforcementRule.inferOrIgnore(plan, c => drafts += c))
    assert(drafts.isEmpty)
  }
}
