// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import com.invaract.verification.{ContractViolationException, VerificationOptions, ViolationType}
import com.invaract.verification.notification.{ContractValidationEvent, TestNotificationSink}
import org.apache.spark.sql.SparkSession

import java.nio.file.{Files, Path}

/** `ALTER TABLE` on a declared output (docs/SPARK_ADAPTER_HARDENING_PLAN.md, H-05): a statement that changes the
  * table's columns has the columns it leaves checked as a write's would be; one that moves the table away from where
  * the contract says it is (SET LOCATION, RENAME TO, ADD PARTITION ... LOCATION) is refused. These statements used to
  * pass unchecked, leaving an output whose columns no longer matched its contract. A table the contract does not
  * declare is not its concern, as for a write.
  */
class SchemaDdlSupportSpec extends ConnectorSpecBase {
  private var dir: Path = _

  override def beforeAll(): Unit = {
    dir = Files.createTempDirectory("invaract-schema-ddl")
    spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("SchemaDdlSupportSpec")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.warehouse.dir", dir.resolve("warehouse").toString)
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.ui.enabled", "false")
      .withExtensions(injectContractCheck)
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
  }

  /** A parquet table, partitioned by `dt`, made before any contract is active. */
  private def parquetTable(name: String, extraColumn: Option[String] = None): Unit = {
    spark.sql(s"DROP TABLE IF EXISTS $name")
    spark.sql(s"CREATE TABLE $name (id BIGINT, value BIGINT${extraColumn.fold("")(c => s", $c STRING")}, dt STRING) USING parquet PARTITIONED BY (dt)")
    spark.sql(s"INSERT INTO $name SELECT 1, 10${extraColumn.fold("")(_ => ", 'x'")}, 'a'")
  }

  /** A Delta table whose columns can be renamed and dropped (column mapping by name). */
  private def deltaTable(name: String): Unit = {
    spark.sql(s"DROP TABLE IF EXISTS $name")
    spark.sql(
      s"""CREATE TABLE $name (id BIGINT, value BIGINT) USING delta TBLPROPERTIES (
         |'delta.columnMapping.mode' = 'name', 'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""".stripMargin
    )
    spark.sql(s"INSERT INTO $name VALUES (1, 10)")
  }

  private def byName(table: String) = s"spark_catalog/default/$table"

  /** A SQL table's columns are nullable, so the required fields say they may be null (`nullable` defaults to
    * `!required`). */
  private def contract(table: String, valueRequired: Boolean = true, extraFields: String = ""): String =
    s"""id: schema_ddl
       |version: "1.0.0"
       |outputs:
       |  - name: out
       |    location: ${byName(table)}
       |    schema:
       |      fields:
       |        - { name: id, type: long, required: true, nullable: true }
       |        - { name: value, type: long, required: $valueRequired, nullable: true }
       |        - { name: dt, type: string }
       |$extraFields""".stripMargin

  private val strict = VerificationOptions(rejectUndeclaredFields = true)

  private def run(yaml: String, options: VerificationOptions = VerificationOptions())(sql: String): List[(String, Option[String])] =
    try { withContract(yaml, options)(spark.sql(sql).collect()); Nil }
    catch { case e: ContractViolationException => e.result.violations.map(v => v.violationType -> v.column) }

  private def refusal(yaml: String, options: VerificationOptions = VerificationOptions())(sql: String): ContractViolationException =
    intercept[ContractViolationException](withContract(yaml, options)(spark.sql(sql).collect()))

  private def columns(table: String): List[String] = spark.table(table).columns.toList

  private def location(table: String): String =
    spark.sql(s"DESCRIBE TABLE EXTENDED $table").collect().find(_.getString(0) == "Location").map(_.getString(1)).get

  // -- Statements that change a declared output's columns

  test("ADD COLUMNS of an undeclared column, under rejectUndeclaredFields: UNDECLARED_OUTPUT_COLUMN, and the table is unchanged (parquet, Delta)") {
    parquetTable("p1")
    deltaTable("d1")
    List("p1", "d1").foreach { t =>
      val before = columns(t)
      assert(run(contract(t), strict)(s"ALTER TABLE $t ADD COLUMNS (email STRING)") == List(ViolationType.UndeclaredOutputColumn -> Some("email")), t)
      assert(columns(t) == before, t)
    }
  }

  test("the same ADD COLUMNS passes with default options, and when the contract declares the column") {
    parquetTable("p2")
    assert(run(contract("p2"))("ALTER TABLE p2 ADD COLUMNS (email STRING)") == Nil)
    assert(columns("p2").contains("email"))
    deltaTable("d2")
    assert(run(contract("d2", extraFields = "        - { name: email, type: string }\n"), strict)("ALTER TABLE d2 ADD COLUMNS (email STRING)") == Nil)
    assert(columns("d2").contains("email"))
  }

  test("RENAME COLUMN of a required column: MISSING_OUTPUT_FIELD, and the column keeps its name") {
    deltaTable("d3")
    val found = run(contract("d3"))("ALTER TABLE d3 RENAME COLUMN value TO amount")
    assert(found.contains(ViolationType.MissingOutputField -> Some("value")), found)
    assert(columns("d3") == List("id", "value"))
  }

  test("DROP COLUMN of a required column: MISSING_OUTPUT_FIELD; of an optional one, it passes") {
    deltaTable("d4")
    assert(run(contract("d4"))("ALTER TABLE d4 DROP COLUMN id") == List(ViolationType.MissingOutputField -> Some("id")))
    assert(columns("d4") == List("id", "value"))
    assert(run(contract("d4", valueRequired = false))("ALTER TABLE d4 DROP COLUMN value") == Nil)
    assert(columns("d4") == List("id"))
  }

  test("CHANGE COLUMN that leaves the columns as they are passes (parquet)") {
    parquetTable("p5")
    assert(run(contract("p5"), strict)("ALTER TABLE p5 CHANGE COLUMN id id BIGINT COMMENT 'the key'") == Nil)
  }

  test("ADD COLUMNS of a declared column with another type: OUTPUT_FIELD_TYPE_MISMATCH; ALTER COLUMN changing only a comment passes (Delta)") {
    deltaTable("d5")
    val yaml = contract("d5", extraFields = "        - { name: email, type: long }\n")
    assert(run(yaml)("ALTER TABLE d5 ADD COLUMNS (email STRING)") == List(ViolationType.OutputFieldTypeMismatch -> Some("email")))
    assert(columns("d5") == List("id", "value"))
    assert(run(yaml, strict)("ALTER TABLE d5 ALTER COLUMN id COMMENT 'the key'") == Nil, "a comment changes no column")
  }

  // -- Statements that move a declared output

  test("SET LOCATION of a declared output: UNVERIFIABLE_WRITE, and the table stays where it was") {
    parquetTable("p6")
    val before = location("p6")
    val e = refusal(contract("p6"))(s"ALTER TABLE p6 SET LOCATION '${dir.resolve("elsewhere")}'")
    assert(e.result.violations.map(_.violationType) == List(ViolationType.UnverifiableWrite))
    assert(e.getMessage.contains("repoints the table at a different location") && e.getMessage.contains("'out'"), e.getMessage)
    assert(location("p6") == before)
  }

  test("SET LOCATION of one partition of a declared output is refused too") {
    parquetTable("p7")
    val e = refusal(contract("p7"))(s"ALTER TABLE p7 PARTITION (dt='a') SET LOCATION '${dir.resolve("elsewhere_part")}'")
    assert(e.result.violations.map(_.violationType) == List(ViolationType.UnverifiableWrite))
    assert(e.getMessage.contains("repoints a partition at data no write produced"), e.getMessage)
  }

  test("RENAME TO of a declared output: UNVERIFIABLE_WRITE, and the table keeps its name") {
    parquetTable("p8")
    val e = refusal(contract("p8"))("ALTER TABLE p8 RENAME TO p8_renamed")
    assert(e.result.violations.map(_.violationType) == List(ViolationType.UnverifiableWrite))
    assert(e.getMessage.contains("renames the table away"), e.getMessage)
    assert(spark.catalog.tableExists("p8") && !spark.catalog.tableExists("p8_renamed"))
  }

  test("ADD PARTITION ... LOCATION of a declared output is refused; ADD PARTITION without a location passes") {
    parquetTable("p9")
    val e = refusal(contract("p9"))(s"ALTER TABLE p9 ADD PARTITION (dt='z') LOCATION '${dir.resolve("foreign")}'")
    assert(e.result.violations.map(_.violationType) == List(ViolationType.UnverifiableWrite))
    assert(e.getMessage.contains("attaches data no write produced"), e.getMessage)
    assert(run(contract("p9"))("ALTER TABLE p9 ADD PARTITION (dt='y')") == Nil)
    assert(spark.sql("SHOW PARTITIONS p9").collect().map(_.getString(0)).toSet == Set("dt=a", "dt=y"))
  }

  test("RENAME TO onto a declared output's name: the renamed table's columns are checked as the output's") {
    parquetTable("staging_extra", extraColumn = Some("email"))
    spark.sql("DROP TABLE IF EXISTS gold_out")
    assert(run(contract("gold_out"), strict)("ALTER TABLE staging_extra RENAME TO gold_out") == List(ViolationType.UndeclaredOutputColumn -> Some("email")))
    assert(spark.catalog.tableExists("staging_extra") && !spark.catalog.tableExists("gold_out"))
    parquetTable("staging_exact")
    assert(run(contract("gold_out"), strict)("ALTER TABLE staging_exact RENAME TO gold_out") == Nil)
    assert(spark.catalog.tableExists("gold_out"))
  }

  // -- A table the contract does not declare

  test("on a table the contract does not declare, every statement passes and takes effect") {
    parquetTable("u1")
    deltaTable("u2")
    val other = contract("somewhere_else")
    List(
      "ALTER TABLE u1 ADD COLUMNS (email STRING)",
      "ALTER TABLE u1 CHANGE COLUMN id id BIGINT COMMENT 'k'",
      "ALTER TABLE u1 RENAME TO u1_renamed",
      s"ALTER TABLE u1_renamed ADD PARTITION (dt='z') LOCATION '${dir.resolve("u1_z")}'",
      s"ALTER TABLE u1_renamed PARTITION (dt='z') SET LOCATION '${dir.resolve("u1_z2")}'",
      s"ALTER TABLE u1_renamed SET LOCATION '${dir.resolve("u1_moved")}'",
      "ALTER TABLE u2 ADD COLUMNS (email STRING)",
      "ALTER TABLE u2 RENAME COLUMN value TO amount",
      "ALTER TABLE u2 DROP COLUMN id"
    ).foreach(sql => assert(run(other, strict)(sql) == Nil, sql))
    assert(spark.catalog.tableExists("u1_renamed") && columns("u1_renamed").contains("email"))
    assert(location("u1_renamed").endsWith("u1_moved"))
    assert(columns("u2") == List("amount", "email"))
  }

  // -- What is published

  test("a checked statement publishes one validation: FAILED or PASSED on a declared output, PASSED on any other table") {
    parquetTable("e1")
    parquetTable("e2")
    val sink = new TestNotificationSink
    withSink(sink) {
      run(contract("e1"), strict)("ALTER TABLE e1 ADD COLUMNS (email STRING)")
      run(contract("e1"))("ALTER TABLE e1 ADD COLUMNS (email STRING)")
      run(contract("e1"))("ALTER TABLE e2 ADD COLUMNS (email STRING)")
    }
    val validations = sink.events.collect { case v: ContractValidationEvent => v }
    assert(validations.map(_.status) == List("FAILED", "PASSED", "PASSED"))
    assert(validations.map(_.violations.size) == List(1, 0, 0))
  }

  test("moving an undeclared table publishes nothing; renaming one publishes the PASSED check of the table under its new name") {
    parquetTable("e3")
    val sink = new TestNotificationSink
    withSink(sink)(assert(run(contract("somewhere_else"))(s"ALTER TABLE e3 SET LOCATION '${dir.resolve("e3_moved")}'") == Nil))
    assert(sink.events.isEmpty)
    parquetTable("e4")
    withSink(sink)(assert(run(contract("somewhere_else"))("ALTER TABLE e4 RENAME TO e4_renamed") == Nil))
    assert(sink.events.collect { case v: ContractValidationEvent => v.status } == List("PASSED"))
  }

  /** What `extract` makes of `sql`'s analyzed plan; the statement is analyzed, not run. */
  private def extracted(sql: String): List[SchemaDdlSupport.SchemaDdl] =
    List(spark.sessionState.executePlan(spark.sessionState.sqlParser.parsePlan(sql)).analyzed).flatMap(SchemaDdlSupport.extract)

  test("extract: each statement, the table it names, and the columns it leaves") {
    parquetTable("x1")
    deltaTable("x2")
    val List(added: SchemaDdlSupport.NewSchema) = extracted("ALTER TABLE x1 ADD COLUMNS (email STRING)")
    assert(added.statement == "ALTER TABLE ... ADD COLUMNS")
    assert(added.schema.fieldNames.toList == List("id", "value", "email", "dt"), "added before the partition column")
    assert(added.target.names == List(byName("x1")) && added.target.location.endsWith("/warehouse/x1"))
    val List(renamed: SchemaDdlSupport.NewSchema) = extracted("ALTER TABLE x2 RENAME COLUMN value TO amount")
    assert(renamed.statement == "ALTER TABLE ... RENAME COLUMN" && renamed.schema.fieldNames.toList == List("id", "amount"))
    assert(renamed.target.names == List(byName("x2")) && renamed.target.location.endsWith("/warehouse/x2"))
    val List(dropped: SchemaDdlSupport.NewSchema) = extracted("ALTER TABLE x2 DROP COLUMN id")
    assert(dropped.statement == "ALTER TABLE ... DROP COLUMN" && dropped.schema.fieldNames.toList == List("value"))
    val List(replaced: SchemaDdlSupport.NewSchema) = extracted("ALTER TABLE x2 REPLACE COLUMNS (id BIGINT)")
    assert(replaced.statement == "ALTER TABLE ... REPLACE COLUMNS" && replaced.schema.fieldNames.toList == List("id"))
    val List(altered: SchemaDdlSupport.NewSchema) = extracted("ALTER TABLE x2 ALTER COLUMN value FIRST")
    assert(altered.statement == "ALTER TABLE ... ALTER COLUMN" && altered.schema.fieldNames.toList == List("value", "id"))
    val List(changed: SchemaDdlSupport.NewSchema) = extracted("ALTER TABLE x1 CHANGE COLUMN VALUE value BIGINT COMMENT 'v'")
    assert(changed.statement == "ALTER TABLE ... CHANGE COLUMN" && changed.schema.fieldNames.toList == List("id", "value", "dt"))
    assert(changed.schema("value").getComment().contains("v"), "the named column is replaced, whatever its case")
    assert(changed.schema("id").getComment().isEmpty)
    val List(moved: SchemaDdlSupport.Displaced) = extracted(s"ALTER TABLE x1 SET LOCATION '${dir.resolve("x1_moved")}'")
    assert(moved.statement == "ALTER TABLE ... SET LOCATION" && moved.target.names == List(byName("x1")) && moved.problem == SchemaDdlSupport.RepointsTable)
    val List(movedPart: SchemaDdlSupport.Displaced) = extracted(s"ALTER TABLE x1 PARTITION (dt='a') SET LOCATION '${dir.resolve("x1_a")}'")
    assert(movedPart.problem == SchemaDdlSupport.RepointsPartition)
    val List(attached: SchemaDdlSupport.Displaced) = extracted(s"ALTER TABLE x1 ADD PARTITION (dt='q') PARTITION (dt='r') LOCATION '${dir.resolve("x1_r")}'")
    assert(attached.statement == "ALTER TABLE ... ADD PARTITION ... LOCATION" && attached.problem == SchemaDdlSupport.AttachesData)
    val List(rename: SchemaDdlSupport.RenamedOnto) = extracted("ALTER TABLE x1 RENAME TO x1_new")
    assert(rename.statement == "ALTER TABLE ... RENAME TO")
    assert(rename.source.names == List(byName("x1")) && rename.target.names == List(byName("x1_new")))
    assert(rename.source.location.endsWith("/warehouse/x1") && rename.target.location.endsWith("/warehouse/x1_new"), rename.target.location)
    assert(rename.schema.fieldNames.toList == List("id", "value", "dt"))
    assert(extracted("ALTER TABLE x1 ADD PARTITION (dt='q')") == Nil, "a partition with no location attaches nothing")
    assert(extracted("ALTER TABLE x2 SET TBLPROPERTIES ('k' = 'v')") == Nil, "a property changes no column")
    spark.sql("CREATE OR REPLACE VIEW xv AS SELECT id FROM x2")
    assert(extracted("ALTER VIEW xv RENAME TO xv2") == Nil, "a view holds no data")
    assert(extracted("SELECT id FROM x2") == Nil)
  }

  test("applySchemaChanges: Spark's own result, or None when Spark cannot work the change out") {
    import org.apache.spark.sql.connector.catalog.TableChange
    import org.apache.spark.sql.types.{LongType, StringType, StructType}
    val schema = new StructType().add("id", LongType).add("s", new StructType().add("a", LongType))
    val added = SchemaDdlSupport.applySchemaChanges(schema, Seq(TableChange.addColumn(Array("s", "b"), StringType)), None)
    assert(added.map(_.catalogString).contains("struct<id:bigint,s:struct<a:bigint,b:string>>"))
    assert(SchemaDdlSupport.applySchemaChanges(schema, Seq(TableChange.deleteColumn(Array("missing"), false)), None).isEmpty)
  }

  test("a column change Invaract cannot work out is refused on a declared output, and let through on any other table") {
    val target = TableTargets.Target(dir.resolve("warehouse").resolve("w1").toString, List(byName("w1")))
    val refused = SchemaDdlSupport.refusal(parseContract(contract("w1")), "ALTER TABLE ... ADD COLUMNS", target, SchemaDdlSupport.Unworkable)
    assert(refused.map(_.problem).contains("ALTER TABLE ... ADD COLUMNS changes the table's columns in a way Invaract could not work out, and 'out' is an output this contract declares"))
    assert(refused.exists(_.remediation.startsWith("Write the output with a query Invaract can check")))
    assert(SchemaDdlSupport.refusal(parseContract(contract("elsewhere")), "ALTER TABLE ... ADD COLUMNS", target, SchemaDdlSupport.Unworkable).isEmpty)
  }
}
