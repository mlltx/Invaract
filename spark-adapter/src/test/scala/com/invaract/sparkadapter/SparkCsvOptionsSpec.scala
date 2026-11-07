// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import com.invaract.contract.CsvOptions
import com.invaract.verification.{ContractViolationException, ViolationType}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{LongType, StructField, StructType}

import java.nio.file.{Files, Path}

/** `check.csvOptions` on a real Spark session: the options of each CSV read and write, Spark's defaults standing for
  * those the job leaves unset, against the contract's `csv:` options (docs/SPARK_ADAPTER.md, "CSV options"). */
class SparkCsvOptionsSpec extends ConnectorSpecBase {
  private var scratchDir: Path = _

  override def beforeAll(): Unit = {
    scratchDir = Files.createTempDirectory("invaract-csv-options")
    spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("SparkCsvOptionsSpec")
      .config("spark.sql.warehouse.dir", scratchDir.resolve("warehouse").toString)
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.ui.enabled", "false")
      .withExtensions(injectContractCheck)
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
  }

  private def at(name: String): String = scratchDir.resolve(name).toString

  private def output(location: String, csv: String): String =
    s"""id: csv_options
       |version: "1.0.0"
       |outputs:
       |  - name: report
       |    location: $location
       |    format: csv
       |    csv: $csv
       |    schema:
       |      fields:
       |        - { name: id, type: long }
       |        - { name: value, type: long }
       |""".stripMargin

  private def withInput(location: String, csv: String, out: String): String =
    s"""id: csv_options
       |version: "1.0.0"
       |inputs:
       |  - name: orders
       |    location: $location
       |    format: csv
       |    csv: $csv
       |    schema:
       |      fields:
       |        - { name: id, type: long }
       |        - { name: value, type: long }
       |outputs:
       |  - name: report
       |    location: $out
       |    schema:
       |      fields:
       |        - { name: id, type: long }
       |        - { name: value, type: long }
       |""".stripMargin

  private val schema = StructType(Seq(StructField("id", LongType), StructField("value", LongType)))

  private def refused(body: => Unit): ContractViolationException = intercept[ContractViolationException](body)

  // --- writes ------------------------------------------------------------------------------------

  test("a CSV write with the declared options passes; with another delimiter it is refused, naming both") {
    val out = at("write_options")
    withContract(output(out, """{ delimiter: ";", header: true }""")) {
      df().write.mode("overwrite").option("sep", ";").option("header", "true").csv(out)
      val ex = refused(df().write.mode("overwrite").option("delimiter", "|").option("header", "true").csv(out))
      val List(v) = ex.result.violations
      assert(v.violationType == ViolationType.OutputCsvOptionsMismatch)
      assert(v.expected.contains("delimiter ';'") && v.actual.contains("delimiter '|'"))
    }
  }

  test("an option the job leaves unset is Spark's default: no header line, comma, UTF-8") {
    val out = at("write_defaults")
    // the docs-site guide's example (guides/csv-options.mdx, "What Spark jobs are held to")
    withContract(output(out, """{ delimiter: ";", header: true }""")) {
      val List(v) = refused(df().write.mode("overwrite").option("sep", ";").csv(out)).result.violations
      assert(v.actual.contains("header false"))
      assert(v.message == "contract declares the output CSV options header true but the write uses header false")
    }
    withContract(output(out, """{ header: false, delimiter: ",", encoding: utf8, quote: "\"", escape: "\\", nullValue: "" }""")) {
      df().write.mode("overwrite").csv(out) // every one Spark's default, by any spelling of it
    }
  }

  test("charset is Spark's other name for encoding, and the values are compared by meaning") {
    val out = at("write_charset")
    withContract(output(out, "{ encoding: ISO-8859-1 }")) {
      df().write.mode("overwrite").option("charset", "latin1").csv(out)
      assert(refused(df().write.mode("overwrite").option("charset", "UTF-16").csv(out)).result.violations.size == 1)
    }
  }

  test("an insert into an existing USING csv table and a CREATE TABLE AS SELECT are checked with their table's options") {
    spark.sql(s"CREATE TABLE csv_opts_tbl (id BIGINT, value BIGINT) USING csv OPTIONS (sep '|') LOCATION '${at("csv_opts_tbl")}'")
    val tableLocation = spark.sql("DESCRIBE TABLE EXTENDED csv_opts_tbl").where("col_name = 'Location'").head().getString(1)
    withContract(output(tableLocation, """{ delimiter: ";" }""")) {
      val ex = refused(df().write.insertInto("csv_opts_tbl"))
      assert(ex.result.violations.flatMap(_.actual) == List("delimiter '|'"))
    }
    val ctas = at("csv_opts_ctas")
    withContract(output(ctas, """{ delimiter: ";" }""")) {
      df().createOrReplaceTempView("csv_opts_src")
      val ex = refused(spark.sql(s"CREATE TABLE csv_opts_ctas USING csv OPTIONS (sep '|') LOCATION '$ctas' AS SELECT * FROM csv_opts_src"))
      assert(ex.result.violations.map(_.violationType) == List(ViolationType.OutputCsvOptionsMismatch))
    }
  }

  test("a write in another format has no CSV options to compare; a write elsewhere is held to the one declared output") {
    val out = at("write_parquet")
    withContract(output(out, """{ delimiter: ";" }""").replace("format: csv", "format: parquet")) {
      df().write.mode("overwrite").parquet(out)
    }
    // the one output a contract declares is what any write is checked against, its options as its format and save mode
    withContract(output(at("declared"), """{ delimiter: ";" }""")) {
      val types = refused(df().write.mode("overwrite").csv(at("elsewhere"))).result.violations.map(_.violationType)
      assert(types == List(ViolationType.OutputLocationMismatch, ViolationType.OutputCsvOptionsMismatch))
    }
  }

  // --- reads -------------------------------------------------------------------------------------

  test("a CSV read with other options than the input declares is refused; with the declared ones it passes") {
    val in = at("read_orders")
    df().write.mode("overwrite").option("sep", ";").csv(in)
    val out = at("read_report")
    withContract(withInput(in, """{ delimiter: ";", header: false }""", out)) {
      spark.read.option("sep", ";").schema(schema).csv(in).write.mode("overwrite").parquet(out)
      val ex = refused(spark.read.option("sep", ";").option("header", "true").schema(schema).csv(in).write.mode("overwrite").parquet(out))
      val List(v) = ex.result.violations
      assert(v.violationType == ViolationType.InputCsvOptionsMismatch && v.actual.contains("header true"))
    }
  }

  test("a streaming CSV source is checked with its options") {
    val in = at("stream_orders")
    df().write.mode("overwrite").option("sep", ";").csv(in)
    val out = at("stream_report")
    withContract(withInput(in, """{ delimiter: ";" }""", out)) {
      val ex = refused {
        spark.readStream.schema(schema).option("sep", ",").csv(in).writeStream.format("parquet")
          .option("checkpointLocation", at("stream_checkpoint")).start(out).awaitTermination(10000)
      }
      assert(ex.result.violations.map(_.violationType) == List(ViolationType.InputCsvOptionsMismatch))
    }
  }

  // --- the options themselves --------------------------------------------------------------------

  test("Spark's spellings map onto the contract's names, the one Spark reads first winning") {
    assert(CsvOptionsSupport.explicit(Map("SEP" -> ";", "delimiter" -> "|", "Charset" -> "latin1", "path" -> "/x")) ==
      Map("delimiter" -> ";", "encoding" -> "latin1"))
    assert(CsvOptionsSupport.explicit(Map("encoding" -> "UTF-16", "charset" -> "latin1", "nullValue" -> "NA", "dateFormat" -> "d", "timestampFormat" -> "t",
      "quote" -> "'", "escape" -> "#", "header" -> "TRUE")) ==
      Map("encoding" -> "UTF-16", "nullValue" -> "NA", "dateFormat" -> "d", "timestampFormat" -> "t", "quote" -> "'", "escape" -> "#", "header" -> "TRUE"))
    assert(CsvOptionsSupport.explicit(Map.empty).isEmpty)
    assert(CsvOptionsSupport.effective(Map("sep" -> ";")) == CsvOptionsSupport.SparkDefaults + ("delimiter" -> ";"))
    assert(CsvOptionsSupport.SparkDefaults.keySet == CsvOptions.Names.toSet)
    assert(CsvOptionsSupport.drafted(Map("sep" -> ";", "mode" -> "FAILFAST")) == Map("delimiter" -> ";", "header" -> "false"))
    assert(CsvOptionsSupport.drafted(Map("header" -> "true")) == Map("header" -> "true"))
  }

  test("a dry run's draft declares the options the job set, and header") {
    val out = at("dry_run")
    df().write.mode("overwrite").option("sep", ";").csv(at("dry_in"))
    var drafts = List.empty[com.invaract.contract.Contract]
    capturedPlans.clear()
    spark.read.option("sep", ";").schema(schema).csv(at("dry_in")).write.mode("overwrite").option("sep", "|").option("header", "true").csv(out)
    val analyzed = capturedPlans.find(p => WriteCommandSupport.combined.isDefinedAt(p)).get
    ContractEnforcementRule.inferOrIgnore(analyzed, c => drafts ::= c)
    val List(draft) = drafts
    assert(draft.outputs.head.csv.contains(CsvOptions(delimiter = Some("|"), header = Some(true))))
    assert(draft.inputs.head.csv.contains(CsvOptions(delimiter = Some(";"), header = Some(false))))
  }
}
