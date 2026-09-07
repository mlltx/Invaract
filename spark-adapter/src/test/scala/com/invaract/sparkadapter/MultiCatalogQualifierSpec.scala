// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.{Files, Path}

/** Regression coverage for the multi-catalog / bare-relation-leaf gap found
  * during the fingerprinting audit's "multi-catalog qualifier truncation"
  * investigation (docs/SEMANTIC_LINEAGE_FINGERPRINTING.md §11's
  * "AttributeReference.qualifier.lastOption's truncation" bullet, and the
  * Implementation notes' "Gap-closing pass").
  *
  * Two independent, real Iceberg `hadoop`-type catalogs (`cat_a`/`cat_b`,
  * each its own warehouse directory - no shared state, no network
  * dependency, following `IcebergConnectorSpec`'s own dual-catalog
  * precedent), each holding a `sales.orders` table with the identical
  * schema and column names - the concrete multi-catalog scenario the audit
  * was asked to investigate (`catalog_a.sales.orders` vs.
  * `catalog_b.sales.orders`).
  *
  * The originally-hypothesized mechanism (`AttributeReference.qualifier`
  * carrying a real multi-part qualifier that then gets truncated to its
  * last segment) is confirmed, by this suite's own first test, NOT to be
  * what happens for the one Spark API path that bypasses
  * `computeAliasDisambiguation`'s `SubqueryAlias`-based coverage: a bare
  * `spark.read.format(...).load(tableIdentifier)` read produces an
  * `AttributeReference` with a completely EMPTY `.qualifier` (`Seq()`),
  * not a truncated one - every reference that *does* carry a real
  * multi-part qualifier (`spark.table(...)`/SQL `FROM`) is already wrapped
  * in a `SubqueryAlias` and therefore already covered. The real,
  * confirmed bug is broader and more severe than the original hypothesis:
  * two GENUINELY DIFFERENT physical tables (any two, not only same-final-
  * segment-name ones), both read via `.load()` and referenced in the same
  * plan, produced `ColumnRef`s with no qualifier at all to distinguish
  * them - collapsing the `overall` and per-output fingerprints of two
  * transformations that select different physical columns down to the
  * exact same bytes. See `SparkPlanAdapter.computeAliasDisambiguation`'s
  * own "Bare relation leaves" doc for the fix.
  */
class MultiCatalogQualifierSpec extends AnyFunSuite with BeforeAndAfterAll {
  private var spark: SparkSession = _
  private var scratchDir: Path = _

  override def beforeAll(): Unit = {
    scratchDir = Files.createTempDirectory("invaract-multicatalog-test")
    spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("MultiCatalogQualifierSpec")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.cat_a", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.cat_a.type", "hadoop")
      .config("spark.sql.catalog.cat_a.warehouse", scratchDir.resolve("wh_a").toString)
      .config("spark.sql.catalog.cat_b", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.cat_b.type", "hadoop")
      .config("spark.sql.catalog.cat_b.warehouse", scratchDir.resolve("wh_b").toString)
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    spark.sql("CREATE TABLE cat_a.sales.orders (id BIGINT, amount BIGINT) USING iceberg")
    spark.sql("CREATE TABLE cat_b.sales.orders (id BIGINT, amount BIGINT) USING iceberg")
    spark.sql("INSERT INTO cat_a.sales.orders VALUES (1, 100)")
    spark.sql("INSERT INTO cat_b.sales.orders VALUES (1, 999)")
  }

  override def afterAll(): Unit = spark.stop()

  private def locationOfCatalog(catalog: String): String =
    scratchDir.resolve(s"wh_$catalog").resolve("sales").resolve("orders").toString

  test("a bare .load() read of a catalog table has NO Spark-assigned qualifier (refutes the original truncation hypothesis)") {
    val df = spark.read.format("iceberg").load("cat_a.sales.orders")
    val analyzed = df.queryExecution.analyzed
    // Confirms the mechanism directly, rather than assuming it: a
    // multi-part qualifier (`ArraySeq(cat_a, sales, orders)`) is what
    // `spark.table(...)` produces (see the next test) - a bare `.load()`
    // produces no qualifier at all, not a truncated one.
    analyzed.output.foreach { attr =>
      assert(attr.qualifier.isEmpty, s"expected no Spark-assigned qualifier on a bare .load() attribute, got ${attr.qualifier}")
    }
  }

  test("spark.table() DOES carry a real multi-part qualifier, but it's irrelevant: SubqueryAlias coverage shadows it entirely") {
    val a = spark.table("cat_a.sales.orders")
    val b = spark.table("cat_b.sales.orders")
    assert(a.queryExecution.analyzed.output.forall(_.qualifier == Seq("cat_a", "sales", "orders")))
    assert(b.queryExecution.analyzed.output.forall(_.qualifier == Seq("cat_b", "sales", "orders")))

    // Both tables share the same final segment name ("orders"), the exact
    // scenario the original hypothesis worried about - but since both
    // reads pass through a SubqueryAlias, computeAliasDisambiguation's
    // first pass (keyed by `identifier.name`, unchanged by this fix)
    // already disambiguates them by encounter order, exactly as it
    // already does for a same-table self-join. No collision reaches
    // qualifier.lastOption at all.
    val joined = a.join(b, a("id") === b("id"))
    val translated = SparkPlanAdapter.translate(joined.queryExecution.analyzed).plan
    val join = translated.asInstanceOf[com.invaract.ir.Join]
    val leftRead = join.left.asInstanceOf[com.invaract.ir.Read]
    val rightRead = join.right.asInstanceOf[com.invaract.ir.Read]
    assert(leftRead.alias == Some("orders#0"))
    assert(rightRead.alias == Some("orders#1"))
  }

  test("a bare .load() read of a UNIQUE table keeps Read.alias == None, and its ColumnRef.qualifier is its own physical location") {
    val df = spark.read.format("iceberg").load("cat_a.sales.orders")
    val translated = SparkPlanAdapter.translate(df.queryExecution.analyzed).plan
    val read = translated.asInstanceOf[com.invaract.ir.Read]
    // Byte-for-byte backward compatible for the ordinary, non-colliding
    // case: alias stays unset, exactly as before this fix.
    assert(read.alias == None)
    assert(read.dataset.location == locationOfCatalog("a"))

    val selected = df.select(df("amount").as("result"))
    val translatedSelect = SparkPlanAdapter.translate(selected.queryExecution.analyzed).plan
    translatedSelect match {
      case com.invaract.ir.Project(_, List(com.invaract.ir.NamedExpr("result", com.invaract.ir.ColumnReference(com.invaract.ir.ColumnRef("amount", qualifier, _))))) =>
        assert(qualifier == Some(locationOfCatalog("a")), s"expected the column's qualifier to be its own Read's physical location, got $qualifier")
      case other => fail(s"unexpected shape: $other")
    }
  }

  test("two DIFFERENT catalogs' same-named tables joined via .load(): selecting one side's column vs. the other's must fingerprint DIFFERENTLY") {
    val a = spark.read.format("iceberg").load("cat_a.sales.orders")
    val b = spark.read.format("iceberg").load("cat_b.sales.orders")
    val joined = a.join(b, a("id") === b("id"))

    val selectA: DataFrame = joined.select(a("amount").as("result"))
    val selectB: DataFrame = joined.select(b("amount").as("result"))

    val planA = SparkPlanAdapter.translate(selectA.queryExecution.analyzed).plan
    val planB = SparkPlanAdapter.translate(selectB.queryExecution.analyzed).plan

    // Exact-value assertions on the qualifiers themselves first - these
    // pin the actual mechanism (matching each Read's own physical
    // location), not merely "the two differ".
    def resultQualifier(plan: com.invaract.ir.Plan): Option[String] = plan match {
      case com.invaract.ir.Project(_, List(com.invaract.ir.NamedExpr("result", com.invaract.ir.ColumnReference(com.invaract.ir.ColumnRef("amount", qualifier, _))))) => qualifier
      case other => fail(s"unexpected shape: $other")
    }
    assert(resultQualifier(planA) == Some(locationOfCatalog("a")))
    assert(resultQualifier(planB) == Some(locationOfCatalog("b")))

    val fpA = com.invaract.fingerprint.TransformationFingerprinter.fingerprint(planA)
    val fpB = com.invaract.fingerprint.TransformationFingerprinter.fingerprint(planB)

    // Before the fix, ALL FOUR of these were byte-identical, despite
    // selectA/selectB being genuinely different transformations (one
    // reads cat_a's amount, the other cat_b's) - the confirmed false
    // negative this test guards against.
    assert(fpA.outputs("result").expression != fpB.outputs("result").expression,
      "selecting cat_a's amount vs. cat_b's amount must produce different expression fingerprints")
    assert(fpA.outputs("result").lineage != fpB.outputs("result").lineage,
      "selecting cat_a's amount vs. cat_b's amount must produce different lineage fingerprints (different resolved source)")
    assert(fpA.outputs("result").combined != fpB.outputs("result").combined)
    assert(fpA.overall != fpB.overall)
  }

  test("a bare self-join via .load() of the SAME catalog table (no SubqueryAlias at all) still translates the two occurrences distinctly") {
    val l = spark.read.format("iceberg").load("cat_a.sales.orders")
    val r = spark.read.format("iceberg").load("cat_a.sales.orders")
    val joined = l.join(r, l("id") === r("id"))

    val translated = SparkPlanAdapter.translate(joined.queryExecution.analyzed).plan
    val join = translated.asInstanceOf[com.invaract.ir.Join]
    val leftRead = join.left.asInstanceOf[com.invaract.ir.Read]
    val rightRead = join.right.asInstanceOf[com.invaract.ir.Read]
    val loc = locationOfCatalog("a")
    // Exact suffixes, in encounter order - same discipline as the
    // SubqueryAlias-based self-join fix's own test (pins the index
    // arithmetic, not merely that the two differ).
    assert(leftRead.alias == Some(s"$loc#0"), s"expected '$loc#0', got ${leftRead.alias}")
    assert(rightRead.alias == Some(s"$loc#1"), s"expected '$loc#1', got ${rightRead.alias}")

    val selectL = joined.select(l("amount").as("result"))
    val selectR = joined.select(r("amount").as("result"))
    val planL = SparkPlanAdapter.translate(selectL.queryExecution.analyzed).plan
    val planR = SparkPlanAdapter.translate(selectR.queryExecution.analyzed).plan
    val fpL = com.invaract.fingerprint.TransformationFingerprinter.fingerprint(planL)
    val fpR = com.invaract.fingerprint.TransformationFingerprinter.fingerprint(planR)
    assert(fpL.outputs("result").expression != fpR.outputs("result").expression,
      "left vs. right occurrence of the same self-joined table must produce different expression fingerprints")
    assert(fpL.outputs("result").lineage != fpR.outputs("result").lineage)
  }

  test("an ordinary single .load() read (no collision) is byte-for-byte unaffected by this fix") {
    val df = spark.read.format("iceberg").load("cat_a.sales.orders")
    val fp1 = com.invaract.fingerprint.TransformationFingerprinter.fingerprint(
      SparkPlanAdapter.translateAsWrite(df.queryExecution.analyzed, com.invaract.ir.DatasetRef("out")).plan
    )
    val fp2 = com.invaract.fingerprint.TransformationFingerprinter.fingerprint(
      SparkPlanAdapter.translateAsWrite(spark.read.format("iceberg").load("cat_a.sales.orders").queryExecution.analyzed, com.invaract.ir.DatasetRef("out")).plan
    )
    assert(fp1.overall == fp2.overall, "re-reading the identical unique table twice, unrelated to any collision, must still fingerprint identically")
  }
}
