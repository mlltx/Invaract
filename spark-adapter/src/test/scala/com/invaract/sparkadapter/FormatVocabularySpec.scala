// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import com.invaract.contract.{CatalogTechnologies, Formats}
import org.apache.spark.sql.connector.catalog.CatalogPlugin
import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat
import org.apache.spark.sql.execution.datasources.json.JsonFileFormat
import org.apache.spark.sql.execution.datasources.orc.OrcFileFormat
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.text.TextFileFormat
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalatest.funsuite.AnyFunSuite

/** Spark's format and catalog-technology names land inside the canonical vocabularies
  * (`com.invaract.contract.Formats` / `CatalogTechnologies`) - the rule every adapter follows so
  * one contract means the same thing on every engine (docs/MULTI_ENGINE_ADAPTERS.md,
  * "Conventions every adapter follows").
  */
class FormatVocabularySpec extends AnyFunSuite {

  test("the format names Spark's built-in file sources register are canonical, and map one-to-one") {
    val reported = Map(
      Formats.Parquet -> SparkPlanAdapter.formatOf(new ParquetFileFormat),
      Formats.Csv -> SparkPlanAdapter.formatOf(new CSVFileFormat),
      Formats.Json -> SparkPlanAdapter.formatOf(new JsonFileFormat),
      Formats.Orc -> SparkPlanAdapter.formatOf(new OrcFileFormat),
      Formats.Text -> SparkPlanAdapter.formatOf(new TextFileFormat)
    )
    reported.foreach { case (canonical, actual) => assert(actual.contains(canonical), s"expected $canonical, got $actual") }
  }

  test("a provider that is not a registered data source reports no format rather than a guess") {
    assert(SparkPlanAdapter.formatOf(new Object).isEmpty)
  }

  private def plugin(simpleName: String): CatalogPlugin = simpleName match {
    case "DeltaCatalog"        => new DeltaCatalog
    case "SparkCatalog"        => new SparkCatalog
    case "SparkSessionCatalog" => new SparkSessionCatalog
    case "JDBCTableCatalog"    => new JDBCTableCatalog
    case _                     => new SomeOtherCatalog
  }

  test("the catalog plugins Invaract knows map to canonical technologies") {
    Map(
      "DeltaCatalog" -> CatalogTechnologies.Delta,
      "SparkCatalog" -> CatalogTechnologies.Iceberg,
      "SparkSessionCatalog" -> CatalogTechnologies.Iceberg,
      "JDBCTableCatalog" -> CatalogTechnologies.Jdbc
    ).foreach { case (cls, canonical) =>
      val technology = CatalogIdentitySupport.technologyOfCatalogPlugin(plugin(cls))
      assert(technology == canonical, cls)
      assert(CatalogTechnologies.isCanonical(technology), cls)
    }
  }

  test("an unknown catalog plugin passes through under its own class name (the vocabulary is open)") {
    assert(CatalogIdentitySupport.technologyOfCatalogPlugin(plugin("SomeOtherCatalog")) == "SomeOtherCatalog")
    assert(!CatalogTechnologies.isCanonical("SomeOtherCatalog"))
  }
}

// Stand-ins named like the real plugins: the mapping keys on the class's simple name, so the real
// Delta/Iceberg/JDBC jars need not be on this suite's classpath.
private class FakeCatalog extends CatalogPlugin {
  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = ()
  override def name(): String = "fake"
}
private class DeltaCatalog extends FakeCatalog
private class SparkCatalog extends FakeCatalog
private class SparkSessionCatalog extends FakeCatalog
private class JDBCTableCatalog extends FakeCatalog
private class SomeOtherCatalog extends FakeCatalog
