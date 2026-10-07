// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.ir

import org.scalatest.funsuite.AnyFunSuite

class FunctionCatalogSpec extends AnyFunSuite {

  test("every catalog entry is named in canonical upper case, uniquely, with a description") {
    val names = FunctionCatalog.all.map(_.name)
    assert(names == names.map(_.toUpperCase))
    assert(names.distinct == names)
    assert(FunctionCatalog.all.forall(_.description.trim.nonEmpty))
  }

  test("the catalog is exactly the functions known to differ between runs") {
    assert(FunctionCatalog.all.map(_.name).toSet == Set(
      "UUID", "RAND", "RANDN", "CURRENT_TIMESTAMP", "CURRENT_DATE", "UNIX_TIMESTAMP", "ROW_ID", "SOURCE_FILE_NAME", "PARTITION_ID", "SOURCE_BLOCK_START", "SOURCE_BLOCK_LENGTH", "SHUFFLE"
    ))
    assert(FunctionCatalog.all.forall(_.nonDeterministic))
    assert(FunctionCatalog.nonDeterministicNames == FunctionCatalog.all.map(_.name).toSet)
  }

  test("lookup is case-insensitive and returns the canonical entry") {
    assert(FunctionCatalog.lookup("uuid").contains(FunctionCatalog.Uuid))
    assert(FunctionCatalog.lookup("Uuid").contains(FunctionCatalog.Uuid))
    assert(FunctionCatalog.lookup("UUID").contains(FunctionCatalog.Uuid))
    assert(FunctionCatalog.lookup("not_a_function").isEmpty)
    assert(FunctionCatalog.lookup("").isEmpty)
  }

  test("isNonDeterministic: true for every catalog function in any case, false for anything else") {
    FunctionCatalog.all.foreach { f =>
      assert(FunctionCatalog.isNonDeterministic(f.name))
      assert(FunctionCatalog.isNonDeterministic(f.name.toLowerCase))
    }
    List("SUM", "COALESCE", "UPPER", "ISNOTNULL", "", "GENERATE_UUID").foreach(n => assert(!FunctionCatalog.isNonDeterministic(n), n))
  }

  test("isSeedBearing: exactly RAND and RANDN, in any case") {
    assert(FunctionCatalog.all.filter(_.seedBearing).map(_.name).toSet == Set("RAND", "RANDN"))
    assert(FunctionCatalog.isSeedBearing("rand") && FunctionCatalog.isSeedBearing("RANDN") && FunctionCatalog.isSeedBearing("Randn"))
    List("UUID", "CURRENT_TIMESTAMP", "RANDOM", "", "SUM").foreach(n => assert(!FunctionCatalog.isSeedBearing(n), n))
  }

  // --- aliases ---------------------------------------------------------------------------------

  /** What a BigQuery adapter would declare: `GENERATE_UUID()` is the catalog's UUID. */
  private val bigQuery = FunctionAliases("bigquery", Map("GENERATE_UUID" -> "UUID", "RAND" -> "RAND", "SESSION_ROW_NUMBER" -> "ROW_ID"))

  test("an alias maps an engine's own name onto the canonical one, whatever the case") {
    assert(bigQuery.canonicalName("GENERATE_UUID") == "UUID")
    assert(bigQuery.canonicalName("generate_uuid") == "UUID")
    assert(bigQuery.canonicalName("Generate_Uuid") == "UUID")
    assert(bigQuery.canonicalName("session_row_number") == "ROW_ID")
  }

  test("a name with no alias passes through, upper-cased - never guessed at") {
    assert(bigQuery.canonicalName("sum") == "SUM")
    assert(bigQuery.canonicalName("Some_Engine_Fn") == "SOME_ENGINE_FN")
    assert(bigQuery.canonicalName("") == "")
  }

  test("the same logic on two engines gets the same canonical, non-deterministic function") {
    val spark = FunctionAliases("spark", Map("uuid" -> "UUID"))
    val a = spark.canonicalName("uuid")
    val b = bigQuery.canonicalName("GENERATE_UUID")
    assert(a == b)
    assert(FunctionCatalog.isNonDeterministic(a))
    // ...and without the alias the engine's own name would have been silently classified deterministic.
    assert(!FunctionCatalog.isNonDeterministic("GENERATE_UUID"))
  }

  test("an alias whose target is not a canonical name is rejected, naming the engine, the alias and the target") {
    val e = intercept[IllegalArgumentException](FunctionAliases("bigquery", Map("GENERATE_UUID" -> "UUIDD")))
    assert(e.getMessage.contains("bigquery") && e.getMessage.contains("GENERATE_UUID") && e.getMessage.contains("UUIDD"))
  }

  test("an alias target must be spelled canonically (upper case), not just match case-insensitively") {
    intercept[IllegalArgumentException](FunctionAliases("bigquery", Map("GENERATE_UUID" -> "uuid")))
  }

  test("an engine whose names already are the catalog's needs no aliases") {
    val id = FunctionAliases.identity("reference")
    assert(id.engine == "reference" && id.aliases.isEmpty)
    assert(id.canonicalName("uuid") == "UUID")
  }
}
