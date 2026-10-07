// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import com.invaract.ir.FunctionAliases

/** Spark's own function names, mapped onto `ir.FunctionCatalog`'s canonical vocabulary
  * (docs/MULTI_ENGINE_ADAPTERS.md, Stage 5).
  *
  * Catalyst reports a different `prettyName` for the same expression class depending on which SQL
  * alias the query used (`random()` and `rand()` are both `Rand`, but report "random" and "rand"), so
  * the name an IR `Function` carries would otherwise depend on spelling, and a list of "Spark's
  * non-deterministic names" would have to enumerate every alias of each. Folding them onto the
  * catalog once, here, means everything downstream - non-determinism, the fingerprint's seed
  * handling - asks the catalog and never has to know Spark's spellings.
  *
  * Only a name that differs from its catalog name needs an entry; the rest (`uuid`, `rand`, ...)
  * are already the catalog's after upper-casing. `SparkFunctionAliasesSpec` sweeps Spark's own
  * function registry to prove no non-deterministic function it has escapes the catalog.
  */
object SparkFunctionAliases {

  val aliases: FunctionAliases = FunctionAliases(
    "spark",
    Map(
      "random" -> "RAND",
      "now" -> "CURRENT_TIMESTAMP",
      "curdate" -> "CURRENT_DATE",
      "monotonically_increasing_id" -> "ROW_ID",
      "input_file_name" -> "SOURCE_FILE_NAME",
      "spark_partition_id" -> "PARTITION_ID",
      "input_file_block_start" -> "SOURCE_BLOCK_START",
      "input_file_block_length" -> "SOURCE_BLOCK_LENGTH"
    )
  )

  /** The canonical name for a Catalyst expression's `prettyName`. */
  def canonicalName(prettyName: String): String = aliases.canonicalName(prettyName)
}
