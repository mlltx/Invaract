// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import com.invaract.ir
import com.invaract.ir.{FunctionAliases, FunctionCatalog}
import com.invaract.fingerprint.NonDeterminism

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import scala.util.Try

/** Spark's function names against `ir.FunctionCatalog`. The unit tests pin the table; the sweep proves
  * it complete against Spark itself - for every function Spark's own registry can call with no
  * arguments, whatever Catalyst says is non-deterministic must come out of translation as a
  * catalog function, or the fingerprint would silently report it deterministic.
  */
class SparkFunctionAliasesSpec extends AnyFunSuite with BeforeAndAfterAll {
  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder().master("local[1]").appName("SparkFunctionAliasesSpec").config("spark.ui.enabled", "false").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
  }

  override def afterAll(): Unit = spark.stop()

  private def functionNames(e: ir.Expr): List[String] = e match {
    case ir.Function(name, args) => name :: args.flatMap(functionNames)
    case ir.Alias(_, inner)      => functionNames(inner)
    case ir.Cast(inner, _)       => functionNames(inner)
    case ir.Arithmetic(_, ops)   => ops.flatMap(functionNames)
    case _                       => Nil
  }

  /** The translated IR of the first projected expression of `SELECT <sql>`. */
  private def translated(sql: String): (Expression, ir.Expr) = {
    val analyzed = spark.sql(s"SELECT $sql AS c").queryExecution.analyzed
    val projected = analyzed.expressions.head.children.head
    (projected, SparkPlanAdapter.translateExprStandalone(projected))
  }

  test("each Spark spelling that differs from the catalog maps onto its canonical name, whatever the case") {
    Map(
      "random" -> "RAND", "RANDOM" -> "RAND", "now" -> "CURRENT_TIMESTAMP", "curdate" -> "CURRENT_DATE",
      "monotonically_increasing_id" -> "ROW_ID", "input_file_name" -> "SOURCE_FILE_NAME", "spark_partition_id" -> "PARTITION_ID",
      "input_file_block_start" -> "SOURCE_BLOCK_START", "input_file_block_length" -> "SOURCE_BLOCK_LENGTH"
    ).foreach { case (spark, canonical) => assert(SparkFunctionAliases.canonicalName(spark) == canonical, spark) }
  }

  test("a name that already is the catalog's, and one the catalog does not know, pass through upper-cased") {
    List("uuid" -> "UUID", "rand" -> "RAND", "randn" -> "RANDN", "current_timestamp" -> "CURRENT_TIMESTAMP", "sum" -> "SUM", "isnotnull" -> "ISNOTNULL")
      .foreach { case (n, c) => assert(SparkFunctionAliases.canonicalName(n) == c, n) }
  }

  test("every alias target is a catalog name, and every catalog function is reachable from some Spark spelling") {
    // FunctionAliases rejects a bad target on construction; this pins that Spark covers the whole catalog.
    val spellings = SparkFunctionAliases.aliases.aliases.values.toSet ++ FunctionCatalog.all.map(_.name).filter(n => SparkFunctionAliases.canonicalName(n) == n)
    assert(FunctionCatalog.all.map(_.name).toSet.subsetOf(spellings))
    assert(SparkFunctionAliases.aliases.engine == "spark")
  }

  test("translation uses the alias: random() and rand() are the same canonical RAND function in the IR") {
    val viaRandom = functionNames(translated("random()")._2)
    val viaRand = functionNames(translated("rand()")._2)
    assert(viaRandom.contains("RAND") && viaRand.contains("RAND"))
    assert(!viaRandom.contains("RANDOM"))
  }

  test("now() and current_timestamp() both reach the IR as CURRENT_TIMESTAMP, and are flagged non-deterministic by the fingerprint") {
    List("now()", "current_timestamp()").foreach { sql =>
      val ir = translated(sql)._2
      assert(functionNames(ir).contains("CURRENT_TIMESTAMP"), sql)
      assert(NonDeterminism.classify(ir) == Some(true), sql)
    }
  }

  test("uuid() and monotonically_increasing_id() are flagged non-deterministic; an ordinary function is not") {
    assert(NonDeterminism.classify(translated("uuid()")._2) == Some(true))
    assert(NonDeterminism.classify(translated("monotonically_increasing_id()")._2) == Some(true))
    assert(NonDeterminism.classify(translated("upper('a')")._2) == Some(false))
  }

  test("a seeded and an unseeded rand() both fingerprint without their seed (RAND is seed-bearing under every spelling)") {
    List("rand()", "random()", "rand(7)", "random(7)").foreach { sql =>
      val names = functionNames(translated(sql)._2)
      assert(names.exists(FunctionCatalog.isSeedBearing), sql)
    }
  }

  // --- the sweep -----------------------------------------------------------------------------------

  test("no non-deterministic function Spark can call with no arguments escapes the catalog") {
    val registry = spark.sessionState.functionRegistry.listFunction().map(_.funcName)
    val escaped = scala.collection.mutable.ListBuffer.empty[String]
    val nonDeterministicSeen = scala.collection.mutable.ListBuffer.empty[String]
    var swept = 0
    registry.distinct.sorted.foreach { name =>
      Try {
        val analyzed = spark.sql(s"SELECT `$name`() AS c").queryExecution.analyzed
        val e = analyzed.expressions.head.children.head
        (e, SparkPlanAdapter.translateExprStandalone(e))
      }.foreach { case (e, irExpr) =>
        swept += 1
        if (!e.deterministic) {
          nonDeterministicSeen += name
          if (!NonDeterminism.classify(irExpr).contains(true)) escaped += name
        }
      }
    }
    assert(swept > 20, s"the sweep only reached $swept functions - something is wrong with how it calls them")
    assert(escaped.toList == Nil, s"non-deterministic Spark functions the catalog does not cover (add an alias or a catalog entry): ${escaped.mkString(", ")}")
    // Proves the sweep can see what it is looking for, so an empty `escaped` means something.
    assert(Set("rand", "uuid", "monotonically_increasing_id", "input_file_name", "spark_partition_id")
      .subsetOf(nonDeterministicSeen.toSet), s"non-deterministic functions seen: ${nonDeterministicSeen.mkString(", ")}")
  }

  test("the clock functions are deterministic *within* a query to Spark, but differ between runs - the catalog flags them anyway") {
    // Spark's own `deterministic` flag is the wrong yardstick for these (it replaces them with one
    // literal per query), which is why the sweep above only requires Spark-non-deterministic functions
    // to be covered, not the reverse.
    List("now()", "current_timestamp()", "current_date()", "unix_timestamp()").foreach { sql =>
      assert(NonDeterminism.classify(translated(sql)._2) == Some(true), sql)
    }
  }

  test("the same holds for the non-deterministic functions that need arguments") {
    List("shuffle(array(1, 2, 3))", "rand(1)", "randn(1)").foreach { sql =>
      val (e, irExpr) = translated(sql)
      assert(!e.deterministic, s"$sql was expected to be non-deterministic in Spark")
      assert(NonDeterminism.classify(irExpr).contains(true), s"$sql escapes the catalog")
    }
  }
}
