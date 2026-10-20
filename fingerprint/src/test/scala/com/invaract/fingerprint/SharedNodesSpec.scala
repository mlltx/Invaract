// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.fingerprint

import com.invaract.ir._
import org.scalatest.funsuite.AnyFunSuite

import java.nio.charset.StandardCharsets.UTF_8

/** A plan may share nodes (see `ir.Plan`), and a column may be defined from the same column more than once. The
  * fingerprint is worked out once per node object and hashed from its children's hashes (`Digests`), so neither
  * costs more than the plan's distinct nodes, and sharing never changes a fingerprint.
  */
class SharedNodesSpec extends AnyFunSuite {

  private def col(name: String, qualifier: Option[String] = None) = ColumnReference(ColumnRef(name, qualifier))
  private def positive(name: String) = Comparison(">", col(name), Literal(0, "int"))

  private def sha(parts: Array[Byte]*): Array[Byte] = {
    val md = java.security.MessageDigest.getInstance("SHA-256")
    parts.foreach(p => md.update(p))
    md.digest()
  }
  private def hex(bytes: Array[Byte]) = bytes.map(b => f"${b & 0xff}%02x").mkString

  // --- the hashing rule, byte for byte -----------------------------------------------------------------

  test("a leaf's hash is SHA-256 over its kind byte (1), its length and its bytes") {
    val leaf = CanonicalNode.stringLeaf("ab")
    assert(FingerprintHasher.hash(leaf).value == hex(sha(Array[Byte](1), Array[Byte](2), "ab".getBytes(UTF_8))))
  }

  test("a tag's hash is SHA-256 over its kind byte (0), its name, its field count and each field's hash in order") {
    val (x, y) = (CanonicalNode.stringLeaf("x"), CanonicalNode.stringLeaf("y"))
    val leafHash = (s: String) => sha(Array[Byte](1), Array[Byte](1), s.getBytes(UTF_8))
    val expected = sha(Array[Byte](0), Array[Byte](3), "Tag".getBytes(UTF_8), Array[Byte](2), leafHash("x"), leafHash("y"))
    assert(FingerprintHasher.hash(CTag("Tag", List(x, y))).value == hex(expected))
    assert(FingerprintHasher.hash(CTag("Tag")).value == hex(sha(Array[Byte](0), Array[Byte](3), "Tag".getBytes(UTF_8), Array[Byte](0))))
  }

  test("the hash tells apart field order, a tag from a leaf of the same bytes, and a field's absence") {
    val (x, y) = (CanonicalNode.stringLeaf("x"), CanonicalNode.stringLeaf("y"))
    val hashes = List(CTag("T", List(x, y)), CTag("T", List(y, x)), CTag("T", List(x)), CTag("T"), CTag("x"), x).map(FingerprintHasher.hash(_).value)
    assert(hashes.distinct.size == hashes.size)
    assert(FingerprintHasher.hash(x).version == 4 && FingerprintHasher.CurrentVersion == 4)
  }

  test("a deep canonical tree is hashed without running out of stack") {
    val deep = (1 to 20000).foldLeft[CanonicalNode](CanonicalNode.stringLeaf("leaf"))((below, _) => CTag("Wrap", List(below)))
    assert(FingerprintHasher.hash(deep).value.length == 64)
  }

  // --- sharing never changes a fingerprint ----------------------------------------------------------------

  private val orders = Read(DatasetRef("raw.orders"))

  private def branchesThatRejoin(below: Plan): Plan =
    Union(List(Project(below, List(NamedExpr("v", col("v")))), Project(below, List(NamedExpr("v", col("v"))))))

  test("a plan that shares a node fingerprints exactly as the same plan with that node copied") {
    val shared = Filter(Project(orders, List(NamedExpr("v", col("amount")))), positive("v"))
    def copy = Filter(Project(Read(DatasetRef("raw.orders")), List(NamedExpr("v", col("amount")))), positive("v"))
    val asGraph = Write(DatasetRef("out"), Union(List(shared, shared)))
    val asTree = Write(DatasetRef("out"), Union(List(copy, copy)))
    val (graph, tree) = (TransformationFingerprinter.fingerprint(asGraph), TransformationFingerprinter.fingerprint(asTree))
    assert(graph.overall == tree.overall)
    assert(graph.outputs == tree.outputs)
  }

  test("a change inside a shared node still changes the fingerprint") {
    def plan(threshold: Int) = {
      val shared = Filter(Project(orders, List(NamedExpr("v", col("amount")))), Comparison(">", col("v"), Literal(threshold, "int")))
      Write(DatasetRef("out"), Union(List(shared, shared)))
    }
    assert(TransformationFingerprinter.fingerprint(plan(0)).overall != TransformationFingerprinter.fingerprint(plan(1)).overall)
  }

  test("inputs: a read under a shared node is one occurrence; a read reached directly twice is two, as PlanFacts counts them") {
    val shared = Filter(orders, positive("amount"))
    val throughShared = Canonicalizer.buildScopeInfo(Union(List(shared, shared)))
    assert(throughShared.reads.map(_.inputKey) == List("raw.orders#0"))
    val selfJoin = Canonicalizer.buildScopeInfo(Join(orders, orders, JoinType.Inner))
    assert(selfJoin.reads.map(_.inputKey) == List("raw.orders#0", "raw.orders#1"))
  }

  // --- and it costs the distinct nodes, not the paths ----------------------------------------------------------

  test("branches that rejoin 40 times in a row (2^40 paths) are fingerprinted at once") {
    val plan = Write(DatasetRef("out"), (1 to 40).foldLeft[Plan](Project(orders, List(NamedExpr("v", col("amount")))))((below, _) => branchesThatRejoin(below)))
    val fp = TransformationFingerprinter.fingerprint(plan)
    assert(fp.outputs.keySet == Set("v"))
    assert(fp.inputs.keySet == Set("raw.orders#0"))
  }

  test("a column defined from itself twice, 40 times in a row, is fingerprinted at once, and its definition is seen whole") {
    def chain(n: Int, rate: Int) =
      Write(
        DatasetRef("out"),
        (1 to n).foldLeft[Plan](Project(orders, List(NamedExpr("v", Arithmetic("*", List(col("amount"), Literal(rate, "int")))))))((below, _) =>
          Project(below, List(NamedExpr("v", Arithmetic("+", List(col("v"), col("v"))))))
        )
      )
    val fp = TransformationFingerprinter.fingerprint(chain(40, 2))
    assert(fp.outputs("v").nonDeterministic.contains(false))
    // the literal at the bottom is part of the resolved definition, however deep
    assert(fp.outputs("v").expression != TransformationFingerprinter.fingerprint(chain(40, 3)).outputs("v").expression)
  }

  test("an expression several outputs resolve through is classified once and the same way for each") {
    val random = Project(orders, List(NamedExpr("r", Function("RAND", Nil))))
    val plan = Write(DatasetRef("out"), Project(random, List(NamedExpr("a", col("r")), NamedExpr("b", Arithmetic("+", List(col("r"), col("r")))))))
    val fp = TransformationFingerprinter.fingerprint(plan)
    assert(fp.outputs("a").nonDeterministic.contains(true) && fp.outputs("b").nonDeterministic.contains(true))
  }
}
