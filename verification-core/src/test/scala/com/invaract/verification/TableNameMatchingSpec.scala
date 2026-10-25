// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.verification

import com.invaract.contract._
import com.invaract.contract.LogicalType.{IntegerType, StringType}
import com.invaract.ir._

import org.scalatest.funsuite.AnyFunSuite

/** A contract can name a catalog table (`gold/orders`) rather than where its files are: a read or write that resolved to
  * a catalog table answers to its table name as well as to the location the engine reports (`LocationMatching.namesOf`),
  * and every check that pairs a declared location with a dataset uses both. Spark reports an existing table by its files
  * (`file:/warehouse/gold.db/orders`) and a table it is creating by its qualified name (`spark_catalog.gold.orders`); the
  * table name is the same either way, so a contract that names the table matches both.
  */
class TableNameMatchingSpec extends AnyFunSuite {

  private val Files = "file:/warehouse/gold.db/orders"
  private val orders = CatalogIdentity(catalogName = Some("spark_catalog"), namespace = List("gold"), table = Some("orders"))

  private def ds(name: String, location: String, fields: Field*): Dataset = Dataset(name, location, None, Schema(fields.toList))

  private def contract(inputs: List[Dataset], outputs: List[Dataset]): Contract =
    Contract("t", ContractVersion(1, 0, 0), "active", inputs, outputs, Nil, Map.empty)

  private def col(name: String, qualifier: String) = ColumnReference(ColumnRef(name, Some(qualifier)))

  // --- the primitives ---

  test("tableName joins catalog, namespace and table; without a table there is none") {
    assert(LocationMatching.tableName(orders).contains("spark_catalog/gold/orders"))
    assert(LocationMatching.tableName(orders.copy(catalogName = None)).contains("gold/orders"))
    assert(LocationMatching.tableName(orders.copy(namespace = Nil)).contains("spark_catalog/orders"))
    assert(LocationMatching.tableName(orders.copy(namespace = List("a", "b"))).contains("spark_catalog/a/b/orders"))
    assert(LocationMatching.tableName(orders.copy(table = None)).isEmpty)
    assert(LocationMatching.tableName(CatalogIdentity()).isEmpty)
  }

  test("namesOf is the location, then the table name, each once") {
    assert(LocationMatching.namesOf(Files, None) == List(Files))
    assert(LocationMatching.namesOf(Files, Some(orders)) == List(Files, "spark_catalog/gold/orders"))
    assert(LocationMatching.namesOf("spark_catalog/gold/orders", Some(orders)) == List("spark_catalog/gold/orders"))
    assert(LocationMatching.namesOf(Files, Some(CatalogIdentity(technology = Some("hive")))) == List(Files))
  }

  test("matchesAny: a declared location matches when it matches any name, and never with no names") {
    val names = LocationMatching.namesOf(Files, Some(orders))
    assert(LocationMatching.matchesAny("gold/orders", names))
    assert(LocationMatching.matchesAny("orders", names))
    assert(LocationMatching.matchesAny("/warehouse/gold.db/orders", names))
    assert(!LocationMatching.matchesAny("silver/orders", names))
    assert(!LocationMatching.matchesAny("gold.orders", names), "a dotted name is one segment")
    assert(!LocationMatching.matchesAny("gold/orders", Nil))
  }

  test("LocationIndex over several names: every matching entry once, in declaration order") {
    val index = LocationIndex(Seq("gold/orders" -> 0, "gold.db/orders" -> 1, "customers" -> 2, "orders" -> 3))
    val names = LocationMatching.namesOf(Files, Some(orders))
    assert(index.matchingIndicesOf(names) == Vector(0, 1, 3))
    assert(index.firstOf(names).contains(0))
    assert(index.matchesAnyOf(names))
    assert(index.matchingIndicesOf(List("x/customers", "y/customers")) == Vector(2))
    assert(index.matchingIndicesOf(Nil).isEmpty)
    assert(index.firstOf(List("nothing")).isEmpty)
    assert(!index.matchesAnyOf(List("nothing", "else")))
    assert(!index.matchesAnyOf(Nil))
  }

  test("PlanFacts.namesOf: a read location answers to the table names of the reads there; anything else to itself") {
    val plan = Join(
      Read(DatasetRef(Files), catalog = Some(orders)),
      Join(Read(DatasetRef("/plain/customers")), Read(DatasetRef(Files), alias = Some("again"), catalog = Some(orders.copy(catalogName = None))), JoinType.Inner, None),
      JoinType.Inner,
      None
    )
    val facts = PlanFacts.of(plan)
    assert(facts.namesOf(Files) == List(Files, "spark_catalog/gold/orders", "gold/orders"))
    assert(facts.namesOf("/plain/customers") == List("/plain/customers"))
    assert(facts.namesOf("never/read") == List("never/read"))
  }

  // --- inputs ---

  private val ordersRead = Read(DatasetRef(Files), catalog = Some(orders))
  private def writeOf(input: Plan, location: String = "gold/out", catalog: Option[CatalogIdentity] = None): Write =
    Write(DatasetRef(location), Project(input, List(NamedExpr("id", col("id", Files)))), catalog = catalog)

  private def verify(c: Contract, plan: Plan, inputSchemas: List[(String, LogicalSchema)] = Nil, options: VerificationOptions = VerificationOptions()) =
    StructuralVerifier.verify(c, plan, inputSchemas, LogicalSchema(Nil), options).violations

  test("an input declared by its table name is read when the read resolved to that table, and missing when it did not") {
    val c = contract(List(ds("orders", "gold/orders")), List(ds("out", "gold/out")))
    assert(!verify(c, writeOf(ordersRead)).exists(_.violationType == ViolationType.MissingInput))
    assert(verify(c, writeOf(Read(DatasetRef(Files)))).exists(_.violationType == ViolationType.MissingInput))
  }

  test("a read is declared when an input names its table, and undeclared when none names it either way") {
    val strict = VerificationOptions(rejectUndeclaredInputs = true)
    val named = contract(List(ds("orders", "gold/orders")), List(ds("out", "gold/out")))
    assert(!verify(named, writeOf(ordersRead), options = strict).exists(_.violationType == ViolationType.UndeclaredInput))
    val other = contract(List(ds("orders", "silver/orders")), List(ds("out", "gold/out")))
    val undeclared = verify(other, writeOf(ordersRead), options = strict).filter(_.violationType == ViolationType.UndeclaredInput)
    assert(undeclared.map(_.location) == List(Some(Files)), "the violation names where the read is")
  }

  test("undeclaredReads: a read an input names by table is not undeclared; one only another output's input names is reported as that") {
    val in = ds("orders", "gold/orders")
    val out = Dataset("out", "gold/out", None, Schema(Nil), derivedFrom = Some(Nil))
    val all = InputChecker.Declared(List(in))
    val names: String => List[String] = l => if (l == Files) List(Files, "spark_catalog/gold/orders") else List(l)
    assert(InputChecker.undeclaredReads(List(Files), all, all, None, names).isEmpty)
    assert(InputChecker.undeclaredReads(List(Files), all, all, None).map(_.violationType) == List(ViolationType.UndeclaredInput))
    val notThisOutputs = InputChecker.undeclaredReads(List(Files), all, InputChecker.Declared(Nil), Some(out), names)
    assert(notThisOutputs.size == 1 && notThisOutputs.head.message.contains("orders"), notThisOutputs.toString)
    assert(notThisOutputs.head != InputChecker.undeclaredReads(List(Files), InputChecker.Declared(Nil), InputChecker.Declared(Nil), Some(out), names).head)
  }

  test("an input's schema is checked when the input names the table its read resolved to") {
    val c = contract(List(ds("orders", "gold/orders", Field("id", "integer"))), List(ds("out", "gold/out")))
    val wrongType = List(Files -> LogicalSchema(List(LogicalField("id", StringType))))
    assert(verify(c, writeOf(ordersRead), wrongType).exists(_.violationType == ViolationType.InputFieldTypeMismatch))
    val right = List(Files -> LogicalSchema(List(LogicalField("id", IntegerType))))
    assert(!verify(c, writeOf(ordersRead), right).exists(_.violationType == ViolationType.InputFieldTypeMismatch))
  }

  test("an input's catalog requirement is checked against the read that resolved to its table") {
    val required = Dataset("orders", "gold/orders", None, Schema(Nil), catalog = Some(CatalogRequirement(required = true, namespace = List("silver"))))
    val c = contract(List(required), List(ds("out", "gold/out")))
    assert(verify(c, writeOf(ordersRead)).exists(_.violationType == ViolationType.InputCatalogMismatch))
    val satisfied = required.copy(catalog = Some(CatalogRequirement(required = true, namespace = List("gold"))))
    assert(!verify(contract(List(satisfied), List(ds("out", "gold/out"))), writeOf(ordersRead)).exists(_.violationType == ViolationType.InputCatalogMismatch))
  }

  test("when two reads match one input, the first read's catalog and schema are the ones checked") {
    val silver = CatalogIdentity(catalogName = Some("spark_catalog"), namespace = List("silver"), table = Some("orders"))
    val first = Read(DatasetRef(Files), catalog = Some(orders))
    val second = Read(DatasetRef("/elsewhere/gold/orders"), catalog = Some(silver))
    val plan = Write(DatasetRef("gold/out"), Join(first, second, JoinType.Inner, None))
    val required = Dataset("orders", "gold/orders", None, Schema(List(Field("id", "integer"))), catalog = Some(CatalogRequirement(required = true, namespace = List("gold"))))
    val c = contract(List(required), List(ds("out", "gold/out")))
    val schemas = List(Files -> LogicalSchema(List(LogicalField("id", IntegerType))), "/elsewhere/gold/orders" -> LogicalSchema(List(LogicalField("id", StringType))))
    val violations = verify(c, plan, schemas).map(_.violationType)
    assert(!violations.contains(ViolationType.InputCatalogMismatch), violations.toString)
    assert(!violations.contains(ViolationType.InputFieldTypeMismatch), violations.toString)
    val reversed = verify(c, Write(DatasetRef("gold/out"), Join(second, first, JoinType.Inner, None)), schemas.reverse).map(_.violationType)
    assert(reversed.contains(ViolationType.InputCatalogMismatch) && reversed.contains(ViolationType.InputFieldTypeMismatch), reversed.toString)
  }

  // --- outputs ---

  private val two = List(ds("a", "gold/orders"), ds("b", "silver/orders"))

  test("a write to a table matches the output naming that table, whichever way the engine reports the write") {
    val byFiles = writeOf(Read(DatasetRef("in")), Files, Some(orders))
    val byQualifiedName = writeOf(Read(DatasetRef("in")), "spark_catalog.gold.orders", Some(orders))
    assert(!verify(contract(Nil, two), byFiles).exists(_.violationType == ViolationType.OutputLocationMismatch))
    assert(!verify(contract(Nil, two), byQualifiedName).exists(_.violationType == ViolationType.OutputLocationMismatch))
    val unnamed = verify(contract(Nil, two), writeOf(Read(DatasetRef("in")), Files, None)).filter(_.violationType == ViolationType.OutputLocationMismatch)
    assert(unnamed.map(_.location) == List(Some(Files)), "without a catalog identity only the location can match")
  }

  test("the output a write matches by table name is the one checked: its schema, not another output's") {
    val outputs = List(ds("a", "gold/orders", Field("id", "integer")), ds("b", "silver/orders", Field("other", "string")))
    val write = writeOf(Read(DatasetRef("in")), Files, Some(orders))
    val violations = StructuralVerifier.verify(contract(Nil, outputs), write, Nil, LogicalSchema(List(LogicalField("id", IntegerType)))).violations
    assert(violations.isEmpty, violations.toString)
  }

  test("OutputChecker over names: matchOutput, expectedOutputFor, namesOf, locationFinding") {
    val names = List(Files, "spark_catalog/gold/orders")
    assert(OutputChecker.matchOutput(two, names).map(_.name).contains("a"))
    assert(OutputChecker.matchOutput(two, List(Files)).isEmpty)
    assert(OutputChecker.matchOutput(two, Nil).isEmpty)
    assert(OutputChecker.expectedOutputFor(two, names).map(_.name).contains("a"))
    assert(OutputChecker.expectedOutputFor(two, List("elsewhere")).isEmpty)
    assert(OutputChecker.expectedOutputFor(List(two.head), List("elsewhere")).map(_.name).contains("a"))
    assert(OutputChecker.namesOf(writeOf(Read(DatasetRef("in")), Files, Some(orders))) == names)
    assert(OutputChecker.locationFinding(two, names, Files).isEmpty)
    assert(OutputChecker.locationFinding(two, List(Files), Files).map(_.location) == List(Some(Files)))
    assert(OutputChecker.locationFinding(two, Files).map(_.location) == List(Some(Files)))
  }

  test("unwrittenOutputs counts an output as written when a write's table name matches it") {
    assert(OutputChecker.unwrittenOutputs(two, List(Files, "spark_catalog/gold/orders")).map(_.location) == List(Some("silver/orders")))
    assert(OutputChecker.unwrittenOutputs(two, List(Files)).size == 2)
  }

  // --- state changes (Iceberg CALL procedures) ---

  private val schemaOut = List(ds("a", "db/t", Field("id", "integer")))

  test("a state change on a table an output names is governed by it; without the table name it is not this contract's concern") {
    val wrong = LogicalSchema(List(LogicalField("id", StringType)))
    val governed = StructuralVerifier.verifyStateChange(contract(Nil, schemaOut), "local.db.t", List("local/db/t"), wrong, VerificationOptions(), false)
    assert(governed.violations.map(_.violationType) == List(ViolationType.OutputFieldTypeMismatch))
    assert(StructuralVerifier.verifyStateChange(contract(Nil, schemaOut), "local.db.t", Nil, wrong, VerificationOptions(), false).passed)
    assert(StructuralVerifier.verifyStateChange(contract(Nil, schemaOut), "local.db.t", wrong).passed)
  }

  test("VerificationPipeline.verifyStateChange with a table name refuses a state change that breaks the output naming it") {
    val wrong = LogicalSchema(List(LogicalField("id", StringType)))
    intercept[ContractViolationException] {
      VerificationPipeline.verifyStateChange(contract(Nil, schemaOut), "CALL rollback", "local.db.t", List("local/db/t"), wrong, false, VerificationOptions(), None, None, None)
    }
    VerificationPipeline.verifyStateChange(contract(Nil, schemaOut), "CALL rollback", "local.db.t", wrong, false, VerificationOptions())
  }

  // --- the analyses ---

  test("static data quality: the output and the input axioms are found by table name") {
    val notNull = Field("id", "string", nullable = false)
    val c = contract(List(ds("orders", "gold/orders", notNull)), List(ds("out", "gold/out_table", notNull)))
    val out = CatalogIdentity(catalogName = Some("spark_catalog"), namespace = List("gold"), table = Some("out_table"))
    val plan = Write(DatasetRef("file:/warehouse/gold.db/out_table"), Project(ordersRead, List(NamedExpr("id", col("id", Files)))), catalog = Some(out))
    assert(StaticDataQualityVerifier.verify(c, plan) == List(DataQualityCheckResult("id", "NOT NULL", DataQualityVerdict.Guaranteed)))
    val untabled = Write(DatasetRef("file:/warehouse/gold.db/out_table"), Project(Read(DatasetRef(Files)), List(NamedExpr("id", col("id", Files)))), catalog = Some(out))
    assert(StaticDataQualityVerifier.verify(c, untabled) == List(DataQualityCheckResult("id", "NOT NULL", DataQualityVerdict.NotGuaranteed)))
    assert(StaticDataQualityVerifier.verify(c, plan.copy(catalog = None)).isEmpty, "the output names the table only")
  }

  test("role consistency: an input named by its table is seen contributing to the output") {
    val control = Dataset("calendar", "gold/orders", None, Schema(Nil), datasetType = Some(DatasetType.Control))
    val c = contract(List(control), List(ds("out", "gold/out")))
    val results = RoleConsistencyVerifier.verify(c, writeOf(ordersRead))
    assert(results.map(_.verdict) == List(RoleConformanceVerdict.Contradicts))
    assert(RoleConsistencyVerifier.verify(c, writeOf(Read(DatasetRef(Files)))).isEmpty)
  }

  test("sensitivity: a tagged input named by its table tags the output column built from it") {
    val tagged = ds("orders", "gold/orders", Field("id", "string", sensitivityTags = Set("pii")))
    val c = contract(List(tagged), List(ds("out", "gold/out")))
    assert(SensitivityLineage.propagate(writeOf(ordersRead), c).map(_.sensitivityTags) == List(Set("pii")))
    assert(SensitivityLineage.propagate(writeOf(Read(DatasetRef(Files))), c).map(_.sensitivityTags) == List(Set.empty))
    assert(SensitivityLineage.propagate(Lineage.trace(writeOf(ordersRead)), c).map(_.sensitivityTags) == List(Set.empty), "lineage alone has no reads")
  }
}
