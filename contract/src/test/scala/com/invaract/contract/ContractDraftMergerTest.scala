// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.contract

import com.invaract.contract.MergeConflict._
import org.scalatest.funsuite.AnyFunSuite

class ContractDraftMergerTest extends AnyFunSuite {

  private def schema(fields: (String, String)*): Schema =
    Schema(fields.toList.map { case (n, t) => Field(n, t, required = true, nullable = true) })

  private val ordersSchema = schema("order_id" -> "integer", "amount" -> "integer")
  private val customersSchema = schema("customer_id" -> "string", "name" -> "string")
  private val productsSchema = schema("product_id" -> "integer")

  private def input(name: String, location: String, s: Schema, description: Option[String] = None) =
    Dataset(name, location, None, s, description = description)

  private def output(location: String, s: Schema, derivedFrom: Option[List[String]], name: String = "output") =
    Dataset(name, location, Some("parquet"), s, saveMode = Some("overwrite"), derivedFrom = derivedFrom)

  /** A draft as dry-run infers it: positional input names, one output naming what it read. */
  private def draft(inputs: List[Dataset], out: Dataset): Contract =
    Contract("inferred_contract", ContractVersion(0, 1, 0), "draft", inputs, List(out), Nil, Map.empty)

  private def merge(drafts: Contract*) = ContractDraftMerger.merge(drafts.toList, "merged", ContractVersion(1, 0, 0))

  private def merged(drafts: Contract*): Contract = merge(drafts: _*) match {
    case Right(c)        => c
    case Left(conflicts) => fail(s"expected a merged contract, got: $conflicts")
  }

  // ---- the scenarios ----------------------------------------------------

  test("two writes sharing one input: one merged input, two outputs, each derivedFrom rewritten to it") {
    val a = draft(List(input("input", "/data/orders.parquet", ordersSchema)), output("/out/a.parquet", ordersSchema, Some(List("input"))))
    val b = draft(List(input("input", "/data/orders.parquet", ordersSchema)), output("/out/b.parquet", ordersSchema, Some(List("input"))))

    val c = merged(a, b)

    assert(c.inputs.map(i => i.name -> i.location) == List("orders" -> "/data/orders.parquet"))
    assert(c.outputs.map(o => o.name -> o.derivedFrom) == List("a" -> Some(List("orders")), "b" -> Some(List("orders"))))
    assert(c.id == "merged" && c.version == ContractVersion(1, 0, 0))
  }

  test("disjoint inputs: the union, and every output keeps naming only its own inputs") {
    val a = draft(
      List(input("input_1", "/data/orders.parquet", ordersSchema), input("input_2", "/data/customers.parquet", customersSchema)),
      output("/out/a.parquet", ordersSchema, Some(List("input_1", "input_2")))
    )
    val b = draft(List(input("input", "/data/products.parquet", productsSchema)), output("/out/b.parquet", productsSchema, Some(List("input"))))

    val c = merged(a, b)

    assert(c.inputs.map(_.name) == List("orders", "customers", "products"))
    assert(c.outputs.map(o => o.name -> o.derivedFrom) == List("a" -> Some(List("orders", "customers")), "b" -> Some(List("products"))))
    assert(c.inputsFor(c.outputs(1)).map(_.name) == List("products"))
  }

  test("conflicting schemas for the same input location are reported, not resolved to one of them") {
    val a = draft(List(input("input", "/data/orders.parquet", ordersSchema)), output("/out/a.parquet", ordersSchema, Some(List("input"))))
    val changed = schema("order_id" -> "long", "amount" -> "integer")
    val b = draft(List(input("input", "/data/orders.parquet", changed)), output("/out/b.parquet", ordersSchema, Some(List("input"))))

    merge(a, b) match {
      case Left(List(InputSchemaConflict(loc, schemas))) =>
        assert(loc == "/data/orders.parquet")
        assert(schemas == List(ordersSchema, changed))
      case other => fail(s"expected exactly one InputSchemaConflict, got: $other")
    }
  }

  test("a nullability-only difference at the same location is a conflict too") {
    val strict = Schema(List(Field("id", "integer", required = true, nullable = false)))
    val lax = Schema(List(Field("id", "integer", required = true, nullable = true)))
    val a = draft(List(input("input", "/data/x.parquet", strict)), output("/out/a.parquet", ordersSchema, Some(List("input"))))
    val b = draft(List(input("input", "/data/x.parquet", lax)), output("/out/b.parquet", ordersSchema, Some(List("input"))))

    assert(merge(a, b).left.exists(_.exists(_.isInstanceOf[InputSchemaConflict])))
  }

  test("writes that read nothing: outputs keep derivedFrom [] (never 'all inputs'), and the merged contract has no inputs") {
    val a = draft(Nil, output("/out/a.parquet", ordersSchema, Some(Nil)))
    val b = draft(Nil, output("/out/b.parquet", ordersSchema, Some(Nil)))

    val c = merged(a, b)

    assert(c.inputs.isEmpty)
    assert(c.outputs.map(_.derivedFrom) == List(Some(Nil), Some(Nil)))
  }

  test("a write that reads nothing beside one that does: each keeps its own mapping") {
    val a = draft(Nil, output("/out/a.parquet", ordersSchema, Some(Nil)))
    val b = draft(List(input("input", "/data/orders.parquet", ordersSchema)), output("/out/b.parquet", ordersSchema, Some(List("input"))))

    val c = merged(a, b)

    assert(c.outputs.map(o => o.name -> o.derivedFrom) == List("a" -> Some(Nil), "b" -> Some(List("orders"))))
  }

  // ---- outputs -----------------------------------------------------------

  test("two drafts writing one location are ONE output: schema must agree, derivedFrom is the union in merged-input order") {
    val a = draft(List(input("input", "/data/orders.parquet", ordersSchema)), output("/out/x.parquet", ordersSchema, Some(List("input"))))
    val b = draft(List(input("input", "/data/customers.parquet", customersSchema)), output("/out/x.parquet", ordersSchema, Some(List("input"))))

    val c = merged(a, b)

    assert(c.outputs.size == 1)
    assert(c.outputs.head.derivedFrom == Some(List("orders", "customers")))
  }

  test("two writes to one location with different schemas: an OutputSchemaConflict") {
    val a = draft(Nil, output("/out/x.parquet", ordersSchema, Some(Nil)))
    val b = draft(Nil, output("/out/x.parquet", customersSchema, Some(Nil)))

    assert(merge(a, b) == Left(List(OutputSchemaConflict("/out/x.parquet", List(ordersSchema, customersSchema)))))
  }

  test("one of the writes to a location saying 'all inputs' (derivedFrom None) makes the merged output 'all inputs'") {
    val a = draft(List(input("input", "/data/orders.parquet", ordersSchema)), output("/out/x.parquet", ordersSchema, Some(List("input"))))
    val b = draft(Nil, output("/out/x.parquet", ordersSchema, None))

    assert(merged(a, b).outputs.head.derivedFrom.isEmpty)
  }

  // ---- naming and location identity ---------------------------------------

  test("names come from the location, not from the draft-local positional names or from which write read a dataset first") {
    val a = draft(
      List(input("input_1", "/data/customers.parquet", customersSchema), input("input_2", "/data/orders.parquet", ordersSchema)),
      output("/out/a.parquet", ordersSchema, Some(List("input_2")))
    )
    val b = draft(List(input("input", "/data/orders.parquet", ordersSchema)), output("/out/b.parquet", ordersSchema, Some(List("input"))))

    val c = merged(a, b)

    assert(c.inputs.map(_.name) == List("customers", "orders"))
    assert(c.outputs.map(o => o.name -> o.derivedFrom) == List("a" -> Some(List("orders")), "b" -> Some(List("orders"))))
  }

  test("two different locations with the same basename get _2, _3 in order of appearance") {
    val a = draft(
      List(input("input_1", "/raw/orders.parquet", ordersSchema), input("input_2", "/curated/orders.parquet", ordersSchema)),
      output("/out/a.parquet", ordersSchema, Some(List("input_1", "input_2")))
    )
    val b = draft(List(input("input", "/other/orders.csv", ordersSchema)), output("/out/b.parquet", ordersSchema, Some(List("input"))))

    assert(merged(a, b).inputs.map(i => i.name -> i.location) ==
      List("orders" -> "/raw/orders.parquet", "orders_2" -> "/curated/orders.parquet", "orders_3" -> "/other/orders.csv"))
  }

  test("names are lower-cased and sanitized; catalog identifiers use their last dotted part; an empty result falls back to 'dataset'") {
    val a = draft(
      List(
        input("input_1", "/data/Daily Orders (v2).parquet", ordersSchema),
        input("input_2", "spark_catalog.default.Customers", customersSchema),
        input("input_3", "/data/.parquet", productsSchema)
      ),
      output("/out/Report-2024.parquet", ordersSchema, Some(List("input_1", "input_2", "input_3")))
    )

    val c = merged(a)

    assert(c.inputs.map(_.name) == List("daily_orders_v2", "customers", "dataset"))
    assert(c.outputs.head.name == "report_2024")
  }

  test("locations are compared normalized: file: prefix, backslashes and a trailing slash do not make a second input") {
    val a = draft(List(input("input", "file:/data/orders.parquet", ordersSchema)), output("/out/a.parquet", ordersSchema, Some(List("input"))))
    val b = draft(List(input("input", "\\data\\orders.parquet", ordersSchema)), output("/out/b.parquet", ordersSchema, Some(List("input"))))
    val c = draft(List(input("input", "/data/orders.parquet/", ordersSchema)), output("/out/c.parquet", ordersSchema, Some(List("input"))))

    val m = merged(a, b, c)

    assert(m.inputs.size == 1 && m.inputs.head.location == "file:/data/orders.parquet")
    assert(m.outputs.forall(_.derivedFrom == Some(List("orders"))))
  }

  test("a lone '/' keeps its slash when normalized (it is not turned into an empty location)") {
    val a = draft(List(input("input", "/", ordersSchema)), output("/out/a.parquet", ordersSchema, Some(List("input"))))
    assert(merged(a).inputs.map(_.name) == List("dataset"))
  }

  test("an input and an output may share a derived name (their name spaces are separate)") {
    val a = draft(List(input("input", "/data/orders.parquet", ordersSchema)), output("/out/orders.parquet", ordersSchema, Some(List("input"))))
    val c = merged(a)
    assert(c.inputs.map(_.name) == List("orders") && c.outputs.map(_.name) == List("orders"))
  }

  // ---- what else is merged ------------------------------------------------

  test("input descriptions: distinct ones, first-appearance order, space-separated; none stays None") {
    val a = draft(List(input("input", "/data/x.parquet", ordersSchema, Some("Observed: contributes."))), output("/out/a.parquet", ordersSchema, Some(List("input"))))
    val b = draft(List(input("input", "/data/x.parquet", ordersSchema, Some("Observed: referenced only in a Filter."))), output("/out/b.parquet", ordersSchema, Some(List("input"))))
    val c = draft(List(input("input", "/data/x.parquet", ordersSchema, Some("Observed: contributes."))), output("/out/c.parquet", ordersSchema, Some(List("input"))))
    val plain = draft(List(input("input", "/data/y.parquet", ordersSchema)), output("/out/d.parquet", ordersSchema, Some(List("input"))))

    val m = merged(a, b, c, plain)

    assert(m.inputs.map(_.description) == List(Some("Observed: contributes. Observed: referenced only in a Filter."), None))
  }

  test("input and output format/saveMode come from the first draft that has one") {
    val a = draft(List(input("input", "/data/x.parquet", ordersSchema)), output("/out/x.parquet", ordersSchema, Some(List("input"))).copy(format = None, saveMode = None))
    val b = draft(List(input("input", "/data/x.parquet", ordersSchema).copy(format = Some("parquet"))), output("/out/x.parquet", ordersSchema, Some(List("input"))).copy(format = Some("delta"), saveMode = Some("append")))

    val m = merged(a, b)

    assert(m.inputs.head.format == Some("parquet"))
    assert(m.outputs.head.format == Some("delta") && m.outputs.head.saveMode == Some("append"))
  }

  test("rules are unioned without duplicates; extensions and customRuleTypes keep the EARLIEST draft's value per key") {
    val rule = ContractRule("forbid_cross_join", Map.empty)
    val other = ContractRule("forbid_unconditional_delete", Map.empty)
    val a = draft(Nil, output("/out/a.parquet", ordersSchema, Some(Nil))).copy(
      rules = List(rule), extensions = Map("owner" -> "first", "team" -> "x"), customRuleTypes = Map("k" -> "A"), status = "active"
    )
    val b = draft(Nil, output("/out/b.parquet", ordersSchema, Some(Nil))).copy(
      rules = List(rule, other), extensions = Map("owner" -> "second"), customRuleTypes = Map("k" -> "B", "j" -> "C")
    )

    val m = merged(a, b)

    assert(m.rules == List(rule, other))
    assert(m.extensions == Map("owner" -> "first", "team" -> "x"))
    assert(m.customRuleTypes == Map("k" -> "A", "j" -> "C"))
    assert(m.status == "active", "the first draft's status")
  }

  // ---- refusals --------------------------------------------------------------

  test("no drafts: NoDrafts") {
    assert(merge() == Left(List(NoDrafts)))
    assert(NoDrafts.location.isEmpty && NoDrafts.message.nonEmpty)
  }

  test("a derivedFrom naming an input the draft does not declare is a MalformedDraft, with that draft's index") {
    val ok = draft(Nil, output("/out/a.parquet", ordersSchema, Some(Nil)))
    val bad = draft(List(input("input", "/data/x.parquet", ordersSchema)), output("/out/b.parquet", ordersSchema, Some(List("input", "ghost"))))

    merge(ok, bad) match {
      case Left(List(MalformedDraft(1, message))) => assert(message.contains("'ghost'") && message.contains("derivedFrom"))
      case other                                  => fail(s"expected one MalformedDraft(1, ...), got: $other")
    }
  }

  test("two inputs sharing a name inside one draft is a MalformedDraft") {
    val bad = draft(
      List(input("input", "/data/x.parquet", ordersSchema), input("input", "/data/y.parquet", ordersSchema)),
      output("/out/a.parquet", ordersSchema, None)
    )
    assert(merge(bad).left.exists(_.exists { case MalformedDraft(0, m) => m.contains("'input'"); case _ => false }))
  }

  test("an output-less result is reported as InvalidResult (it never returns a contract ContractValidator rejects)") {
    val noOutput = draft(Nil, output("/out/a.parquet", ordersSchema, Some(Nil))).copy(outputs = Nil)
    merge(noOutput) match {
      case Left(List(InvalidResult(message))) => assert(message.nonEmpty)
      case other                              => fail(s"expected an InvalidResult, got: $other")
    }
  }

  test("every conflict names its location, and the schema conflicts render a readable message") {
    val a = draft(List(input("input", "/data/x.parquet", ordersSchema)), output("/out/x.parquet", ordersSchema, Some(List("input"))))
    val b = draft(List(input("input", "/data/x.parquet", customersSchema)), output("/out/x.parquet", customersSchema, Some(List("input"))))

    val conflicts = merge(a, b).left.getOrElse(fail("expected conflicts"))

    assert(conflicts.map(_.location) == List(Some("/data/x.parquet"), Some("/out/x.parquet")))
    assert(conflicts.forall(c => c.message.contains(c.location.get) && c.message.contains("order_id:integer") && c.message.contains("customer_id:string")))
  }

  // ---- the result is a real contract -------------------------------------------

  test("the merged contract passes ContractValidator, is deterministic, and round-trips through ContractParser unchanged") {
    val a = draft(
      List(input("input_1", "/data/orders.parquet", ordersSchema, Some("Observed: contributes.")), input("input_2", "/data/customers.parquet", customersSchema)),
      output("/out/a.parquet", ordersSchema, Some(List("input_1", "input_2")))
    )
    val b = draft(List(input("input", "/data/products.parquet", productsSchema)), output("/out/b.parquet", productsSchema, Some(List("input"))))
    val c = draft(Nil, output("/out/c.parquet", ordersSchema, Some(Nil)))

    val m = merged(a, b, c)

    assert(ContractValidator.validate(m).errors.isEmpty)
    assert(merged(a, b, c) == m)
    assert(ContractParser.parse(ContractParser.write(m)) == m)
  }

  test("merges drafts exactly as dry-run prints them (parsed from YAML), and the result still parses and validates") {
    def yaml(inputLocation: String, outputLocation: String) =
      s"""id: inferred_contract
         |version: 0.1.0
         |status: draft
         |inputs:
         |- name: input
         |  location: $inputLocation
         |  schema:
         |    fields:
         |    - name: id
         |      type: integer
         |      required: true
         |      nullable: true
         |  description: 'Observed: contributes to at least one produced output column.'
         |outputs:
         |- name: output
         |  location: $outputLocation
         |  format: parquet
         |  schema:
         |    fields:
         |    - name: id
         |      type: integer
         |      required: true
         |      nullable: true
         |  saveMode: overwrite
         |  derivedFrom:
         |  - input
         |""".stripMargin
    val drafts = List(yaml("/data/in.csv", "/out/one.parquet"), yaml("/data/in.csv", "/out/two.parquet")).map(ContractParser.parse)

    val m = ContractDraftMerger.merge(drafts, "pipeline", ContractVersion(1, 0, 0)).getOrElse(fail("expected a merge"))

    assert(m.inputs.map(_.name) == List("in") && m.outputs.map(o => o.name -> o.derivedFrom) == List("one" -> Some(List("in")), "two" -> Some(List("in"))))
    assert(ContractValidator.validate(ContractParser.parse(ContractParser.write(m))).errors.isEmpty)
  }
}
