// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import com.invaract.contract.{LogicalField, LogicalSchema, LogicalType, Field => CField}
import com.invaract.sparkadapter.SchemaChecker.Side

import com.invaract.contract.LogicalType._
import org.scalatest.funsuite.AnyFunSuite

class SchemaCheckerSpec extends AnyFunSuite {

  private def check(
      fields: List[CField],
      schema: LogicalSchema,
      side: Side = Side.Output,
      rejectUndeclared: Boolean = false,
      caseSensitive: Boolean = false
  ): List[Violation] = SchemaChecker.check(fields, schema, side, "warehouse/ds", rejectUndeclared, caseSensitive)

  private def types(vs: List[Violation]): List[String] = vs.map(_.violationType)

  // --- scalar behaviour is unchanged ----------------------------------------

  test("scalars: matching type and nullability is clean; a wrong scalar type reports the bare typeName on both sides") {
    val schema = Cols().add("id", IntegerType, nullable = false).add("name", StringType)
    assert(check(List(CField("id", "integer", nullable = false), CField("name", "string")), schema).isEmpty)

    val vs = check(List(CField("id", "string")), schema)
    assert(types(vs) == List(ViolationType.OutputFieldTypeMismatch))
    assert(vs.head.expected.contains("string") && vs.head.actual.contains("integer"))
    assert(vs.head.column.contains("id"))
    assert(vs.head.message == "field 'id' declares type 'string' but the actual OUTPUT schema has type 'integer'")
  }

  test("declared type is compared case-insensitively and trimmed; decimal keeps its precision") {
    val schema = Cols().add("id", IntegerType).add("amt", DecimalType(10, 2))
    assert(check(List(CField("id", "  INTEGER ")), schema).isEmpty)
    assert(check(List(CField("amt", "decimal(10,2)")), schema).isEmpty)
    assert(types(check(List(CField("amt", "decimal(12,2)")), schema)) == List(ViolationType.OutputFieldTypeMismatch))
  }

  test("a bare 'decimal' does not match a column of a particular precision: declare decimal(p,s)") {
    val vs = check(List(CField("amt", "decimal")), Cols().add("amt", DecimalType(10, 2)))
    assert(types(vs) == List(ViolationType.OutputFieldTypeMismatch))
    assert(vs.head.expected.contains("decimal") && vs.head.actual.contains("decimal(10,2)"))
  }

  test("a plain (non-nested) declared type must be the contract vocabulary's own keyword: 'int' is not 'integer'") {
    // Only types written with '<' are parsed as Spark DDL; a plain keyword keeps the strict, existing comparison.
    val schema = Cols().add("id", IntegerType)
    val vs = check(List(CField("id", "int")), schema)
    assert(types(vs) == List(ViolationType.OutputFieldTypeMismatch))
    assert(vs.head.expected.contains("int") && vs.head.actual.contains("integer"))
  }

  test("input and output sides differ only in violation types and wording") {
    val schema = Cols().add("a", StringType)
    val fields = List(CField("a", "integer", required = true), CField("missing", "integer", required = true), CField("n", "string", nullable = false))
    val in = check(fields, schema.add("n", StringType), Side.Input, rejectUndeclared = true)
    val out = check(fields, schema.add("n", StringType), Side.Output, rejectUndeclared = true)
    assert(types(in) == List(ViolationType.InputFieldTypeMismatch, ViolationType.MissingInputField, ViolationType.InputFieldNullabilityMismatch))
    assert(types(out) == List(ViolationType.OutputFieldTypeMismatch, ViolationType.MissingOutputField, ViolationType.OutputFieldNullabilityMismatch))
    assert(in.forall(_.message.contains("INPUT")) && out.forall(_.message.contains("OUTPUT")))
    assert(in(1).remediation.contains("to the input") && out(1).remediation.contains("to the output"))
    assert(in(2).remediation.contains("before the input is produced") && out(2).remediation.contains("before the output is produced"))
    val undeclaredIn = check(Nil, Cols().add("z", StringType), Side.Input, rejectUndeclared = true)
    val undeclaredOut = check(Nil, Cols().add("z", StringType), Side.Output, rejectUndeclared = true)
    assert(types(undeclaredIn) == List(ViolationType.UndeclaredInputColumn) && types(undeclaredOut) == List(ViolationType.UndeclaredOutputColumn))
    assert(undeclaredIn.head.remediation.contains("transformation's input") && undeclaredOut.head.remediation.contains("transformation's output"))
  }

  test("a missing field is only flagged when required; nullability is one-directional") {
    val schema = Cols().add("a", StringType, nullable = true).add("b", StringType, nullable = false)
    assert(check(List(CField("gone", "string")), schema).isEmpty)
    assert(types(check(List(CField("gone", "string", required = true)), schema)) == List(ViolationType.MissingOutputField))
    assert(types(check(List(CField("a", "string", nullable = false)), schema)) == List(ViolationType.OutputFieldNullabilityMismatch))
    assert(check(List(CField("b", "string", nullable = true)), schema).isEmpty) // stricter than required is fine
  }

  test("undeclared columns are reported only under rejectUndeclaredFields, in schema order") {
    val schema = Cols().add("a", StringType).add("z", StringType).add("y", StringType)
    assert(check(List(CField("a", "string")), schema).isEmpty)
    assert(check(List(CField("a", "string")), schema, rejectUndeclared = true).flatMap(_.column) == List("z", "y"))
  }

  // --- case sensitivity -------------------------------------------------------

  test("case-insensitive (Spark's default): a differently-cased declared name matches the actual column") {
    val schema = Cols().add("id", IntegerType, nullable = false)
    assert(check(List(CField("ID", "integer", required = true, nullable = false)), schema, caseSensitive = false).isEmpty)
    assert(check(List(CField("ID", "integer", required = true, nullable = false)), schema, rejectUndeclared = true).isEmpty) // default is insensitive
  }

  test("case-insensitive: type and nullability are still checked on the case-matched column") {
    val schema = Cols().add("id", StringType)
    val vs = check(List(CField("ID", "integer", nullable = false)), schema)
    assert(types(vs) == List(ViolationType.OutputFieldTypeMismatch, ViolationType.OutputFieldNullabilityMismatch))
    assert(vs.forall(_.column.contains("ID"))) // reported under the contract's own spelling
  }

  test("case-sensitive: the same names are a missing field and an undeclared column") {
    val schema = Cols().add("id", IntegerType)
    val vs = check(List(CField("ID", "integer", required = true)), schema, rejectUndeclared = true, caseSensitive = true)
    assert(types(vs) == List(ViolationType.MissingOutputField, ViolationType.UndeclaredOutputColumn))
    assert(vs.flatMap(_.column) == List("ID", "id"))
  }

  test("case-sensitive: exact-case names still match") {
    val schema = Cols().add("ID", IntegerType)
    assert(check(List(CField("ID", "integer", required = true)), schema, rejectUndeclared = true, caseSensitive = true).isEmpty)
  }

  // --- nested: struct via `properties` ---------------------------------------

  private val address = Cols().add("zip", StringType, nullable = false).add("city", StringType)
  private val customer = Cols().add("id", IntegerType).add("address", address)

  private def addressField(props: CField*) = CField("address", "struct", properties = props.toList)

  test("struct properties: a matching nested schema is clean") {
    assert(check(List(addressField(CField("zip", "string", nullable = false), CField("city", "string"))), customer).isEmpty)
  }

  test("struct properties: a nested type mismatch is reported with the dotted path") {
    val vs = check(List(addressField(CField("zip", "integer"))), customer)
    assert(types(vs) == List(ViolationType.OutputFieldTypeMismatch))
    assert(vs.head.column.contains("address.zip"))
    assert(vs.head.message == "field 'address.zip' declares type 'integer' but the actual OUTPUT schema has type 'string'")
    assert(vs.head.remediation.startsWith("Cast 'address.zip' to 'integer'"))
  }

  test("struct properties: a missing required nested field, and a nested nullability mismatch") {
    val missing = check(List(addressField(CField("country", "string", required = true))), customer)
    assert(types(missing) == List(ViolationType.MissingOutputField) && missing.head.column.contains("address.country"))
    assert(missing.head.message == "required field 'address.country' is absent from the actual OUTPUT schema")
    assert(check(List(addressField(CField("country", "string"))), customer).isEmpty) // optional

    val nullable = check(List(addressField(CField("city", "string", nullable = false))), customer)
    assert(types(nullable) == List(ViolationType.OutputFieldNullabilityMismatch) && nullable.head.column.contains("address.city"))
  }

  test("struct properties: undeclared nested columns are reported under rejectUndeclaredFields, at every level") {
    val vs = check(List(CField("id", "integer"), addressField(CField("zip", "string"))), customer, rejectUndeclared = true)
    assert(types(vs) == List(ViolationType.UndeclaredOutputColumn))
    assert(vs.head.column.contains("address.city"))
    assert(check(List(CField("id", "integer"), addressField(CField("zip", "string"))), customer).isEmpty)

    val deeper = Cols().add("a", Cols().add("b", Cols().add("c", IntegerType).add("extra", IntegerType)))
    val declared = CField("a", "struct", properties = List(CField("b", "struct", properties = List(CField("c", "integer")))))
    assert(check(List(declared), deeper, rejectUndeclared = true).flatMap(_.column) == List("a.b.extra"))
  }

  test("struct properties: nested names follow the case rule too") {
    val vs = check(List(addressField(CField("ZIP", "string", required = true))), customer)
    assert(vs.isEmpty)
    val cs = check(List(CField("id", "integer"), addressField(CField("ZIP", "string", required = true))), customer, rejectUndeclared = true, caseSensitive = true)
    assert(cs.flatMap(_.column) == List("address.ZIP", "address.zip", "address.city"))
    assert(types(cs) == List(ViolationType.MissingOutputField, ViolationType.UndeclaredOutputColumn, ViolationType.UndeclaredOutputColumn))
  }

  test("struct properties: a declared struct whose actual column is not a struct is one type mismatch, with no recursion") {
    val schema = Cols().add("address", StringType)
    val vs = check(List(addressField(CField("zip", "string", required = true))), schema)
    assert(types(vs) == List(ViolationType.OutputFieldTypeMismatch))
    assert(vs.head.actual.contains("string"))
  }

  test("a struct with no properties stays a shallow check: any struct matches 'struct', and an array does not") {
    assert(check(List(CField("address", "struct")), customer).isEmpty)
    val vs = check(List(CField("id", "struct")), customer)
    assert(vs.head.actual.contains("integer"))
    val arr = check(List(CField("address", "array")), customer)
    assert(arr.head.actual.contains("struct<zip:string,city:string>")) // nested actual is described in full
  }

  // --- nested: DDL-typed array / map / struct ---------------------------------

  private val schema = Cols()
    .add("tags", ArrayType(StringType))
    .add("scores", MapType(StringType, IntegerType))
    .add("pt", Cols().add("x", IntegerType).add("y", IntegerType))
    .add("matrix", ArrayType(ArrayType(DoubleType)))

  test("DDL types: an exact structural match passes, whatever the inner nullability flags") {
    assert(check(List(CField("tags", "array<string>"), CField("scores", "map<string,int>"), CField("pt", "struct<x:int,y:int>"), CField("matrix", "array<array<double>>")), schema).isEmpty)
  }

  test("DDL types: a changed element, key/value or struct member type is a mismatch that names both full types") {
    val tags = check(List(CField("tags", "array<int>")), schema)
    assert(types(tags) == List(ViolationType.OutputFieldTypeMismatch))
    assert(tags.head.expected.contains("array<int>") && tags.head.actual.contains("array<string>"))
    assert(tags.head.message == "field 'tags' declares type 'array<int>' but the actual OUTPUT schema has type 'array<string>'")
    assert(types(check(List(CField("scores", "map<string,long>")), schema)) == List(ViolationType.OutputFieldTypeMismatch))
    assert(types(check(List(CField("scores", "map<int,int>")), schema)) == List(ViolationType.OutputFieldTypeMismatch))
    assert(types(check(List(CField("pt", "struct<x:int,y:string>")), schema)) == List(ViolationType.OutputFieldTypeMismatch))
    assert(types(check(List(CField("matrix", "array<array<int>>")), schema)) == List(ViolationType.OutputFieldTypeMismatch))
  }

  test("DDL types: struct member count and names matter; names follow the case rule") {
    assert(types(check(List(CField("pt", "struct<x:int>")), schema)) == List(ViolationType.OutputFieldTypeMismatch))
    assert(types(check(List(CField("pt", "struct<x:int,y:int,z:int>")), schema)) == List(ViolationType.OutputFieldTypeMismatch))
    assert(types(check(List(CField("pt", "struct<x:int,z:int>")), schema)) == List(ViolationType.OutputFieldTypeMismatch))
    assert(check(List(CField("pt", "struct<X:int,Y:int>")), schema).isEmpty)
    assert(types(check(List(CField("pt", "struct<X:int,Y:int>")), schema, caseSensitive = true)) == List(ViolationType.OutputFieldTypeMismatch))
  }

  test("DDL types: a container declared against a different kind of actual column is a mismatch") {
    assert(types(check(List(CField("tags", "map<string,string>")), schema)) == List(ViolationType.OutputFieldTypeMismatch))
    assert(types(check(List(CField("id", "array<int>")), Cols().add("id", IntegerType))) == List(ViolationType.OutputFieldTypeMismatch))
    val scalarActual = check(List(CField("id", "array<int>")), Cols().add("id", IntegerType))
    assert(scalarActual.head.actual.contains("integer")) // a scalar actual keeps the plain typeName
  }

  test("DDL types: an unparseable declared type is a mismatch, not an exception") {
    val vs = check(List(CField("tags", "array<")), schema)
    assert(types(vs) == List(ViolationType.OutputFieldTypeMismatch))
    assert(vs.head.actual.contains("array<string>"))
    assert(types(check(List(CField("tags", "array<nonsense>")), schema)) == List(ViolationType.OutputFieldTypeMismatch))
  }

  test("a bare 'array' / 'map' keeps its existing shallow meaning: any array, any map") {
    assert(check(List(CField("tags", "array"), CField("scores", "map"), CField("matrix", "array")), schema).isEmpty)
  }

  test("a DDL type's own field nullability is still checked at the field level") {
    val vs = check(List(CField("tags", "array<string>", nullable = false)), Cols().add("tags", ArrayType(StringType), nullable = true))
    assert(types(vs) == List(ViolationType.OutputFieldNullabilityMismatch))
  }

  test("sameShape: scalars compare by value, containers recurse, nullability flags are ignored") {
    assert(SchemaChecker.sameShape(IntegerType, IntegerType, caseSensitive = false))
    assert(!SchemaChecker.sameShape(IntegerType, LongType, caseSensitive = false))
    assert(SchemaChecker.sameShape(DecimalType(10, 2), DecimalType(10, 2), caseSensitive = true))
    assert(!SchemaChecker.sameShape(DecimalType(10, 2), DecimalType(10, 3), caseSensitive = true))
    assert(SchemaChecker.sameShape(ArrayType(IntegerType), ArrayType(IntegerType), caseSensitive = true))
    assert(!SchemaChecker.sameShape(ArrayType(IntegerType), ArrayType(LongType), caseSensitive = true))
    assert(SchemaChecker.sameShape(MapType(StringType, IntegerType), MapType(StringType, IntegerType), caseSensitive = true))
    assert(!SchemaChecker.sameShape(MapType(StringType, IntegerType), MapType(StringType, LongType), caseSensitive = true))
    assert(!SchemaChecker.sameShape(MapType(StringType, IntegerType), MapType(LongType, IntegerType), caseSensitive = true))
    assert(!SchemaChecker.sameShape(ArrayType(IntegerType), IntegerType, caseSensitive = true))
    val s1 = StructType(List(LogicalField("A", IntegerType, nullable = true)))
    val s2 = StructType(List(LogicalField("a", IntegerType, nullable = false)))
    assert(SchemaChecker.sameShape(s1, s2, caseSensitive = false))
    assert(!SchemaChecker.sameShape(s1, s2, caseSensitive = true))
    // A struct with a different number of fields, or a different field type, is a different shape.
    assert(!SchemaChecker.sameShape(s1, StructType(List(LogicalField("A", IntegerType), LogicalField("B", IntegerType))), caseSensitive = true))
    assert(!SchemaChecker.sameShape(s1, StructType(List(LogicalField("A", LongType))), caseSensitive = true))
  }

  // --- the checker needs no Spark at all ------------------------------------------------------
  // Everything above goes through `SparkSchemas`; these build the `LogicalSchema` an adapter for
  // any other engine would hand over, to prove the checker is engine-neutral rather than merely
  // reachable from Spark.

  test("a schema built directly from LogicalTypes is checked exactly as a Spark-derived one is") {
    val schema = LogicalSchema(
      List(
        LogicalField("id", LongType, nullable = false),
        LogicalField("amount", DecimalType(18, 2)),
        LogicalField("tags", ArrayType(StringType)),
        LogicalField("addr", StructType(List(LogicalField("zip", StringType))))
      )
    )
    assert(
      check(
        List(
          CField("id", "long", nullable = false),
          CField("amount", "decimal(18,2)"),
          CField("tags", "array<string>"),
          CField("addr", "struct<zip:string>")
        ),
        schema
      ).isEmpty
    )
    val vs = check(List(CField("id", "string"), CField("tags", "array<int>")), schema)
    assert(types(vs) == List(ViolationType.OutputFieldTypeMismatch, ViolationType.OutputFieldTypeMismatch))
    assert(vs.map(_.actual) == List(Some("long"), Some("array<string>")))
  }

  test("an engine type with no logical equivalent never satisfies a declared standard type") {
    val schema = LogicalSchema(List(LogicalField("area", OtherType("geography"))))
    assert(types(check(List(CField("area", "string")), schema)) == List(ViolationType.OutputFieldTypeMismatch))
    assert(check(List(CField("area", "geography")), schema).isEmpty) // its own native keyword still matches
  }

  test("undeclared columns and nested NOT NULL / COMMENT in a declared type behave as before") {
    val schema = LogicalSchema(List(LogicalField("p", StructType(List(LogicalField("x", IntegerType)))), LogicalField("extra", DoubleType)))
    val vs = check(List(CField("p", "struct<x:int NOT NULL COMMENT 'the x'>")), schema, rejectUndeclared = true)
    assert(types(vs) == List(ViolationType.UndeclaredOutputColumn))
    assert(vs.head.column.contains("extra") && vs.head.actual.contains("double"))
  }
}
