// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.beamadapter

import com.invaract.contract.{LogicalField, LogicalSchema, LogicalType}
import org.apache.beam.sdk.coders.Coder
import org.apache.beam.sdk.schemas.Schema.{FieldType, TypeName}
import org.apache.beam.sdk.schemas.{Schema => BeamSchema}
import org.yaml.snakeyaml.Yaml

import scala.collection.JavaConverters._
import scala.util.Try

/** What a collection's columns are, in the contract's terms: from the Beam row schema when the collection has one, or from
  * the Avro schema its coder carries (a Parquet or Avro read yields Avro records). Both mappings follow
  * docs/ENGINE_RESEARCH_BIGQUERY_BEAM.md; a type with no neutral equivalent becomes `OtherType`, which equals only itself,
  * so a mismatch stays visible instead of being papered over by a lossy guess.
  *
  * The Avro schema is read from its JSON text (a JSON document is also a YAML one, and the contract module already reads
  * YAML), so this adapter does not need Avro on its own classpath: a job that never touches Avro is unaffected.
  */
private[beamadapter] object BeamSchemas {

  /** The columns of `collection`, or `None` when it exposes none (a collection of plain Java objects). */
  def of(collection: BeamCollection): Option[LogicalSchema] =
    collection.rowSchema.map(fromBeam).orElse(collection.coder.flatMap(fromCoder))

  def fromBeam(schema: BeamSchema): LogicalSchema =
    LogicalSchema(schema.getFields.asScala.toList.map(f => LogicalField(f.getName, beamType(f.getType), f.getType.getNullable)))

  private def beamType(t: FieldType): LogicalType = t.getTypeName match {
    case TypeName.BYTE     => LogicalType.ByteType
    case TypeName.INT16    => LogicalType.ShortType
    case TypeName.INT32    => LogicalType.IntegerType
    case TypeName.INT64    => LogicalType.LongType
    case TypeName.FLOAT    => LogicalType.FloatType
    case TypeName.DOUBLE   => LogicalType.DoubleType
    case TypeName.STRING   => LogicalType.StringType
    case TypeName.BOOLEAN  => LogicalType.BooleanType
    case TypeName.BYTES    => LogicalType.BinaryType
    // a Beam DECIMAL carries no precision or scale, so no neutral decimal(p,s) can be claimed for it
    case TypeName.DECIMAL  => LogicalType.OtherType("decimal")
    // Beam's primitive DATETIME is an instant (a Joda DateTime), so it is a timestamp, not a local date-time
    case TypeName.DATETIME => LogicalType.TimestampType
    case TypeName.ARRAY | TypeName.ITERABLE => LogicalType.ArrayType(beamType(t.getCollectionElementType))
    case TypeName.MAP      => LogicalType.MapType(beamType(t.getMapKeyType), beamType(t.getMapValueType))
    case TypeName.ROW      => LogicalType.StructType(fromBeam(t.getRowSchema).fields)
    case TypeName.LOGICAL_TYPE => logicalType(t.getLogicalType.getIdentifier)
    case other             => LogicalType.OtherType(other.name().toLowerCase)
  }

  /** Beam's SQL logical types by identifier (`beam:logical_type:<name>:v1`); anything else keeps its identifier. */
  private def logicalType(identifier: String): LogicalType = {
    val name = identifier.toLowerCase
    if (name.contains("datetime")) LogicalType.TimestampNtzType
    else if (name.contains("timestamp")) LogicalType.TimestampType
    else if (name.contains("date")) LogicalType.DateType
    else if (name.contains("time")) LogicalType.TimeType
    else if (name.contains("bytes")) LogicalType.BinaryType
    else LogicalType.OtherType(identifier)
  }

  /** The Avro schema `coder` carries, as the contract's columns. An Avro coder exposes it as `getSchema` (an
    * `org.apache.avro.Schema`, read here through its JSON form, so no Avro class is needed by this module). */
  private def fromCoder(coder: Coder[_]): Option[LogicalSchema] =
    Try {
      val method = coder.getClass.getMethods.find(m => m.getName == "getSchema" && m.getParameterCount == 0)
      method.flatMap(m => Option(m.invoke(coder))).map(_.toString)
    }.toOption.flatten.flatMap(fromAvroJson)

  /** The columns of an Avro record schema given as JSON, or `None` when it is not a record. */
  def fromAvroJson(json: String): Option[LogicalSchema] =
    Try(new Yaml().load[AnyRef](json)).toOption.flatMap {
      case record: java.util.Map[_, _] if record.get("type") == "record" => Some(recordSchema(record.asInstanceOf[java.util.Map[String, AnyRef]]))
      case _                                                             => None
    }

  private def recordSchema(record: java.util.Map[String, AnyRef]): LogicalSchema =
    LogicalSchema(record.get("fields").asInstanceOf[java.util.List[java.util.Map[String, AnyRef]]].asScala.toList.map { f =>
      val (dataType, nullable) = avroType(f.get("type"))
      LogicalField(f.get("name").toString, dataType, nullable)
    })

  /** `(type, nullable)` of an Avro type: a union with `null` is that type, nullable; anything else is not nullable. */
  private def avroType(avro: AnyRef): (LogicalType, Boolean) = avro match {
    case name: String => (primitive(name), name == "null")
    case union: java.util.List[_] =>
      val branches = union.asScala.toList.map(_.asInstanceOf[AnyRef])
      val nonNull = branches.filterNot(_ == "null")
      val nullable = nonNull.size != branches.size
      nonNull match {
        case single :: Nil => (avroType(single)._1, nullable)
        case _             => (LogicalType.OtherType("union"), nullable)
      }
    case complex: java.util.Map[_, _] => (complexType(complex.asInstanceOf[java.util.Map[String, AnyRef]]), false)
    case _                            => (LogicalType.OtherType("unknown"), false)
  }

  private def primitive(name: String): LogicalType = name match {
    case "boolean" => LogicalType.BooleanType
    case "int"     => LogicalType.IntegerType
    case "long"    => LogicalType.LongType
    case "float"   => LogicalType.FloatType
    case "double"  => LogicalType.DoubleType
    case "bytes"   => LogicalType.BinaryType
    case "string"  => LogicalType.StringType
    // a bare name that is not a primitive is a reference to a named type defined elsewhere in the schema
    case other     => LogicalType.OtherType(other)
  }

  private def complexType(t: java.util.Map[String, AnyRef]): LogicalType = {
    def asInt(key: String, default: Int): Int = Option(t.get(key)).map(_.toString.toInt).getOrElse(default)
    (t.get("type"), Option(t.get("logicalType")).map(_.toString)) match {
      case ("record", _) => LogicalType.StructType(recordSchema(t).fields)
      case ("array", _)  => LogicalType.ArrayType(avroType(t.get("items"))._1)
      case ("map", _)    => LogicalType.MapType(LogicalType.StringType, avroType(t.get("values"))._1)
      case ("enum", _)   => LogicalType.StringType
      case (_, Some("decimal"))                                             => LogicalType.DecimalType(asInt("precision", 38), asInt("scale", 0))
      case (_, Some("date"))                                                => LogicalType.DateType
      case (_, Some("time-millis" | "time-micros"))                         => LogicalType.TimeType
      case (_, Some("timestamp-millis" | "timestamp-micros"))               => LogicalType.TimestampType
      case (_, Some("local-timestamp-millis" | "local-timestamp-micros"))   => LogicalType.TimestampNtzType
      case (_, Some("uuid"))                                                => LogicalType.StringType
      case ("fixed", _)                                                     => LogicalType.BinaryType
      // `{"type": "long"}` and the like: a primitive written as an object
      case (name: String, _) => primitive(name)
      case _                 => LogicalType.OtherType("unknown")
    }
  }
}
