// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.verification.notification.kafka

import org.apache.kafka.common.compress.Compression
import org.apache.kafka.common.record.{MemoryRecords, SimpleRecord}
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.JavaConverters._

/** Guards the `at.yawk.lz4:lz4-java` pin in this module's `build.sbt`
  * (`dependencyOverrides`): `kafka-clients` declares lz4-java as a runtime
  * dependency at an older, advisory-affected version, and the pin replaces
  * it with a newer one. Nothing else in this module's tests ever reaches the
  * LZ4 codec (the sink only hands records to a `MockProducer`), so without
  * this a pin to a version `kafka-clients` cannot actually use would pass
  * every test and only fail for a user who sets `compression.type=lz4`.
  *
  * This builds a real LZ4-compressed record batch with Kafka's own
  * `MemoryRecords` — the same codec path (`net.jpountz.lz4`'s compressor and
  * decompressor, plus the XXHash frame checksums) a real producer and
  * consumer use — and reads it back.
  */
class KafkaLz4CodecSpec extends AnyFunSuite {

  private def roundTrip(payloads: Seq[String]): Seq[String] = {
    val batch = MemoryRecords.withRecords(
      Compression.lz4().build(),
      payloads.map(p => new SimpleRecord(p.getBytes("UTF-8"))): _*
    )
    batch.records().asScala.toSeq.map { r =>
      val bytes = new Array[Byte](r.value().remaining())
      r.value().get(bytes)
      new String(bytes, "UTF-8")
    }
  }

  test("a record batch compressed with Kafka's LZ4 codec reads back unchanged") {
    val payloads = Seq("alpha", "beta", "gamma")
    assert(roundTrip(payloads) == payloads)
  }

  test("the batch is really LZ4-compressed, not stored uncompressed") {
    val big = "invaract-notification-" * 500
    val batch = MemoryRecords.withRecords(Compression.lz4().build(), new SimpleRecord(big.getBytes("UTF-8")))
    assert(batch.batches().iterator().next().compressionType().name == "lz4")
    assert(batch.sizeInBytes() < big.length / 4, s"expected a much smaller batch, got ${batch.sizeInBytes()} bytes")
  }

  test("a large, repetitive payload round-trips through compression and decompression") {
    val big = "invaract-notification-" * 5000
    assert(roundTrip(Seq(big)) == Seq(big))
  }
}
