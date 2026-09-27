// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.time.{Seconds, Span}

import java.nio.file.{Files, Path}

class CheckpointLineageTrackerSpec extends AnyFunSuite with BeforeAndAfterAll {
  private var spark: SparkSession = _
  private var dataDir: Path = _
  private var ordersCsv: Path = _
  private var customersCsv: Path = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder().master("local[*]").appName("CheckpointLineageTrackerSpec")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    spark.sparkContext.setCheckpointDir(Files.createTempDirectory("invaract-checkpoint-tracker-checkpoints").toString)

    dataDir = Files.createTempDirectory("invaract-checkpoint-tracker-test")
    ordersCsv = dataDir.resolve("orders.csv")
    Files.write(ordersCsv, "order_id,customer_id,amount\n1,a,100\n2,b,200\n".getBytes("UTF-8"))
    customersCsv = dataDir.resolve("customers.csv")
    Files.write(customersCsv, "customer_id,name,state\na,Alice,NY\nb,Bob,CA\n".getBytes("UTF-8"))
  }

  override def afterAll(): Unit = spark.stop()

  private def readCsv(path: Path) =
    spark.read.option("header", "true").option("inferSchema", "true").csv(path.toString)

  test("a real .checkpoint() of a single read is observed, by its real location") {
    val tracker = new CheckpointLineageTracker
    spark.listenerManager.register(tracker)
    try {
      readCsv(ordersCsv).checkpoint(true)

      // onSuccess fires on Spark's own asynchronous listener thread - see
      // SparkPlanAdapterSpec's identical pattern for SparkAdapterListener.
      eventually(timeout(Span(5, Seconds))) {
        assert(
          tracker.observedLocations.exists(_.contains("orders.csv")),
          s"expected orders.csv among observed locations, got: ${tracker.observedLocations}"
        )
      }
    } finally {
      spark.listenerManager.unregister(tracker)
    }
  }

  test("a real .checkpoint() following a multi-input join with a column-dropping select observes BOTH inputs' real locations") {
    // The exact motivating scenario: the checkpoint's own OUTPUT columns
    // (order_id, name) cover neither input's full declared field set once
    // customer_id/amount/state are projected away - this is what
    // StructuralVerifier's column-overlap heuristic alone can't see through,
    // and what this tracker exists to fix by capturing the PRE-checkpoint
    // plan's real Read nodes instead of guessing from post-checkpoint
    // columns.
    val tracker = new CheckpointLineageTracker
    spark.listenerManager.register(tracker)
    try {
      val orders = readCsv(ordersCsv)
      val customers = readCsv(customersCsv)
      val joined = orders.join(customers, "customer_id").select(orders("order_id"), customers("name"))
      joined.checkpoint(true)

      eventually(timeout(Span(5, Seconds))) {
        assert(
          tracker.observedLocations.exists(_.contains("orders.csv")),
          s"expected orders.csv among observed locations, got: ${tracker.observedLocations}"
        )
        assert(
          tracker.observedLocations.exists(_.contains("customers.csv")),
          s"expected customers.csv among observed locations, got: ${tracker.observedLocations}"
        )
      }
    } finally {
      spark.listenerManager.unregister(tracker)
    }
  }

  test(".localCheckpoint() is observed the same way .checkpoint() is") {
    val tracker = new CheckpointLineageTracker
    spark.listenerManager.register(tracker)
    try {
      readCsv(customersCsv).localCheckpoint(true)

      eventually(timeout(Span(5, Seconds))) {
        assert(
          tracker.observedLocations.exists(_.contains("customers.csv")),
          s"expected customers.csv among observed locations, got: ${tracker.observedLocations}"
        )
      }
    } finally {
      spark.listenerManager.unregister(tracker)
    }
  }

  test("a non-checkpoint action (.count()) is not observed - the tracker only reacts to checkpoint/localCheckpoint") {
    val tracker = new CheckpointLineageTracker
    spark.listenerManager.register(tracker)
    try {
      readCsv(ordersCsv).count()
      // Give the async listener thread the same grace period the positive
      // tests give a real success, before asserting the negative.
      Thread.sleep(500)
      assert(tracker.observedLocations.isEmpty, s"tracker captured a non-checkpoint action: ${tracker.observedLocations}")
    } finally {
      spark.listenerManager.unregister(tracker)
    }
  }

  test("observedLocations accumulates across multiple checkpoints in the same session, rather than only keeping the most recent one") {
    val tracker = new CheckpointLineageTracker
    spark.listenerManager.register(tracker)
    try {
      readCsv(ordersCsv).checkpoint(true)
      readCsv(customersCsv).checkpoint(true)

      eventually(timeout(Span(5, Seconds))) {
        assert(tracker.observedLocations.exists(_.contains("orders.csv")))
        assert(tracker.observedLocations.exists(_.contains("customers.csv")))
      }
    } finally {
      spark.listenerManager.unregister(tracker)
    }
  }

  test("onFailure is a no-op - a failed checkpoint attempt never gets recorded as observed") {
    val tracker = new CheckpointLineageTracker
    // onFailure's contract is "does nothing"; directly exercised since a
    // real checkpoint failure is impractical to force deterministically in
    // a unit test.
    tracker.onFailure("checkpoint", readCsv(ordersCsv).queryExecution, new RuntimeException("simulated"))
    assert(tracker.observedLocations.isEmpty)
  }
}
