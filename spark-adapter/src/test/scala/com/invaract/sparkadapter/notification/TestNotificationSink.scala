// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter.notification

import scala.collection.mutable

/** An in-memory `NotificationSink` for tests: records every published event,
  * in order, so a test can assert on exactly what was published without
  * standing up a real destination (a file, a network endpoint). Shared by
  * `ContractEnforcementRuleSpec` and `SparkAdapterListenerSpec` rather than
  * duplicated in each.
  *
  * `publish` and `events` are synchronized on `this`: `publish` can run on
  * Spark's own listener-bus thread (`SparkAdapterListener.onSuccess`) while
  * a test's main thread concurrently polls `events` via ScalaTest's
  * `eventually` - a plain, unsynchronized `mutable.ListBuffer` gives no
  * memory-visibility guarantee across that thread boundary, so a write on
  * one thread isn't guaranteed to become visible to a read on the other
  * without one.
  */
private[sparkadapter] class TestNotificationSink extends NotificationSink {
  private val _events = mutable.ListBuffer.empty[NotificationEvent]

  def events: List[NotificationEvent] = synchronized(_events.toList)

  def clear(): Unit = synchronized(_events.clear())

  override def publish(event: NotificationEvent): Unit = synchronized(_events += event)
}
