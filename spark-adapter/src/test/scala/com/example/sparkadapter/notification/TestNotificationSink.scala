// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.example.sparkadapter.notification

import scala.collection.mutable

/** An in-memory `NotificationSink` for tests: records every published event,
  * in order, so a test can assert on exactly what was published without
  * standing up a real destination (a file, a network endpoint). Shared by
  * `ContractEnforcementRuleSpec` and `SparkAdapterListenerSpec` rather than
  * duplicated in each.
  */
private[sparkadapter] class TestNotificationSink extends NotificationSink {
  private val _events = mutable.ListBuffer.empty[NotificationEvent]

  def events: List[NotificationEvent] = _events.toList

  def clear(): Unit = _events.clear()

  override def publish(event: NotificationEvent): Unit = _events += event
}
