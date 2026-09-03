// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter.notification

import java.io.FileInputStream
import java.util.{Properties => JProperties}

import scala.collection.JavaConverters._

/** A notification sink's configuration, as loaded from a plain Java
  * `.properties` file — deliberately not YAML, and deliberately not part
  * of the contract document itself: unlike a contract (which describes
  * data shape and is meant to be shared/reviewed alongside the
  * transformation it governs), sink configuration is a deployment-
  * environment concern — an endpoint, a file path, possibly credentials —
  * that varies per environment a contract does not, and that a `.properties`
  * file needs no new library dependency to read (`java.util.Properties`
  * is part of the JDK; see CLAUDE.md's dependency discipline — `contract`'s
  * own YAML parsing pulls in SnakeYAML, which this module has no direct
  * dependency on).
  *
  * Recognized keys:
  *   - `sink.enabled` (`true`/`false`, default `false`) — the master
  *     switch; every other key is ignored when this is off.
  *   - `sink.class` — the fully-qualified class name of a `NotificationSink`
  *     implementation with a public no-arg constructor. Required when
  *     `sink.enabled=true`.
  *   - `sink.property.<name>` — passed to that sink's `configure` as
  *     `<name> -> value`, with the `sink.property.` prefix stripped. Which
  *     names a given sink recognizes is up to that sink (e.g.
  *     `FileNotificationSink` requires `sink.property.path`).
  *
  * See docs-site's "Notification sinks" guide for a full worked example.
  */
case class NotificationConfig(enabled: Boolean, sinkClassName: Option[String], properties: Map[String, String])

object NotificationConfig {

  /** The configuration a caller gets by simply never pointing at a
    * notification config file at all — no sink, no behavior change. This
    * is the default: notification is entirely opt-in.
    */
  val disabled: NotificationConfig = NotificationConfig(enabled = false, sinkClassName = None, properties = Map.empty)

  private val PropertyPrefix = "sink.property."

  /** Reads `path` as a `.properties` file. Throws if the file doesn't exist
    * or isn't readable — the same "fail loudly, at setup time" treatment
    * `ContractParser.parseFile` gives a malformed contract, since silently
    * falling back to `disabled` would leave a typo'd path producing zero
    * events forever with no indication why.
    */
  def load(path: String): NotificationConfig = {
    val props = new JProperties()
    val in = new FileInputStream(path)
    try {
      props.load(in)
    } finally {
      in.close()
    }

    val enabled = props.getProperty("sink.enabled", "false").trim.equalsIgnoreCase("true")
    val sinkClassName = Option(props.getProperty("sink.class")).map(_.trim).filter(_.nonEmpty)
    val sinkProperties = props
      .stringPropertyNames()
      .asScala
      .collect {
        case key if key.startsWith(PropertyPrefix) =>
          key.stripPrefix(PropertyPrefix) -> props.getProperty(key)
      }
      .toMap

    NotificationConfig(enabled, sinkClassName, sinkProperties)
  }
}
