// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.verification.notification

import com.invaract.contract.ReflectivePluginResolver

/** Builds the `NotificationSink` a `NotificationConfig` describes. */
object NotificationSinkFactory {

  /** `None` when `config.enabled` is `false` — the common case: notification
    * is off by default, imposing zero behavior change on a caller that
    * never sets up a config file. When enabled, instantiates
    * `config.sinkClassName` reflectively (a public no-arg constructor is
    * required), calls its `configure(config.properties)` once, and wraps
    * it in `SafeNotificationSink` before returning it.
    *
    * Construction failures — a missing/misspelled class, a sink lacking a
    * no-arg constructor, `configure` rejecting its properties (e.g.
    * `FileNotificationSink` with no `path`) — throw immediately, the same
    * "fail loudly, at setup time" treatment `ContractParser.parseFile`
    * gives a malformed contract: a typo'd sink class should be caught the
    * moment the job starts, not silently produce zero events forever.
    * Once built, though, every `publish` call is protected by
    * `SafeNotificationSink` — a *runtime* publish failure (a network
    * sink's connection dropping mid-job) must never abort a legitimate
    * contract check or write, since notification is a best-effort side
    * channel, not part of enforcement.
    */
  def create(config: NotificationConfig): Option[NotificationSink] =
    if (!config.enabled) {
      None
    } else {
      val className = config.sinkClassName.getOrElse(
        throw new IllegalArgumentException("Notification config has sink.enabled=true but no sink.class")
      )
      val sink = instantiate(className)
      sink.configure(config.properties)
      Some(new SafeNotificationSink(sink))
    }

  /** A new, unconfigured instance of the sink class `className` (it needs a public no-arg
    * constructor). Shared by `create` and by sinks that build a sink of their own from
    * configuration, such as `HttpNotificationSink`'s dead letter.
    */
  def instantiate(className: String): NotificationSink =
    // Uncached on purpose: a sink is stateful, so each caller gets its own.
    ReflectivePluginResolver.instantiate[NotificationSink](className)
}
