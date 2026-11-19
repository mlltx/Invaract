// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import org.apache.logging.log4j.{Level, LogManager}
import org.apache.logging.log4j.core.{LogEvent, LoggerContext}
import org.apache.logging.log4j.core.appender.AbstractAppender
import org.apache.logging.log4j.core.config.{LoggerConfig, Property}

/** Captures what one logger logs at `level` or worse while `body` runs. The suites raise Spark's log level, so a
  * logger config for just that logger is installed for the duration and removed afterwards, rather than relying on
  * the ambient level.
  */
object LogCapture {
  def messages[T](loggerName: String, level: Level)(body: => T): (T, List[String]) = {
    val captured = new java.util.concurrent.CopyOnWriteArrayList[String]()
    val context = LogManager.getContext(false).asInstanceOf[LoggerContext]
    val config = context.getConfiguration
    val appender = new AbstractAppender(s"capture-$loggerName", null, null, true, Property.EMPTY_ARRAY) {
      override def append(event: LogEvent): Unit = captured.add(event.getMessage.getFormattedMessage)
    }
    appender.start()
    val loggerConfig = new LoggerConfig(loggerName, level, false)
    loggerConfig.addAppender(appender, null, null)
    config.addLogger(loggerName, loggerConfig)
    context.updateLoggers()
    try {
      val result = body
      (result, captured.toArray.toList.map(_.toString))
    } finally {
      config.removeLogger(loggerName)
      appender.stop()
      context.updateLoggers()
    }
  }

  /** The name `ContractEnforcementRule` logs under. */
  val EnforcementRule: String = ContractEnforcementRule.getClass.getName
}
