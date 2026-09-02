// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter.notification

import org.scalatest.funsuite.AnyFunSuite

/** A sink with a public no-arg constructor, real enough to instantiate
  * reflectively - records `configure`'s properties and every published
  * event so a test can assert `NotificationSinkFactory.create` really
  * wired both through. State lives on the companion object, not the
  * instance: `NotificationSinkFactory.create` constructs this reflectively
  * and hands back only a `SafeNotificationSink` wrapper, so a test has no
  * other way to reach the instance it actually built.
  */
object RecordingTestSink {
  @volatile var lastConfigured: Map[String, String] = Map.empty
  @volatile var published: List[NotificationEvent] = Nil
}

class RecordingTestSink extends NotificationSink {
  override def configure(properties: Map[String, String]): Unit = RecordingTestSink.lastConfigured = properties
  override def publish(event: NotificationEvent): Unit = RecordingTestSink.published = RecordingTestSink.published :+ event
}

/** Always throws from `publish` - used to prove `SafeNotificationSink`
  * (the wrapper every `NotificationSinkFactory.create` result carries)
  * actually swallows a publish-time failure rather than letting it
  * propagate.
  */
class ThrowingTestSink extends NotificationSink {
  override def publish(event: NotificationEvent): Unit = throw new RuntimeException("boom")
}

/** No public no-arg constructor - `NotificationSinkFactory.create` must
  * reject this at construction time with a clear error, not an opaque
  * reflection exception.
  */
class NoArgConstructorTestSink(unused: String) extends NotificationSink {
  override def publish(event: NotificationEvent): Unit = ()
}

/** Does not implement `NotificationSink` at all - proves the factory's own
  * `ClassCastException` handling, distinct from a missing-constructor
  * failure.
  */
class NotASinkAtAll

class NotificationSinkFactorySpec extends AnyFunSuite {

  private val sampleEvent = ContractValidationEvent(
    contract = "demo@1.0.0",
    status = "PASSED",
    violations = Nil,
    timestamp = 0L,
    metadata = Map.empty
  )

  test("create returns None when config.enabled is false, regardless of sinkClassName") {
    val config = NotificationConfig(enabled = false, sinkClassName = Some(classOf[RecordingTestSink].getName), properties = Map("x" -> "y"))
    assert(NotificationSinkFactory.create(config).isEmpty)
  }

  test("create throws when enabled but sinkClassName is missing") {
    val config = NotificationConfig(enabled = true, sinkClassName = None, properties = Map.empty)
    assertThrows[IllegalArgumentException] {
      NotificationSinkFactory.create(config)
    }
  }

  test("create instantiates the named class, calls configure once with the given properties, and returns Some") {
    RecordingTestSink.lastConfigured = Map.empty
    RecordingTestSink.published = Nil

    val config = NotificationConfig(
      enabled = true,
      sinkClassName = Some(classOf[RecordingTestSink].getName),
      properties = Map("path" -> "/tmp/x")
    )
    val sink = NotificationSinkFactory.create(config).getOrElse(fail("expected Some(sink)"))

    assert(RecordingTestSink.lastConfigured == Map("path" -> "/tmp/x"), "configure must run at construction time, before any publish")

    sink.publish(sampleEvent)
    assert(RecordingTestSink.published == List(sampleEvent))
  }

  test("create wraps the sink in SafeNotificationSink: a publish that throws does not propagate") {
    val config = NotificationConfig(enabled = true, sinkClassName = Some(classOf[ThrowingTestSink].getName), properties = Map.empty)
    val sink = NotificationSinkFactory.create(config).getOrElse(fail("expected Some(sink)"))
    sink.publish(sampleEvent) // must not throw, despite ThrowingTestSink always throwing
  }

  test("create throws IllegalArgumentException (not a raw ReflectiveOperationException) for a class with no no-arg constructor") {
    val config = NotificationConfig(enabled = true, sinkClassName = Some(classOf[NoArgConstructorTestSink].getName), properties = Map.empty)
    val ex = intercept[IllegalArgumentException] {
      NotificationSinkFactory.create(config)
    }
    assert(ex.getCause.isInstanceOf[ReflectiveOperationException])
  }

  test("create throws IllegalArgumentException for a class that does not implement NotificationSink") {
    val config = NotificationConfig(enabled = true, sinkClassName = Some(classOf[NotASinkAtAll].getName), properties = Map.empty)
    val ex = intercept[IllegalArgumentException] {
      NotificationSinkFactory.create(config)
    }
    assert(ex.getCause.isInstanceOf[ClassCastException])
  }

  test("create throws IllegalArgumentException for an unresolvable class name") {
    val config = NotificationConfig(enabled = true, sinkClassName = Some("com.invaract.DoesNotExist"), properties = Map.empty)
    val ex = intercept[IllegalArgumentException] {
      NotificationSinkFactory.create(config)
    }
    assert(ex.getCause.isInstanceOf[ReflectiveOperationException])
  }
}
