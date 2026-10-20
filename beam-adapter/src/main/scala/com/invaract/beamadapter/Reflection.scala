// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.beamadapter

/** Reading a member Beam does not publish (docs/BEAM_ADAPTER.md, D2): a field or a no-argument method declared on an object's class
  * or on one it extends, made accessible. This is the one place the adapter reaches past Beam's public API, so every such read
  * fails the same way: by throwing, which each caller turns into a step that fails closed (a write refused, a shape `Unreadable`),
  * never into a guess. The members it is used on are listed where it is used, and held to the Beam releases CI tests.
  */
private[beamadapter] object Reflection {

  /** The value of the field `name` of `o` (`null` when it holds none). Throws `NoSuchFieldException` when no class declares it. */
  def field(o: AnyRef, name: String): AnyRef = {
    val f = classesOf(o).flatMap(_.getDeclaredFields.find(_.getName == name)).toSeq.headOption
      .getOrElse(throw new NoSuchFieldException(s"${o.getClass.getName}.$name"))
    f.setAccessible(true)
    f.get(o)
  }

  /** What the no-argument method `name` of `o` returns (`null` when it returns none). Throws `NoSuchMethodException` when no class
    * declares it. */
  def call(o: AnyRef, name: String): AnyRef = {
    val m = classesOf(o).flatMap(_.getDeclaredMethods.find(m => m.getName == name && m.getParameterCount == 0)).toSeq.headOption
      .getOrElse(throw new NoSuchMethodException(s"${o.getClass.getName}.$name()"))
    m.setAccessible(true)
    m.invoke(o)
  }

  private def classesOf(o: AnyRef): Iterator[Class[_]] = Iterator.iterate[Class[_]](o.getClass)(_.getSuperclass).takeWhile(_ != null)
}
