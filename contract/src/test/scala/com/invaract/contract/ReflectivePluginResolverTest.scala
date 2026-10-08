// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.contract

import org.scalatest.funsuite.AnyFunSuite

import scala.util.{Failure, Success}

/** The plugin contract used by these tests. */
trait ResolverTestPlugin

/** Records which of the classes below ran any code. They are named by string, never referenced, so
  * nothing but the resolver can cause initialization or construction.
  */
object ResolverFlags {
  @volatile var staticInitRan = false
  @volatile var constructorRan = false
  @volatile var pluginConstructed = 0
}

/** Not a plugin, with a static initializer that would record itself if the class were initialized. */
object NotAPluginWithStaticInit { ResolverFlags.staticInitRan = true }

/** Not a plugin, with a constructor that would record itself if the class were instantiated. */
class NotAPluginWithConstructor { ResolverFlags.constructorRan = true }

class GoodResolverTestPlugin extends ResolverTestPlugin { ResolverFlags.pluginConstructed += 1 }

abstract class AbstractResolverTestPlugin extends ResolverTestPlugin

class NoDefaultConstructorPlugin(val x: Int) extends ResolverTestPlugin

class ReflectivePluginResolverTest extends AnyFunSuite {

  private val pkg = "com.invaract.contract"
  private def resolver = new ReflectivePluginResolver[ResolverTestPlugin]

  test("a class that is not a plugin is rejected without its static initializer running") {
    ResolverFlags.staticInitRan = false
    val e = intercept[IllegalArgumentException](resolver.resolve(s"$pkg.NotAPluginWithStaticInit$$"))
    assert(e.getMessage.contains("does not implement ResolverTestPlugin"))
    assert(!ResolverFlags.staticInitRan, "naming a class must not initialize it")
  }

  test("a class that is not a plugin is rejected without its constructor running") {
    ResolverFlags.constructorRan = false
    val e = intercept[IllegalArgumentException](resolver.resolve(s"$pkg.NotAPluginWithConstructor"))
    assert(e.getMessage.contains("does not implement ResolverTestPlugin"))
    assert(!ResolverFlags.constructorRan, "a class that is not a plugin must not be constructed")
  }

  test("a genuine plugin is constructed once and the instance is cached") {
    ResolverFlags.pluginConstructed = 0
    val r = resolver
    val first = r.resolve(s"$pkg.GoodResolverTestPlugin")
    val second = r.resolve(s"$pkg.GoodResolverTestPlugin")
    assert(first.isInstanceOf[GoodResolverTestPlugin])
    assert(first eq second)
    assert(ResolverFlags.pluginConstructed == 1)
  }

  test("a missing class, an abstract class and a class with no no-arg constructor are all 'Could not instantiate'") {
    List(s"$pkg.DoesNotExist", s"$pkg.AbstractResolverTestPlugin", s"$pkg.NoDefaultConstructorPlugin").foreach { name =>
      val e = intercept[IllegalArgumentException](resolver.resolve(name))
      assert(e.getMessage.contains("Could not instantiate"), name)
      assert(e.getMessage.contains(name), name)
      assert(e.getCause != null, name)
    }
  }

  test("a failed resolution is not cached: the same name resolves once the class is acceptable") {
    val r = resolver
    intercept[IllegalArgumentException](r.resolve(s"$pkg.NotAPluginWithConstructor"))
    intercept[IllegalArgumentException](r.resolve(s"$pkg.NotAPluginWithConstructor"))
    assert(r.tryResolve(s"$pkg.GoodResolverTestPlugin").isSuccess)
  }

  test("tryResolve wraps both outcomes instead of throwing") {
    val r = resolver
    assert(r.tryResolve(s"$pkg.GoodResolverTestPlugin").isInstanceOf[Success[_]])
    assert(r.tryResolve(s"$pkg.NotAPluginWithConstructor").isInstanceOf[Failure[_]])
    assert(r.tryResolve(s"$pkg.DoesNotExist").isInstanceOf[Failure[_]])
  }
}
