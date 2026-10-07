// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import org.scalatest.funsuite.AnyFunSuite

class CustomRuleVerifierFactoryTest extends AnyFunSuite {

  test("resolve builds a real instance of a class implementing CustomRuleVerifier") {
    val verifier = CustomRuleVerifierFactory.resolve(classOf[ForbidPasswordColumnUpdateVerifier].getName)
    assert(verifier.isInstanceOf[ForbidPasswordColumnUpdateVerifier])
  }

  test("resolve returns the same cached instance for the same class name") {
    val className = classOf[ForbidPasswordColumnUpdateVerifier].getName
    assert(CustomRuleVerifierFactory.resolve(className) eq CustomRuleVerifierFactory.resolve(className))
  }

  test("resolve throws IllegalArgumentException for a class that doesn't exist") {
    val e = intercept[IllegalArgumentException] {
      CustomRuleVerifierFactory.resolve("com.invaract.sparkadapter.NoSuchClassAtAll")
    }
    assert(e.getMessage.contains("Could not instantiate"))
  }

  test("resolve throws IllegalArgumentException for a class without a public no-arg constructor") {
    val e = intercept[IllegalArgumentException] {
      CustomRuleVerifierFactory.resolve(classOf[NoNoArgConstructorCustomRuleVerifier].getName)
    }
    assert(e.getMessage.contains("it needs a public no-arg constructor"))
  }

  test("resolve throws IllegalArgumentException for a class that doesn't implement CustomRuleVerifier") {
    val e = intercept[IllegalArgumentException] {
      CustomRuleVerifierFactory.resolve(classOf[NotACustomRuleVerifier].getName)
    }
    assert(e.getMessage.contains("does not implement CustomRuleVerifier"))
  }

  test("tryResolve wraps a successful resolution in Success") {
    val result = CustomRuleVerifierFactory.tryResolve(classOf[ForbidPasswordColumnUpdateVerifier].getName)
    assert(result.isSuccess)
  }

  test("tryResolve wraps a failed resolution in Failure, without throwing") {
    val result = CustomRuleVerifierFactory.tryResolve("com.invaract.sparkadapter.NoSuchClassAtAll")
    assert(result.isFailure)
  }
}
