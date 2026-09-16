// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.contract

import org.scalatest.funsuite.AnyFunSuite

class CustomPolicyEvaluatorFactoryTest extends AnyFunSuite {

  test("resolve builds a real instance of a class implementing CustomPolicyEvaluator") {
    val evaluator = CustomPolicyEvaluatorFactory.resolve(classOf[ContractIdMustBeLowercaseEvaluator].getName)
    assert(evaluator.isInstanceOf[ContractIdMustBeLowercaseEvaluator])
  }

  test("resolve returns the same cached instance for the same class name") {
    val className = classOf[ContractIdMustBeLowercaseEvaluator].getName
    assert(CustomPolicyEvaluatorFactory.resolve(className) eq CustomPolicyEvaluatorFactory.resolve(className))
  }

  test("resolve throws IllegalArgumentException for a class that doesn't exist") {
    val e = intercept[IllegalArgumentException] {
      CustomPolicyEvaluatorFactory.resolve("com.invaract.contract.NoSuchClassAtAll")
    }
    assert(e.getMessage.contains("Could not instantiate"))
  }

  test("resolve throws IllegalArgumentException for a class without a public no-arg constructor") {
    val e = intercept[IllegalArgumentException] {
      CustomPolicyEvaluatorFactory.resolve(classOf[NoNoArgConstructorCustomPolicyEvaluator].getName)
    }
    assert(e.getMessage.contains("it needs a public no-arg constructor"))
  }

  test("resolve throws IllegalArgumentException for a class that doesn't implement CustomPolicyEvaluator") {
    val e = intercept[IllegalArgumentException] {
      CustomPolicyEvaluatorFactory.resolve(classOf[NotACustomPolicyEvaluator].getName)
    }
    assert(e.getMessage.contains("does not implement CustomPolicyEvaluator"))
  }

  test("tryResolve wraps a successful resolution in Success") {
    val result = CustomPolicyEvaluatorFactory.tryResolve(classOf[ContractIdMustBeLowercaseEvaluator].getName)
    assert(result.isSuccess)
  }

  test("tryResolve wraps a failed resolution in Failure, without throwing") {
    val result = CustomPolicyEvaluatorFactory.tryResolve("com.invaract.contract.NoSuchClassAtAll")
    assert(result.isFailure)
  }
}
