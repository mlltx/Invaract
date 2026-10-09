// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.verification

import com.invaract.contract.{ContractParser, ContractVersion}

import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.Files

class ContractReferenceSpec extends AnyFunSuite with BeforeAndAfterEach {

  override def beforeEach(): Unit = FakeRegistryClient.reset()

  private val Url = "https://fake-registry.test"

  private def config(entries: (String, String)*): ConfigSource = ConfigSource.fromMap(entries.toMap)
  private def registry(client: Class[_]): ConfigSource =
    config(InvaractConf.RegistryUrl -> Url, InvaractConf.RegistryClientClass -> client.getName)

  // --- parse ---

  test("parse recognises a reference, and the 'latest' sugar") {
    assert(ContractReference.parse("registry://customer_orders@2.1.0").contains(("customer_orders", "2.1.0")))
    assert(ContractReference.parse("registry://customer_orders@latest").contains(("customer_orders", "latest")))
  }

  test("parse is None for an ordinary path, including one that contains '@'") {
    assert(ContractReference.parse("/path/to/contract.yaml").isEmpty)
    assert(ContractReference.parse("/data/customer_orders@2.1.0.yaml").isEmpty)
  }

  test("parse is None for a malformed reference") {
    for (bad <- List("registry://", "registry://customer_orders", "registry://@2.1.0", "registry://customer_orders@")) {
      assert(ContractReference.parse(bad).isEmpty, bad)
    }
  }

  test("parse splits on the first '@' only") {
    assert(ContractReference.parse("registry://weird@id@2.1.0").contains(("weird", "id@2.1.0")))
  }

  // --- resolve: a path is unaffected ---

  test("resolve parses an ordinary path exactly as ContractParser.parseFile would, without a registry configured") {
    val file = Files.createTempFile("contract-reference", ".yaml")
    file.toFile.deleteOnExit()
    Files.write(file, ContractParser.write(FakeRegistryClient.contract("from_file")).getBytes("UTF-8"))
    assert(ContractReference.resolve(file.toString, ConfigSource.empty).id == "from_file")
    assert(FakeRegistryClient.calls.isEmpty)
  }

  test("a malformed reference is treated as a path, so it fails as a missing file, not as a registry error") {
    intercept[Exception](ContractReference.resolve("registry://no_version", ConfigSource.empty))
  }

  // --- resolve: through the registry client ---

  test("'latest' calls configure with the URL and getLatest with the id, and never get") {
    FakeRegistryClient.stubLatest(Url, FakeRegistryClient.contract("customer_orders"))
    val resolved = ContractReference.resolve("registry://customer_orders@latest", registry(classOf[FakeRegistryClient]))
    assert(resolved.id == "customer_orders")
    assert(FakeRegistryClient.configuredWith.contains(Url))
    assert(FakeRegistryClient.calls == List("latest:customer_orders"))
  }

  test("an exact version calls get with the parsed version, and never getLatest") {
    FakeRegistryClient.stubExact(Url, ContractVersion(2, 1, 0), FakeRegistryClient.contract("customer_orders"))
    val resolved = ContractReference.resolve("registry://customer_orders@2.1.0", registry(classOf[FakeRegistryClient]))
    assert(resolved.id == "customer_orders")
    assert(FakeRegistryClient.calls == List("get:customer_orders@2.1.0"))
  }

  test("a client with no 'get' resolves 'latest', because only the method a call needs is looked for") {
    assert(ContractReference.resolve("registry://c@latest", registry(classOf[LatestOnlyRegistryClient])).id == "c")
  }

  test("a client with no 'get' is rejected for an exact version") {
    val ex = intercept[IllegalStateException](ContractReference.resolve("registry://c@1.0.0", registry(classOf[LatestOnlyRegistryClient])))
    assert(ex.getMessage.contains("is not a registry client"))
    assert(ex.getMessage.contains("get(String, ContractVersion)"))
  }

  test("a missing registry URL names the setting as the engine spells it, and the reference") {
    val spelled = new ConfigSource {
      def get(name: String): Option[String] = None
      override def describe(name: String): String = "engine." + name
    }
    val ex = intercept[IllegalStateException](ContractReference.resolve("registry://customer_orders@latest", spelled))
    assert(ex.getMessage == "A 'registry://customer_orders@latest' contract reference requires 'engine.registryUrl' to be set.")
  }

  test("an unknown client class is reported by name, with how to fix it") {
    val cfg = config(InvaractConf.RegistryUrl -> Url, InvaractConf.RegistryClientClass -> "com.invaract.verification.NoSuchClassEver")
    val ex = intercept[IllegalStateException](ContractReference.resolve("registry://c@latest", cfg))
    assert(ex.getMessage.contains("NoSuchClassEver"))
    assert(ex.getMessage.contains("--jars"))
  }

  test("a client without a public no-arg constructor is reported as such") {
    val ex = intercept[IllegalStateException](ContractReference.resolve("registry://c@latest", registry(classOf[NoNoArgRegistryClient])))
    assert(ex.getMessage.contains("no-arg constructor"))
  }

  test("a class that is not a registry client is loaded but never initialised") {
    InitCounter.count = 0
    val ex = intercept[IllegalStateException](ContractReference.resolve("registry://c@latest", config(InvaractConf.RegistryUrl -> Url, InvaractConf.RegistryClientClass -> "com.invaract.verification.InitProbe$")))
    assert(ex.getMessage.contains("is not a registry client"))
    assert(InitCounter.count == 0, "its static initialiser did not run")
  }

  test("a class with the methods missing is rejected before it is constructed") {
    val ex = intercept[IllegalStateException](ContractReference.resolve("registry://c@latest", registry(classOf[CountingNoMethodsClient])))
    assert(ex.getMessage.contains("is not a registry client"))
    assert(FakeRegistryClient.constructed == 0)
  }

  test("a method that returns something other than a Contract is rejected") {
    val ex = intercept[IllegalStateException](ContractReference.resolve("registry://c@latest", registry(classOf[WrongReturnRegistryClient])))
    assert(ex.getMessage.contains("must return a Contract"))
  }

  test("the default client class is registry-client's own") {
    assert(ContractReference.DefaultRegistryClientClass == "com.invaract.registryclient.HttpContractRegistryClient")
    // unset registryClientClass falls back to it: with no registry-client on this classpath it is reported by name
    val ex = intercept[IllegalStateException](ContractReference.resolve("registry://c@latest", config(InvaractConf.RegistryUrl -> Url)))
    assert(ex.getMessage.contains("HttpContractRegistryClient"))
  }
}
