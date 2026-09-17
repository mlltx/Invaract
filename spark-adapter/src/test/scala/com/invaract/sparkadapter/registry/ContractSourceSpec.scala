// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter.registry

import com.invaract.contract.ContractVersion

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite

import java.io.File

class ContractSourceSpec extends AnyFunSuite with BeforeAndAfterAll with BeforeAndAfterEach {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("ContractSourceSpec")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
  }

  override def afterEach(): Unit = {
    FakeRegistryClient.reset()
    spark.conf.unset(ContractSource.RegistryUrlConfKey)
    // RuntimeConfig.unset throws if the key was never set - only clear it
    // when a test actually set it.
    try spark.conf.unset(ContractSource.RegistryClientClassConfKey)
    catch { case _: NoSuchElementException => () }
  }

  // --- parse: pure, no Spark session needed ---

  test("parse recognizes an ordinary registry reference") {
    assert(ContractSource.parse("registry://customer_orders@2.1.0") == Some(("customer_orders", "2.1.0")))
  }

  test("parse recognizes the 'latest' sugar version") {
    assert(ContractSource.parse("registry://customer_orders@latest") == Some(("customer_orders", "latest")))
  }

  test("parse returns None for an ordinary file path") {
    assert(ContractSource.parse("/path/to/contract.yaml") == None)
    assert(ContractSource.parse("demo/contracts/invaract_output.yaml") == None)
  }

  test("parse returns None for an ordinary file path that happens to contain '@', not just paths without one") {
    // Without the leading `raw.startsWith(Scheme)` guard, stripPrefix is a
    // no-op on a string that doesn't start with it, and this path's own
    // '@' would then wrongly satisfy the split("@", 2) pattern below,
    // misparsing an ordinary path as a registry reference.
    assert(ContractSource.parse("/data/customer_orders@2.1.0.yaml") == None)
  }

  test("parse returns None for a reference missing the '@' separator") {
    assert(ContractSource.parse("registry://customer_orders") == None)
  }

  test("parse returns None for a reference with an empty id or empty version") {
    assert(ContractSource.parse("registry://@2.1.0") == None)
    assert(ContractSource.parse("registry://customer_orders@") == None)
  }

  test("parse does not treat a bare 'registry://' with nothing after it as a reference") {
    assert(ContractSource.parse("registry://") == None)
  }

  test("parse only splits on the first '@', so an id or version containing '@' round-trips (id side)") {
    // Not a realistic contract id, but proves split(_, 2) is used, not a
    // naive split that would silently drop everything after a second '@'.
    assert(ContractSource.parse("registry://weird@id@2.1.0") == Some(("weird", "id@2.1.0")))
  }

  // --- resolve: an ordinary path is completely unaffected ---

  test("resolve parses an ordinary file path exactly as ContractParser.parseFile would, unaffected by this object existing") {
    val contract = FakeRegistryClient.sampleContract("from_file")
    val file = File.createTempFile("contract-source-spec", ".yaml")
    file.deleteOnExit()
    java.nio.file.Files.write(file.toPath, com.invaract.contract.ContractParser.write(contract).getBytes("UTF-8"))

    val resolved = ContractSource.resolve(file.getPath, spark)
    assert(resolved.id == "from_file")
  }

  // --- resolve: registry:// references, via the reflective fake client ---

  test("resolve fetches the latest version through the reflectively-loaded client when the version segment is 'latest'") {
    val expected = FakeRegistryClient.sampleContract("customer_orders")
    FakeRegistryClient.stub("https://fake-registry.test", latest = Some(expected))
    spark.conf.set(ContractSource.RegistryUrlConfKey, "https://fake-registry.test")
    spark.conf.set(ContractSource.RegistryClientClassConfKey, classOf[FakeRegistryClient].getName)

    val resolved = ContractSource.resolve("registry://customer_orders@latest", spark)
    assert(resolved.id == "customer_orders")
  }

  test("resolve fetches an exact version through the reflectively-loaded client when a real version is named") {
    val expected = FakeRegistryClient.sampleContract("customer_orders")
    FakeRegistryClient.stub("https://fake-registry.test", byVersion = Map(ContractVersion(2, 1, 0) -> expected))
    spark.conf.set(ContractSource.RegistryUrlConfKey, "https://fake-registry.test")
    spark.conf.set(ContractSource.RegistryClientClassConfKey, classOf[FakeRegistryClient].getName)

    val resolved = ContractSource.resolve("registry://customer_orders@2.1.0", spark)
    assert(resolved.id == "customer_orders")
  }

  test("resolve throws IllegalStateException naming the missing conf key when registryUrl is not set") {
    val ex = intercept[IllegalStateException] {
      ContractSource.resolve("registry://customer_orders@latest", spark)
    }
    assert(ex.getMessage.contains(ContractSource.RegistryUrlConfKey))
    assert(ex.getMessage.contains("registry://customer_orders@latest".stripPrefix(ContractSource.Scheme)))
  }

  test("resolve uses only getLatest, never get, for a 'latest' reference against a client with no get overload at all") {
    spark.conf.set(ContractSource.RegistryUrlConfKey, "https://fake-registry.test")
    spark.conf.set(ContractSource.RegistryClientClassConfKey, classOf[GetLatestOnlyFakeRegistryClient].getName)

    val resolved = ContractSource.resolve("registry://customer_orders@latest", spark)
    assert(resolved.id == "customer_orders")
  }

  test("resolve throws a clear IllegalStateException naming the class when the configured registry client class does not exist") {
    spark.conf.set(ContractSource.RegistryUrlConfKey, "https://fake-registry.test")
    spark.conf.set(ContractSource.RegistryClientClassConfKey, "com.invaract.sparkadapter.registry.NoSuchClassEver")

    val ex = intercept[IllegalStateException] {
      ContractSource.resolve("registry://customer_orders@latest", spark)
    }
    assert(ex.getMessage.contains("NoSuchClassEver"))
  }

  test("resolve throws a clear IllegalStateException when the configured class has no public no-arg constructor") {
    spark.conf.set(ContractSource.RegistryUrlConfKey, "https://fake-registry.test")
    spark.conf.set(ContractSource.RegistryClientClassConfKey, classOf[NoNoArgConstructorFakeRegistryClient].getName)

    val ex = intercept[IllegalStateException] {
      ContractSource.resolve("registry://customer_orders@latest", spark)
    }
    assert(ex.getMessage.contains("no-arg constructor"))
  }

  test("resolve defaults to registry-client's own real class name when registryClientClass is unset") {
    assert(ContractSource.DefaultRegistryClientClass == "com.invaract.registryclient.HttpContractRegistryClient")
  }
}
