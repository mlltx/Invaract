// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter.registry

import com.invaract.contract.{Contract, ContractParser, ContractVersion}

/** A real, permanent fake `ContractRegistryClient`-shaped class for
  * `ContractSourceSpec` — a class name reflectively resolved against the
  * real test classpath via `spark.invaract.registryClientClass`, the same
  * "real fixture, not a mock" approach
  * `contract.CustomPolicyEvaluatorFixtures`/`NotificationSinkFactory`'s
  * own tests use. Implements no shared trait (there deliberately isn't
  * one — see `ContractSource`'s own doc) — only the exact method
  * signatures `ContractSource` invokes via reflection: `configure`,
  * `getLatest`, `get`.
  *
  * A public no-arg constructor is required (the same requirement
  * `ContractSource.instantiateClient` documents); state is `var`s on the
  * instance rather than anything shared across instances, since
  * `ContractSource` constructs a fresh one per call.
  */
class FakeRegistryClient {
  private var configuredUrl: String = _
  private var contractsById: Map[String, Contract] = Map.empty

  def configure(registryUrl: String): Unit = {
    configuredUrl = registryUrl
  }

  def getLatest(contractId: String): Contract =
    FakeRegistryClient.latestFor(contractId, configuredUrl)

  def get(contractId: String, version: ContractVersion): Contract =
    FakeRegistryClient.exactFor(contractId, version, configuredUrl)
}

/** Companion holding the fixed responses `FakeRegistryClient` instances
  * return, keyed by the `registryUrl` each test configures it with (a
  * `ContractSource.fetchFromRegistry` call constructs a *new*
  * `FakeRegistryClient` instance every time via reflection, so per-test
  * state has to live somewhere shared across instances - this companion
  * object, reset per test to avoid cross-test leakage).
  */
object FakeRegistryClient {
  private var responsesByUrl: Map[String, (Option[Contract], Map[ContractVersion, Contract])] = Map.empty

  /** Registers what a `FakeRegistryClient` configured with `registryUrl`
    * should return: `latest` for `getLatest`, and `byVersion` for `get`.
    * Call this before invoking `ContractSource.resolve` with a
    * `spark.invaract.registryUrl` set to `registryUrl`.
    */
  def stub(registryUrl: String, latest: Option[Contract] = None, byVersion: Map[ContractVersion, Contract] = Map.empty): Unit =
    synchronized {
      responsesByUrl += registryUrl -> (latest, byVersion)
    }

  def reset(): Unit = synchronized {
    responsesByUrl = Map.empty
  }

  private def latestFor(contractId: String, registryUrl: String): Contract =
    synchronized {
      responsesByUrl.get(registryUrl).flatMap(_._1).getOrElse(
        throw new NoSuchElementException(s"FakeRegistryClient: no 'latest' stub registered for registryUrl '$registryUrl'")
      )
    }

  private def exactFor(contractId: String, version: ContractVersion, registryUrl: String): Contract =
    synchronized {
      responsesByUrl.get(registryUrl).flatMap(_._2.get(version)).getOrElse(
        throw new NoSuchElementException(s"FakeRegistryClient: no stub registered for '$contractId'@'$version' at registryUrl '$registryUrl'")
      )
    }

  def sampleContract(id: String = "customer_orders"): Contract =
    ContractParser.parse(
      s"""id: $id
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: gold.$id
         |    schema:
         |      fields:
         |        - name: customer_id
         |          type: string
         |          required: true
         |""".stripMargin
    )
}

/** A second fixture class exposing only `configure` and `getLatest` (no
  * `get` overload at all) — used to prove `ContractSource` only invokes
  * the one method a given `resolve` call actually needs, not both
  * unconditionally.
  */
class GetLatestOnlyFakeRegistryClient {
  def configure(registryUrl: String): Unit = ()
  def getLatest(contractId: String): Contract = FakeRegistryClient.sampleContract(contractId)
}

/** A fixture class missing a public no-arg constructor entirely, to
  * exercise `ContractSource.instantiateClient`'s
  * `ReflectiveOperationException` branch.
  */
class NoNoArgConstructorFakeRegistryClient(unused: String) {
  def configure(registryUrl: String): Unit = ()
  def getLatest(contractId: String): Contract = FakeRegistryClient.sampleContract(contractId)
}
