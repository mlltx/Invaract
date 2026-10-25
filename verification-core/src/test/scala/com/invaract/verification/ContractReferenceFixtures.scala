// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.verification

import com.invaract.contract.{Contract, ContractParser, ContractVersion}

/** A real, permanent stand-in for `registry-client`'s `HttpContractRegistryClient`, named by
  * `registryClientClass` and loaded reflectively. `ContractReference` shares no interface with the real
  * client (there deliberately is none), so this implements only the three method shapes it looks for.
  *
  * `ContractReference` builds a fresh instance per call, so what each instance returns is kept on the
  * companion, keyed by the registry URL a test configures it with.
  */
class FakeRegistryClient {
  private var url: String = _
  def configure(registryUrl: String): Unit = { url = registryUrl; FakeRegistryClient.configuredWith = Some(registryUrl) }
  def getLatest(contractId: String): Contract = FakeRegistryClient.record(s"latest:$contractId", FakeRegistryClient.latestFor(url, contractId))
  def get(contractId: String, version: ContractVersion): Contract =
    FakeRegistryClient.record(s"get:$contractId@$version", FakeRegistryClient.exactFor(url, contractId, version))
}

object FakeRegistryClient {
  @volatile var configuredWith: Option[String] = None
  @volatile var calls: List[String] = Nil
  @volatile var constructed: Int = 0
  private var latest: Map[String, Contract] = Map.empty
  private var exact: Map[(String, ContractVersion), Contract] = Map.empty

  def reset(): Unit = synchronized { latest = Map.empty; exact = Map.empty; configuredWith = None; calls = Nil; constructed = 0 }
  def stubLatest(url: String, contract: Contract): Unit = synchronized { latest += url -> contract }
  def stubExact(url: String, version: ContractVersion, contract: Contract): Unit = synchronized { exact += (url, version) -> contract }

  private[verification] def record(call: String, contract: Contract): Contract = synchronized { calls = calls :+ call; contract }
  private[verification] def latestFor(url: String, id: String): Contract =
    synchronized(latest.getOrElse(url, throw new NoSuchElementException(s"no 'latest' stub for '$url'")))
  private[verification] def exactFor(url: String, id: String, version: ContractVersion): Contract =
    synchronized(exact.getOrElse((url, version), throw new NoSuchElementException(s"no stub for $id@$version at '$url'")))

  def contract(id: String): Contract = ContractParser.parse(
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

/** Has `configure` and `getLatest` but no `get`: proves only the method a call needs is looked for. */
class LatestOnlyRegistryClient {
  def configure(registryUrl: String): Unit = ()
  def getLatest(contractId: String): Contract = FakeRegistryClient.contract(contractId)
}

/** Has the methods but no public no-arg constructor. */
class NoNoArgRegistryClient(unused: String) {
  def configure(registryUrl: String): Unit = ()
  def getLatest(contractId: String): Contract = FakeRegistryClient.contract(contractId)
}

/** Counts how often `InitProbe` is initialised. Kept apart from the probe so reading it does not initialise it. */
object InitCounter { @volatile var count = 0 }

/** A class whose static initialiser has an observable effect (`InitProbe$` is initialised on first use, never on load). */
object InitProbe { InitCounter.count += 1 }

/** The right method names with a wrong return type. */
class WrongReturnRegistryClient {
  def configure(registryUrl: String): Unit = ()
  def getLatest(contractId: String): String = contractId
}

/** Constructing it counts, so a test can show a class lacking a method is never constructed. */
class CountingNoMethodsClient { FakeRegistryClient.constructed += 1 }
