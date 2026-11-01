// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.pythonioadapter

import com.invaract.contract.{Contract, ContractParser, ContractVersion}
import scala.io.Source

/** A contract registry client with the three methods `ContractReference` finds by name (`configure`, `getLatest`, `get`),
  * serving the protocol cases' contract as `orders_report@1.0.0` and nothing else. It stands in for the optional
  * registry-client module in the `registry` protocol case. */
class StubRegistryClient {
  private var url: String = _

  def configure(registryUrl: String): Unit = url = registryUrl

  def getLatest(id: String): Contract = get(id, ContractVersion(1, 0, 0))

  def get(id: String, version: ContractVersion): Contract = {
    require(url == "stub-registry", s"the registry url was '$url'")
    require(id == "orders_report" && version == ContractVersion(1, 0, 0), s"the stub registry holds only orders_report@1.0.0, not $id@$version")
    val source = Source.fromInputStream(getClass.getResourceAsStream("/protocol/passes/contract.yaml"), "UTF-8")
    try ContractParser.parse(source.mkString)
    finally source.close()
  }
}
