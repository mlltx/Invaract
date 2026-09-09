// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter.location

import com.invaract.contract.{Contract, Dataset}

/** Resolves every `ref://<id>` location in a parsed `Contract` to a real,
  * literal location, using `resolver`. This is a pure transform over the
  * object model `contract` already exposes — no changes to `contract`
  * itself (`ContractParser` already accepted `"ref://orders-input"` as an
  * ordinary, valid, non-empty `location` string; it just doesn't know what
  * it means) — so call this *after* `ContractParser.parseFile`/`.parse`
  * and *before* the resolved `Contract` reaches `ContractEnforcementRule`,
  * `StructuralVerifier`, `ContractInference`, or `SensitivityLineage`,
  * every one of which already assumes `Dataset.location` is a literal.
  * Concretely, this means before the `SparkSession` is built: the check
  * rule those consult is installed at session-construction time and can't
  * be changed afterward (see `ContractEnforcementRule`'s class doc), so
  * resolution — and any `LocationResolutionException` it might throw —
  * has to complete first, the same way loading the contract itself already
  * does.
  *
  * Deliberately unconditional: call this even when `resolver` is
  * `NoOpLocationResolver` (no real resolution configured), rather than
  * skipping it for a contract you don't expect to use `ref://` at all. A
  * contract with no references is unaffected either way; one that
  * (accidentally, or because resolution wasn't wired up) does gets a
  * clear `LocationResolutionException` here instead of a confusing
  * `MissingInput`/`OutputLocationMismatch` downstream against the literal
  * string `"ref://..."`.
  */
object ContractLocationResolution {

  def resolve(contract: Contract, resolver: LocationResolver): Contract =
    contract.copy(
      inputs = contract.inputs.map(resolveDataset(_, resolver)),
      outputs = contract.outputs.map(resolveDataset(_, resolver))
    )

  private def resolveDataset(dataset: Dataset, resolver: LocationResolver): Dataset =
    LocationRef.id(dataset.location) match {
      case Some(id) => dataset.copy(location = resolver.resolve(id))
      case None     => dataset
    }
}
