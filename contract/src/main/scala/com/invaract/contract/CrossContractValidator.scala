// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.contract

/** One inconsistency detected between two independently-authored,
  * independently-governed contracts that both reference the same physical
  * location with contradictory declared roles — see
  * docs/CONTRACT_MODEL.md's "Input and Output Types" section, "Cross-contract
  * validation": "A contract identifying an object as a SOURCE where another
  * governed contract establishes it as a DATA_ASSET."
  *
  * @param location the shared physical location both contracts reference —
  *   already contract-declared on both sides (never a raw Spark-reported
  *   one), so a simple backslash-normalized comparison is enough; there is
  *   no "file:"-scheme stripping to do the way `StructuralVerifier.locationsMatch`
  *   needs for a contract-declared location against a real plan's actual one.
  * @param producingContract the contract (as `"id@version"`) that declares
  *   `location` as a `DATA_ASSET` output — the "governed contract" the spec's
  *   own example refers to.
  * @param consumingContract the contract (as `"id@version"`) that declares
  *   the same `location` as a `SOURCE` input instead.
  * @param consumingDataset the consuming contract's own dataset name for
  *   its (contradicting) input declaration.
  */
case class CrossContractIssue(
  location: String,
  producingContract: String,
  consumingContract: String,
  consumingDataset: String,
  message: String
)

/** Cross-references a set of independently-parsed contracts against each
  * other for role declarations that contradict one another — pure,
  * engine-independent, the same `contract`-module-only reach
  * `ContractCompatibility`/`ContractValidator` already have. Distinct from
  * `OrgPolicyEvaluator`: an org policy expresses what *one* organization
  * requires of *any* contract; this instead compares *multiple contracts'
  * own declarations* against each other, something no single contract (or
  * policy) can determine in isolation.
  *
  * Deliberately narrow, per the spec's own recommended-order framing for
  * this phase ("Once multiple contracts can be registered, validate
  * relationships between them" — only after the single-contract type model
  * and conformance model are trusted): the one check implemented is the
  * spec's own worked example — a location one contract declares as a
  * `DATA_ASSET` output, that another contract declares as a `SOURCE` input
  * instead, rather than the same `DATA_ASSET` a governed dependency should
  * be declared as. Not attempted here (left for a later phase, per the
  * spec's own "high-confidence evidence first" priority): reasoning about
  * schema/version compatibility across the same dependency (already
  * `ContractCompatibility`'s own, narrower concern for one contract's two
  * versions), or any check involving a `CONTROL`-declared cross-contract
  * reference (the spec gives no worked example for that combination).
  *
  * How a caller gathers the `List[Contract]` to compare — a local directory
  * of contract files (`CrossContractLintCli`), a full registry listing, a
  * platform's own catalog — is deliberately out of scope here; this module
  * has no Spark dependency and no opinion on where contracts come from, the
  * same boundary `ContractCompatibility.diff` already draws for comparing
  * two versions of one contract.
  */
object CrossContractValidator {

  /** Every `DATA_ASSET`-output-vs-`SOURCE`-input contradiction across every
    * pair of distinct contracts in `contracts` (matched by `Contract.id` —
    * two different versions of the *same* contract id are never compared
    * against each other, the same "not a real dependency" reasoning that
    * excludes a contract from being compared against itself). Order is
    * deterministic: contracts are visited in `contracts`' own order, and for
    * each `DATA_ASSET` output, every other contract's matching `SOURCE`
    * inputs are visited in that other contract's own declared order.
    */
  def validate(contracts: List[Contract]): List[CrossContractIssue] =
    for {
      producer <- contracts
      output <- producer.outputs
      if output.datasetType.contains(DatasetType.DataAsset)
      consumer <- contracts
      if consumer.id != producer.id
      input <- consumer.inputs
      if input.datasetType.contains(DatasetType.Source)
      if sameLocation(output.location, input.location)
    } yield CrossContractIssue(
      location = output.location,
      producingContract = s"${producer.id}@${producer.version}",
      consumingContract = s"${consumer.id}@${consumer.version}",
      consumingDataset = input.name,
      message =
        s"'${output.location}' is declared a DATA_ASSET output by contract '${producer.id}@${producer.version}' " +
          s"(output '${output.name}'), but contract '${consumer.id}@${consumer.version}' declares the same " +
          s"location a SOURCE input instead (input '${input.name}') - a governed contract already establishes " +
          "this as a DATA_ASSET; consider declaring it DATA_ASSET here too so this dependency is tracked " +
          "consistently across both contracts."
    )

  /** Both sides here are contract-declared locations (never a raw
    * Spark-reported one), so the only real-world variation to tolerate is a
    * Windows-authored path's backslashes — unlike
    * `StructuralVerifier.locationsMatch`, there is no `"file:"` scheme or
    * suffix-boundary case to handle, since neither side ever comes from an
    * actual Spark plan. `private[contract]`, not `private`: `TypeGuaranteeValidator`'s
    * own cross-contract checks need the identical comparison and reuse this
    * rather than a second copy.
    */
  private[contract] def sameLocation(a: String, b: String): Boolean = normalize(a) == normalize(b)

  /** `private[contract]`, not `private`: `TypeGuaranteeValidator` reuses
    * this directly as a group-by key (rather than an O(n^2) pairwise
    * `sameLocation` scan) when bucketing declarations by physical location.
    */
  private[contract] def normalize(location: String): String = location.trim.replace('\\', '/')
}
