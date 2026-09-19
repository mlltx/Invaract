// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.registryclient

import com.invaract.contract.{CompatibilityReport, Contract, ContractVersion}

/** One entry of `ContractRegistryClient.listVersions` — a version known to
  * the registry and the `Contract.status` it was registered with (the
  * field already on the existing model, e.g. "draft"/"active"/
  * "deprecated" — the registry does not define its own separate status
  * vocabulary).
  */
case class VersionInfo(version: ContractVersion, status: String)

/** Base type for every failure `ContractRegistryClient` can raise, so a
  * caller can catch this one type when it only cares "did this fail," and
  * the specific subtype when it needs to react differently — the same
  * shape `LocationResolutionException`/`ContractParseException` already
  * use elsewhere in this codebase (a real, named exception per failure
  * mode, not a generic wrapper around an HTTP status code).
  */
class ContractRegistryException(message: String, cause: Throwable = null)
    extends RuntimeException(message, cause)

/** The requested contract id or version does not exist in the registry
  * (HTTP 404 — docs/CONTRACT_REGISTRY.md §3).
  */
class ContractRegistryNotFoundException(contractId: String, version: Option[String])
    extends ContractRegistryException(
      s"No contract registered for id '$contractId'" + version.fold("")(v => s" at version '$v'")
    )

/** A `register` call was rejected because the store's current version for
  * this contract id had already advanced past `expectedPrevious` by the
  * time the request was handled (HTTP 409, the version-conflict case —
  * docs/CONTRACT_REGISTRY.md §4). `actualCurrent` is the version the
  * caller should re-read and retry against; `None` means the registry now
  * believes a first version was already registered by someone else.
  */
class ContractRegistryConflictException(val actualCurrent: Option[ContractVersion])
    extends ContractRegistryException(
      s"Version conflict: registry's current version is " +
        actualCurrent.fold("none (no version registered yet)")(_.toString) +
        ", not what this request expected"
    )

/** A `register` call was rejected because the declared version bump does
  * not match the actual scope of change, and the request did not pass
  * `force = true` (HTTP 409, the compatibility-rejection case —
  * docs/CONTRACT_REGISTRY.md §5). Retry with `force = true` to override
  * deliberately, the same "two honest options, never a silent third"
  * choice CLAUDE.md's API Compatibility Requirement already applies to a
  * different kind of compatibility break.
  */
class ContractRegistryCompatibilityException(val report: CompatibilityReport)
    extends ContractRegistryException(
      "Registration rejected: declared version bump does not match the actual scope of change " +
        s"(${report.breakingChanges.size} breaking change(s)). Retry with force = true to override deliberately."
    )

/** The submitted contract failed parsing or `ContractValidator` (HTTP 400
  * — docs/CONTRACT_REGISTRY.md §5). `messages` is the server's own
  * human-readable issue text; v1 does not round-trip the full structured
  * `ValidationIssue` (severity/path) over the wire, only its rendered
  * message — a deliberate, disclosed v1 simplification, not an oversight.
  */
class ContractRegistryValidationException(val messages: List[String])
    extends ContractRegistryException(
      s"Registration rejected: ${messages.mkString("; ")}"
    )

/** Client for the contract registry HTTP API specified in
  * docs/CONTRACT_REGISTRY.md (Invaract repo) and implemented by the
  * separate `mlltx/invaract-registry` server repo. One real
  * implementation ships in this module, `HttpContractRegistryClient`; the
  * trait exists so `spark-adapter` can reference this shape without a
  * compile-time dependency on this module at all (resolved reflectively —
  * see docs/CONTRACT_REGISTRY.md §7).
  */
trait ContractRegistryClient {

  /** Fetches exactly one version. Throws
    * `ContractRegistryNotFoundException` if the id or version is unknown
    * to the registry.
    */
  def get(contractId: String, version: ContractVersion): Contract

  /** Fetches whatever version the registry currently considers latest for
    * this id. Throws `ContractRegistryNotFoundException` if the id is
    * unknown.
    */
  def getLatest(contractId: String): Contract

  /** Every version the registry knows about for this id, in whatever
    * order the server returns them (docs/CONTRACT_REGISTRY.md §3 does not
    * mandate an order). Throws `ContractRegistryNotFoundException` if the
    * id is unknown.
    */
  def listVersions(contractId: String): List[VersionInfo]

  /** Every contract id the registry knows about at all — the query
    * cross-contract policies and impact analysis will need once they
    * exist (docs/CONTRACT_REGISTRY.md §3).
    */
  def listContractIds(): List[String]

  /** Registers `contract` as a new version, atomically, only if the
    * registry's current version for this id equals `expectedPrevious`
    * (`None` asserts "this must be the very first version ever
    * registered for this id"). Returns the compatibility report computed
    * against the previous version on success (`CompatibilityReport(Nil)`
    * when this is the first version — nothing to compare against).
    *
    * Throws `ContractRegistryConflictException` if another writer already
    * advanced past `expectedPrevious`, `ContractRegistryCompatibilityException`
    * if the declared version bump doesn't match the actual diff and
    * `force` was not set, or `ContractRegistryValidationException` if the
    * contract itself fails parsing/validation.
    */
  def register(
    contract: Contract,
    expectedPrevious: Option[ContractVersion],
    force: Boolean = false
  ): CompatibilityReport
}
