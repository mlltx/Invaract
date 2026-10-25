// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter.registry

import com.invaract.contract.Contract
import com.invaract.sparkadapter.SparkConfigSource
import com.invaract.verification.ContractReference

import org.apache.spark.sql.SparkSession

/** Spark's face of `ContractReference` (verification-core): recognizes a
  * `registry://<id>@<version>` reference in `spark.invaract.contract`'s value
  * and resolves it, the same "a scheme the value can carry instead of a
  * literal path, resolved before anything else uses it" shape
  * `location.LocationRef`'s `ref://` already establishes for
  * `Dataset.location` - see docs/CONTRACT_REGISTRY.md section 7.
  *
  * The parsing, the registry client's loading and the fetch are engine-neutral
  * and live in `ContractReference`, reading `registryUrl`/`registryClientClass`
  * through a `ConfigSource`; this object supplies Spark's spelling of them
  * (`spark.invaract.registryUrl`, `spark.invaract.registryClientClass`) and
  * keeps the names and signatures it has always had. Deliberately **no
  * compile-time dependency on `registry-client`**: see `ContractReference`.
  */
object ContractSource {

  val Scheme: String = ContractReference.Scheme

  /** The sugar version segment meaning "whatever the registry currently
    * considers latest for this id" - docs/CONTRACT_REGISTRY.md section 3.
    */
  val LatestVersion: String = ContractReference.LatestVersion

  /** Base URL of the registry to fetch from (e.g.
    * `https://registry.corp.internal`). Required only when
    * `spark.invaract.contract` actually is a `registry://` reference.
    */
  val RegistryUrlConfKey = "spark.invaract.registryUrl"

  /** Fully-qualified class name of the `ContractRegistryClient`
    * implementation to load, overriding `DefaultRegistryClientClass`.
    */
  val RegistryClientClassConfKey = "spark.invaract.registryClientClass"

  /** `registry-client`'s own real implementation - used when
    * `RegistryClientClassConfKey` is unset, which is the normal case.
    */
  val DefaultRegistryClientClass: String = ContractReference.DefaultRegistryClientClass

  /** `None` for an ordinary file path, `Some((id, version))` for a reference. */
  def parse(raw: String): Option[(String, String)] = ContractReference.parse(raw)

  /** Resolves `spark.invaract.contract`'s raw value to a real `Contract`: a
    * file parse for an ordinary path, a registry fetch for a recognized
    * `registry://<id>@<version>` reference.
    */
  def resolve(raw: String, session: SparkSession): Contract = ContractReference.resolve(raw, SparkConfigSource(session))
}
