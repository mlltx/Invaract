// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.verification

/** An engine-neutral view of the runtime configuration a verification setup
  * reads - what lets a platform attach a capability purely through the
  * engine's own configuration surface, against a job whose source it does not
  * own (CLAUDE.md's External Attachability Requirement).
  *
  * Keys are the neutral names in `InvaractConf` (`locationMap`, `orgPolicy`,
  * ...), not full keys: each adapter decides how its engine spells them.
  * Spark spells `locationMap` as `spark.invaract.locationMap` (the only prefix
  * `spark-submit --conf` passes through); another engine would map the same
  * names onto its own options.
  */
trait ConfigSource {

  /** The value configured for neutral key `name`, if any. */
  def get(name: String): Option[String]

  /** How `name` is spelled in this engine, for error messages that tell a
    * person which setting to fix. Defaults to the neutral name.
    */
  def describe(name: String): String = name
}

object ConfigSource {

  /** Nothing configured. */
  val empty: ConfigSource = new ConfigSource {
    def get(name: String): Option[String] = None
  }

  /** A fixed map, keyed by neutral name - for tests and for adapters whose
    * configuration is already a map.
    */
  def fromMap(entries: Map[String, String]): ConfigSource = new ConfigSource {
    def get(name: String): Option[String] = entries.get(name)
  }

  /** `lookup` called with `prefix + name` - the shape of an engine that spells
    * every Invaract setting under one prefix (Spark's `spark.invaract.`).
    */
  def prefixed(prefix: String)(lookup: String => Option[String]): ConfigSource = new ConfigSource {
    def get(name: String): Option[String] = lookup(prefix + name)
    override def describe(name: String): String = prefix + name
  }
}

/** The neutral names of the settings `VerificationSetup` reads. An adapter
  * prefixes them as its engine requires (Spark: `spark.invaract.<name>`).
  */
object InvaractConf {
  val LocationMap = "locationMap"
  val RejectUndeclaredInputs = "rejectUndeclaredInputs"
  val RejectUndeclaredFields = "rejectUndeclaredFields"
  val ComputeFingerprint = "computeFingerprint"
  val StaticDataQuality = "staticDataQuality"
  val RoleConsistency = "roleConsistency"
  val OrgPolicy = "orgPolicy"
  val OrgPolicyOverlays = "orgPolicyOverlays"

  // What `AttachSetup` reads to decide what a job is attached to, and how.
  /** The contract to enforce: a file path, or a `registry://<id>@<version>` reference (`ContractReference`). */
  val Contract = "contract"
  /** `"true"` runs dry-run mode, which needs no contract at all. */
  val DryRun = "dryRun"
  /** Path to a notification-sink `.properties` file. */
  val NotifyConfig = "notifyConfig"
  /** Base URL of the contract registry a `registry://` reference is fetched from. */
  val RegistryUrl = "registryUrl"
  /** Class name of the registry client to load, overriding the default. */
  val RegistryClientClass = "registryClientClass"
}
