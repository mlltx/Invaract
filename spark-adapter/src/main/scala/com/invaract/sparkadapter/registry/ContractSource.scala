// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter.registry

import com.invaract.contract.{Contract, ContractParser, ContractVersion}

import org.apache.spark.sql.SparkSession

/** Recognizes a `registry://<id>@<version>` reference in
  * `spark.invaract.contract`'s value, the same "a scheme the value can
  * carry instead of a literal path, resolved before anything else uses
  * it" shape `location.LocationRef`'s `ref://` already establishes for
  * `Dataset.location` — see docs/CONTRACT_REGISTRY.md §7.
  *
  * Deliberately has **no compile-time dependency on `registry-client`** —
  * a contract registry is optional, opt-in infrastructure a platform team
  * may never deploy (unlike `fingerprint`, which `spark-adapter` bundles
  * unconditionally because it's core, expected-by-default functionality).
  * The real `HttpContractRegistryClient` is resolved reflectively by
  * class name, the same "class name in config, public no-arg
  * constructor, loaded once" mechanism
  * `notification.NotificationSinkFactory` already establishes for
  * `NotificationSink` — the difference here is there is no shared trait
  * to cast to (that would itself be a compile-time dependency), so the
  * two methods actually needed are invoked via plain
  * `java.lang.reflect.Method.invoke` instead of a virtual call. Their
  * return type, `com.invaract.contract.Contract`, is safe to cast to
  * directly because `contract` is already a real dependency of both this
  * module and `registry-client`. A job that never sets
  * `spark.invaract.registryUrl` or uses a `registry://` reference never
  * touches any of this — `registry-client`'s jar doesn't even need to be
  * on the classpath.
  */
object ContractSource {

  val Scheme = "registry://"

  /** The sugar version segment meaning "whatever the registry currently
    * considers latest for this id" — matches
    * docs/CONTRACT_REGISTRY.md §3's `GET .../versions/latest`.
    */
  val LatestVersion = "latest"

  /** Base URL of the registry to fetch from (e.g.
    * `https://registry.corp.internal`). Required only when
    * `spark.invaract.contract` actually is a `registry://` reference.
    */
  val RegistryUrlConfKey = "spark.invaract.registryUrl"

  /** Fully-qualified class name of the `ContractRegistryClient`
    * implementation to load, overriding `DefaultRegistryClientClass`.
    * Exists for a job pointed at a different client implementation (a
    * test double, a future non-HTTP transport) without touching this
    * module.
    */
  val RegistryClientClassConfKey = "spark.invaract.registryClientClass"

  /** `registry-client`'s own real implementation — used when
    * `RegistryClientClassConfKey` is unset, which is the normal case.
    */
  val DefaultRegistryClientClass = "com.invaract.registryclient.HttpContractRegistryClient"

  /** Recognizes the `registry://<id>@<version>` shape — `None` for an
    * ordinary file path, `Some((id, version))` for a reference. `id` and
    * `version` are returned exactly as written (`version` may be the
    * literal string `"latest"`, not necessarily a parseable
    * `ContractVersion` yet) so a caller can distinguish "not a registry
    * reference at all" from "a malformed one" if it ever needs to.
    */
  def parse(raw: String): Option[(String, String)] =
    if (raw.startsWith(Scheme)) {
      raw.stripPrefix(Scheme).split("@", 2) match {
        case Array(id, version) if id.nonEmpty && version.nonEmpty => Some((id, version))
        case _                                                     => None
      }
    } else {
      None
    }

  /** Resolves `spark.invaract.contract`'s raw value to a real `Contract`:
    * `ContractParser.parseFile` for an ordinary path (unchanged
    * behavior, the only path that existed before this object did), or a
    * registry fetch for a recognized `registry://<id>@<version>`
    * reference.
    */
  def resolve(raw: String, session: SparkSession): Contract =
    parse(raw) match {
      case Some((contractId, version)) => fetchFromRegistry(contractId, version, session)
      case None                        => ContractParser.parseFile(raw)
    }

  private def fetchFromRegistry(contractId: String, version: String, session: SparkSession): Contract = {
    val registryUrl = session.conf.getOption(RegistryUrlConfKey).getOrElse(
      throw new IllegalStateException(
        s"A 'registry://$contractId@$version' contract reference requires '$RegistryUrlConfKey' to be set."
      )
    )
    val client = instantiateClient(session.conf.getOption(RegistryClientClassConfKey).getOrElse(DefaultRegistryClientClass))
    invokeConfigure(client, registryUrl)
    if (version == LatestVersion) invokeGetLatest(client, contractId)
    else invokeGet(client, contractId, ContractVersion.parse(version))
  }

  private def instantiateClient(className: String): AnyRef =
    try {
      Class.forName(className).getDeclaredConstructor().newInstance().asInstanceOf[AnyRef]
    } catch {
      case e: ClassNotFoundException =>
        throw new IllegalStateException(
          s"Could not find registry client class '$className' on the classpath - " +
            "add the invaract-registry-client jar via spark-submit --jars.",
          e
        )
      case e: ReflectiveOperationException =>
        throw new IllegalStateException(
          s"Could not instantiate registry client '$className' (it needs a public no-arg constructor).",
          e
        )
    }

  private def invokeConfigure(client: AnyRef, registryUrl: String): Unit =
    client.getClass.getMethod("configure", classOf[String]).invoke(client, registryUrl)

  private def invokeGetLatest(client: AnyRef, contractId: String): Contract =
    client.getClass.getMethod("getLatest", classOf[String]).invoke(client, contractId).asInstanceOf[Contract]

  private def invokeGet(client: AnyRef, contractId: String, version: ContractVersion): Contract =
    client.getClass
      .getMethod("get", classOf[String], classOf[ContractVersion])
      .invoke(client, contractId, version)
      .asInstanceOf[Contract]
}
