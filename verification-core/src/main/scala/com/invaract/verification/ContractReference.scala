// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.verification

import com.invaract.contract.{Contract, ContractParser, ContractVersion}

import java.lang.reflect.Method

/** Turns the value of the `contract` setting into a `Contract`: a file path
  * (`ContractParser.parseFile`), or a `registry://<id>@<version>` reference
  * fetched from a contract registry (docs/CONTRACT_REGISTRY.md section 7).
  *
  * Engine-neutral: the registry's address and client class come from a
  * `ConfigSource` (`registryUrl`, `registryClientClass`), so any adapter whose
  * `ConfigSource` spells them gets `registry://` for free.
  *
  * Deliberately has **no compile-time dependency on `registry-client`**: a
  * registry is optional infrastructure a platform may never deploy. The client
  * is loaded by class name, and as it shares no interface with this module the
  * three methods used (`configure`, `getLatest`, `get`) are found and invoked
  * reflectively. Because a class name comes from configuration, the loading is
  * kept narrow: the class is loaded without being initialised, and it is
  * constructed only after it is shown to have the methods the call needs, with
  * the right shape, so naming an unrelated class on the classpath runs none of
  * its code. A job that never uses a `registry://` reference never touches any
  * of this.
  */
object ContractReference {

  val Scheme = "registry://"

  /** The version segment meaning "whatever the registry currently considers latest". */
  val LatestVersion = "latest"

  /** `registry-client`'s own implementation, used when `registryClientClass` is unset. */
  val DefaultRegistryClientClass = "com.invaract.registryclient.HttpContractRegistryClient"

  /** `None` for an ordinary file path, `Some((id, version))` for a reference. Both are returned as written
    * (`version` may be the literal `"latest"`), so a caller can tell "not a reference" from "malformed".
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

  /** The contract `raw` names: parsed from a file for an ordinary path, fetched from the registry for a reference. */
  def resolve(raw: String, config: ConfigSource): Contract =
    parse(raw) match {
      case Some((contractId, version)) => fetchFromRegistry(contractId, version, config)
      case None                        => ContractParser.parseFile(raw)
    }

  private def fetchFromRegistry(contractId: String, version: String, config: ConfigSource): Contract = {
    val registryUrl = config.get(InvaractConf.RegistryUrl).getOrElse(
      throw new IllegalStateException(
        s"A 'registry://$contractId@$version' contract reference requires '${config.describe(InvaractConf.RegistryUrl)}' to be set."
      )
    )
    val className = config.get(InvaractConf.RegistryClientClass).getOrElse(DefaultRegistryClientClass)
    val clientClass = loadClientClass(className)
    val configure = method(clientClass, className, "configure", classOf[String])
    val fetch =
      if (version == LatestVersion) method(clientClass, className, "getLatest", classOf[String])
      else method(clientClass, className, "get", classOf[String], classOf[ContractVersion])
    val client = construct(clientClass, className)
    configure.invoke(client, registryUrl)
    val fetched =
      if (version == LatestVersion) fetch.invoke(client, contractId)
      else fetch.invoke(client, contractId, ContractVersion.parse(version))
    fetched.asInstanceOf[Contract]
  }

  /** Loaded without initialising it: a name that is not a registry client must not get to run its static initialiser. */
  private def loadClientClass(className: String): Class[_] =
    try Class.forName(className, false, getClass.getClassLoader)
    catch {
      case e: ClassNotFoundException =>
        throw new IllegalStateException(
          s"Could not find registry client class '$className' on the classpath - " +
            "add the invaract-registry-client jar via spark-submit --jars.",
          e
        )
      case e: LinkageError =>
        throw new IllegalStateException(s"Could not load registry client class '$className': ${e.getMessage}", e)
    }

  /** A public method that returns what a client's method must, found before anything is constructed. */
  private def method(clientClass: Class[_], className: String, name: String, params: Class[_]*): Method = {
    val found =
      try clientClass.getMethod(name, params: _*)
      catch {
        case e: NoSuchMethodException =>
          throw new IllegalStateException(
            s"'$className' is not a registry client: it has no public method $name(${params.map(_.getSimpleName).mkString(", ")}).",
            e
          )
      }
    if (name != "configure" && !classOf[Contract].isAssignableFrom(found.getReturnType)) {
      throw new IllegalStateException(s"'$className' is not a registry client: $name must return a Contract, not ${found.getReturnType.getName}.")
    }
    found
  }

  private def construct(clientClass: Class[_], className: String): AnyRef =
    try clientClass.getDeclaredConstructor().newInstance().asInstanceOf[AnyRef]
    catch {
      case e: ReflectiveOperationException =>
        throw new IllegalStateException(
          s"Could not instantiate registry client '$className' (it needs a public no-arg constructor).",
          e
        )
    }
}
