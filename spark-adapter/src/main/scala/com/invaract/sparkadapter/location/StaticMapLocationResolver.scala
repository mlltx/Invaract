// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter.location

import java.io.FileInputStream
import java.util.{Properties => JProperties}

import scala.collection.JavaConverters._

/** A `LocationResolver` backed by a fixed, in-memory `id -> location` map —
  * the "resolved as an argument passed into the application" mechanism,
  * as opposed to `ref://` ids resolved with an outbound HTTP call. No
  * network dependency, so a contract using it stays exercisable entirely
  * offline (`./dev/test` included) — see `ContractLocationResolution`'s
  * doc for where this plugs into contract loading.
  */
class StaticMapLocationResolver(mapping: Map[String, String]) extends LocationResolver {
  override def resolve(id: String): String =
    mapping.getOrElse(
      id,
      throw new LocationResolutionException(s"No location mapping found for id '$id'")
    )
}

object StaticMapLocationResolver {

  /** Reads `path` as a plain Java `.properties` file, `id=location` per
    * line — the same `.properties` convention
    * `com.invaract.sparkadapter.notification.NotificationConfig` already
    * uses for deployment-environment configuration (an id-to-path mapping
    * is exactly that: it varies per environment a contract does not, so it
    * doesn't belong in the contract document itself). Throws if the file
    * doesn't exist or isn't readable — the same "fail loudly, at setup
    * time" treatment `NotificationConfig.load` and `ContractParser.parseFile`
    * already give their own inputs.
    *
    * `java.util.Properties`' own format treats `\` as an escape character,
    * so a raw Windows path (`C:\data\orders`) needs its backslashes doubled
    * (`C:\\data\\orders`) or replaced with forward slashes (`C:/data/orders`)
    * to survive `load` intact.
    */
  def fromPropertiesFile(path: String): StaticMapLocationResolver = {
    val props = new JProperties()
    val in = new FileInputStream(path)
    try {
      props.load(in)
    } finally {
      in.close()
    }
    new StaticMapLocationResolver(
      props.stringPropertyNames().asScala.map(key => key -> props.getProperty(key)).toMap
    )
  }

  /** Builds a resolver from `"id=location"` strings — e.g. CLI arguments
    * passed straight through to a job, one entry per `id`. Splits on the
    * first `=` only, so a location value containing its own `=` (a query
    * string, say) is preserved intact. Throws on an entry with no `=` at
    * all, rather than silently dropping it.
    */
  def fromArgs(pairs: Seq[String]): StaticMapLocationResolver =
    new StaticMapLocationResolver(pairs.map { pair =>
      pair.indexOf('=') match {
        case -1 => throw new IllegalArgumentException(s"Expected 'id=location', got: '$pair'")
        case i  => pair.substring(0, i) -> pair.substring(i + 1)
      }
    }.toMap)
}
