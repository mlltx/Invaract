// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.contract

/** The one canonical form of a dataset location, used wherever two locations are compared: a contract's declared location
  * against the one an engine reports (`verification-core`'s `LocationMatching`), two contracts' locations against each other
  * (`CrossContractValidator`), and a location against an organizational policy.
  *
  * Writing the same place two ways must not change whether it matches, so the form:
  *   - turns `\` into `/` (a contract written on Windows) and trims surrounding space;
  *   - drops a `file:` scheme, which a local-file engine adds to a plain path (`file:/data/x`, `file:///data/x` are `/data/x`);
  *   - lower-cases a URL scheme and folds the spellings of one store into one (`s3a://`, `s3n://` are `s3://`; `abfss://`
  *     is `abfs://`; `wasbs://` is `wasb://`): they name the same objects, through a different client;
  *   - drops empty and `.` segments, so a repeated `/`, a trailing `/` and a leading `./` change nothing.
  *
  * It leaves case alone (paths and object keys are case-sensitive) and leaves `..` alone (what it climbs out of depends on
  * links the location cannot see).
  */
object Locations {

  /** A URL scheme with `//`: two or more characters, so a Windows drive (`C:`) is never taken for one. */
  private val UrlScheme = """^([A-Za-z][A-Za-z0-9+.-]+)://(.*)$""".r

  private val WindowsDrive = """^[A-Za-z]:/.*""".r

  /** Schemes that are another client's spelling of the same store. */
  private val SameStore = Map("s3a" -> "s3", "s3n" -> "s3", "abfss" -> "abfs", "wasbs" -> "wasb")

  def canonical(location: String): String = {
    val slashed = location.trim.replace('\\', '/')
    val local = if (slashed.regionMatches(true, 0, "file:", 0, 5)) slashed.substring(5) else slashed
    local match {
      case UrlScheme(scheme, rest) =>
        val lower = scheme.toLowerCase
        s"${SameStore.getOrElse(lower, lower)}://${segments(rest)}"
      case _ => (if (local.startsWith("/")) "/" else "") + segments(local)
    }
  }

  private def segments(path: String): String = path.split('/').filter(s => s.nonEmpty && s != ".").mkString("/")

  /** Whether `location` names one place on its own - a URL (`s3://bucket/orders`), a path from the root (`/data/orders`), or a
    * Windows drive path (`C:/data/orders`) - rather than a relative one (`orders`, `sales/orders`), which matches any location
    * ending in it. A `ref://` reference counts as a URL; it is resolved to a real location before it is compared. */
  def isAbsolute(location: String): Boolean = {
    val c = canonical(location)
    c.startsWith("/") || UrlScheme.pattern.matcher(c).matches() || WindowsDrive.pattern.matcher(c).matches()
  }
}
