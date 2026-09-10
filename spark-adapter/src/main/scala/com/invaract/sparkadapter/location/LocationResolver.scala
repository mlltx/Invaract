// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter.location

/** Raised when a `ref://<id>` in a contract can't be resolved to a literal
  * location — an unknown id, an unreachable resolver, or no resolver
  * configured at all. Always thrown from `ContractLocationResolution.resolve`,
  * before a `SparkSession` is built (see that class's doc) — the same
  * "fail loudly, at setup time" treatment a malformed contract or a
  * misconfigured notification sink already gets, so a bad reference never
  * silently falls through to verifying against the wrong (or a literal
  * `"ref://..."`) location.
  */
class LocationResolutionException(message: String, cause: Throwable = null) extends RuntimeException(message, cause)

/** Resolves a contract's `ref://<id>` location references to real, literal
  * locations at job startup — see `LocationRef`'s doc for the syntax, and
  * `ContractLocationResolution` for where this plugs in. Implementations
  * must be fail-closed: `resolve` either returns a genuine literal
  * location or throws `LocationResolutionException`; it must never return
  * a placeholder, a stale cached value, or anything else that could pass
  * silently for a real location.
  */
trait LocationResolver {
  def resolve(id: String): String
}

/** The resolver a job gets by default when it never configures one — every
  * `resolve` call fails immediately. This exists so
  * `ContractLocationResolution.resolve` can *always* run unconditionally,
  * whether or not the caller wired up real resolution: a contract with no
  * `ref://` entries is unaffected either way, and a contract that does
  * declare one fails with a clear, actionable message instead of a
  * confusing downstream `MissingInput`/`OutputLocationMismatch` violation
  * against the literal string `"ref://..."`.
  */
object NoOpLocationResolver extends LocationResolver {
  override def resolve(id: String): String =
    throw new LocationResolutionException(
      s"Contract declares '${LocationRef.Scheme}$id' but no LocationResolver was configured for this job."
    )
}
