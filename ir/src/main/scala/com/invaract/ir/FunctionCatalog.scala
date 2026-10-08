// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.ir

/** One function this IR knows something about beyond its name: the properties a verifier or the
  * fingerprint needs and must not have to guess per engine.
  *
  * @param name the canonical, upper-case name an adapter writes into `Function.name`.
  * @param nonDeterministic evaluating it twice over the same input can give different results
  *   (a generated id, a random number, the clock, a read-order-dependent value).
  * @param seedBearing a call to it carries an analyzer-injected random seed as its sole argument,
  *   indistinguishable from a user-written literal once the plan is analyzed - so the fingerprint
  *   must leave that argument out, or the same code would hash differently on every run.
  */
final case class CanonicalFunction(name: String, description: String, nonDeterministic: Boolean, seedBearing: Boolean = false)

/** The functions the IR gives a meaning to, in one engine-independent place.
  *
  * An IR `Function` carries whatever name its engine's translator wrote. Before this catalog that was
  * Spark's own `prettyName`, so "is this call non-deterministic" was answered by a list of Spark names
  * in the fingerprint module: the same logic on BigQuery - whose `GENERATE_UUID()` is Spark's
  * `uuid()` - would have been classified deterministic, and fingerprinted differently, silently.
  *
  * Now each adapter maps its engine's names onto this vocabulary (`FunctionAliases`) when it
  * translates, and everything downstream (non-determinism, the fingerprint's seed handling) asks the
  * catalog. A function not in the catalog is not flagged: the catalog lists what is *known*, so an
  * adapter owns making sure every non-deterministic function its engine has is aliased onto it (the
  * conformance kit checks that through the engine, not by trusting the table).
  */
object FunctionCatalog {

  val Uuid = CanonicalFunction("UUID", "A new random unique identifier for every row.", nonDeterministic = true)
  val Rand = CanonicalFunction("RAND", "A random number in [0, 1), uniform.", nonDeterministic = true, seedBearing = true)
  val Randn = CanonicalFunction("RANDN", "A random number, standard normal.", nonDeterministic = true, seedBearing = true)
  val CurrentTimestamp = CanonicalFunction("CURRENT_TIMESTAMP", "The wall-clock time the query runs.", nonDeterministic = true)
  val CurrentDate = CanonicalFunction("CURRENT_DATE", "The wall-clock date the query runs.", nonDeterministic = true)
  val UnixTimestamp = CanonicalFunction("UNIX_TIMESTAMP", "The current time as epoch seconds (when called without arguments).", nonDeterministic = true)
  val RowId = CanonicalFunction("ROW_ID", "A per-row generated number that depends on how the data is split and read.", nonDeterministic = true)
  val SourceFileName = CanonicalFunction("SOURCE_FILE_NAME", "The name of the file the current row was read from.", nonDeterministic = true)
  val PartitionId = CanonicalFunction("PARTITION_ID", "The id of the partition the current row is processed in.", nonDeterministic = true)
  val SourceBlockStart = CanonicalFunction("SOURCE_BLOCK_START", "The start offset of the block of the file the current row was read from.", nonDeterministic = true)
  val SourceBlockLength = CanonicalFunction("SOURCE_BLOCK_LENGTH", "The length of the block of the file the current row was read from.", nonDeterministic = true)
  val Shuffle = CanonicalFunction("SHUFFLE", "A random permutation of an array.", nonDeterministic = true)

  val all: List[CanonicalFunction] =
    List(Uuid, Rand, Randn, CurrentTimestamp, CurrentDate, UnixTimestamp, RowId, SourceFileName, PartitionId, SourceBlockStart, SourceBlockLength, Shuffle)

  private val byName: Map[String, CanonicalFunction] = all.map(f => f.name -> f).toMap

  /** The catalog entry for `name` (case-insensitive), if it has one. */
  def lookup(name: String): Option[CanonicalFunction] = byName.get(name.toUpperCase(java.util.Locale.ROOT))

  def isNonDeterministic(name: String): Boolean = lookup(name).exists(_.nonDeterministic)

  def isSeedBearing(name: String): Boolean = lookup(name).exists(_.seedBearing)

  /** Every catalog name that is non-deterministic, canonical (upper-case) spelling. */
  val nonDeterministicNames: Set[String] = all.filter(_.nonDeterministic).map(_.name).toSet
}

/** How one engine's own function names map onto the catalog. An adapter holds one and applies it
  * wherever it writes a `Function` into the IR; a name with no alias is kept as the engine spelled it
  * (upper-cased - the IR's convention), so a function the catalog does not know passes through
  * unchanged rather than being guessed at.
  *
  * Construction refuses an alias whose target is not a catalog name - a typo there would silently
  * leave a non-deterministic function unflagged.
  *
  * @param engine the engine's short name, for the error message.
  * @param aliases engine-native name (any case) -> canonical name.
  */
final case class FunctionAliases(engine: String, aliases: Map[String, String]) {
  aliases.foreach { case (native, canonical) =>
    require(FunctionCatalog.lookup(canonical).exists(_.name == canonical), s"$engine's alias '$native' targets '$canonical', which is not a canonical function name")
  }

  private val byNative: Map[String, String] = aliases.map { case (native, canonical) => native.toUpperCase(java.util.Locale.ROOT) -> canonical }

  /** The canonical name for `nativeName`, or `nativeName` upper-cased when there is no alias. */
  def canonicalName(nativeName: String): String = {
    val upper = nativeName.toUpperCase(java.util.Locale.ROOT)
    byNative.getOrElse(upper, upper)
  }
}

object FunctionAliases {

  /** An engine whose function names already are the catalog's, or that has none of them. */
  def identity(engine: String): FunctionAliases = FunctionAliases(engine, Map.empty)
}
