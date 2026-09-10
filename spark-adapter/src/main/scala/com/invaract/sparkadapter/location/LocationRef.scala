// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter.location

/** Recognizes a contract dataset's `location` value as either a literal
  * physical location (the only kind that has ever existed) or a reference
  * to be resolved at runtime via a `LocationResolver`, before verification
  * ever sees it.
  *
  * A referenced location is written as `ref://<id>` — the same field a
  * literal location already occupies, not a second key — so
  * `contract`'s own `ContractParser`/`ContractValidator`/JSON Schema need
  * no changes at all: `location` stays a required, non-empty string either
  * way, and `contract` never learns this concept exists. `ref://` was
  * chosen deliberately over alternatives like `catalog://` (collides with
  * the unrelated, already-overloaded notion of a Spark/Hive/Unity/Iceberg
  * *catalog*) or `provider://` (collides with Spark's own DataSource
  * "provider" vocabulary) — it doesn't collide with any real storage
  * scheme a contract's `location` might otherwise hold (`s3://`,
  * `hdfs://`, `abfss://`, `gs://`, `dbfs://`, `jdbc:...`), so a resolved
  * literal can never be mistaken for an unresolved reference or vice
  * versa.
  */
object LocationRef {
  val Scheme = "ref://"

  /** `Some(id)` if `location` is a reference (`ref://<id>`, with a
    * non-empty `id`); `None` if it's a literal location — including the
    * degenerate `"ref://"` with no id, which is treated as a literal
    * rather than a reference to an empty-string id.
    */
  def id(location: String): Option[String] =
    if (location.startsWith(Scheme)) {
      val stripped = location.stripPrefix(Scheme)
      if (stripped.nonEmpty) Some(stripped) else None
    } else {
      None
    }
}
