// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invariant Contributors

package com.example.ir

/** Whether, and how, a row-level DML operation deletes rows. A sealed
  * trait rather than `Option[Expr]` because "this operation includes no
  * delete at all" and "this operation deletes unconditionally" are both
  * real, distinct states a bare `Option` can't tell apart — a plain
  * `deleteCondition: Option[Expr] = None` would leave "not a delete" and
  * "unconditional delete" looking identical to a caller.
  */
sealed trait DeleteScope
object DeleteScope {

  /** This operation includes no delete (e.g. a bare UPDATE). */
  case object NotApplicable extends DeleteScope

  /** Deletes every row it reaches, with no filtering predicate at all
    * (`DELETE FROM t` / `.delete()` with no `WHERE`).
    */
  case object Unconditional extends DeleteScope

  /** Deletes only rows matching `condition`. */
  case class Conditional(condition: Expr) extends DeleteScope
}

/** Structural facts about a *standalone* row-level DML operation (UPDATE,
  * DELETE, or a MERGE's own match condition) that `Write`'s "replace/
  * append the output of a query" shape has no vocabulary for. Produced by
  * a front-end translator alongside (not instead of) the ordinary `Write`
  * node the same command already translates to — `RowMutation` exists
  * purely so a rule verifier can check a contract's declared DML rules
  * against the operation's actual shape; the committed schema/format/
  * location remains `Write`'s job, unchanged.
  *
  * Deliberately narrow, mirroring the three rules it exists to support
  * (see `com.example.contract.RuleType`, in the `contract` module):
  *
  *   - `matchCondition`: a MERGE's `ON` clause, kept as a full `Expr` so
  *     a verifier can read the columns it references — not itself
  *     evaluated or simplified.
  *   - `delete`: see `DeleteScope`.
  *   - `updatedColumns`: the column names a standalone UPDATE assigns.
  *
  * A MERGE's individual `WHEN MATCHED`/`WHEN NOT MATCHED [BY SOURCE]`
  * clauses (each with its own condition and, for an update clause, its
  * own assigned columns) are not decomposed per-clause here — `delete`/
  * `updatedColumns` are only populated for a genuinely standalone
  * UPDATE/DELETE command, not derived from a MERGE's clauses. See
  * ROADMAP.md's "Full semantic DML verification" item for this and other
  * still-open scope.
  */
case class RowMutation(
  matchCondition: Option[Expr] = None,
  delete: DeleteScope = DeleteScope.NotApplicable,
  updatedColumns: List[String] = Nil
)
