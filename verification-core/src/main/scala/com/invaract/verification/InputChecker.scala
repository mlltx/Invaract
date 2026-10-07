// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.verification

import com.invaract.contract.{Contract, Dataset, LogicalSchema}
import com.invaract.ir.{Read, UnknownPlan}


/** The input half of `StructuralVerifier.verify`: was every declared input
  * actually read, was anything undeclared read, and do the inputs' schemas and
  * catalog registrations match what the contract declares? Each question is its
  * own small function below; `check` strings them together in the order the
  * violations have always been reported (missing, undeclared, schema, catalog).
  *
  * Every "which declared dataset does this actual location belong to" question
  * is a lookup in a `LocationIndex` built once per check rather than a scan that
  * re-normalizes both sides (see `LocationMatching`); results keep declaration
  * order, so the violations come out in the same order a linear scan gave.
  */
private[invaract] object InputChecker {

  /** `violations` in report order, plus the report-only `unverifiable` inputs
    * (a declared input that could not be confirmed read, but whose absence is
    * not provable either - see `unverifiableEvidenceFor`).
    */
  final case class Findings(violations: List[Violation], unverifiable: List[UnverifiableInput])

  /** A contract's declared inputs, indexed once by location. */
  final class Declared private (val inputs: List[Dataset], val index: LocationIndex[Dataset])
  object Declared {
    def apply(inputs: List[Dataset]): Declared = new Declared(inputs, LocationIndex(inputs.map(i => i.location -> i)))
  }

  /** `scopedOutput`: the declared output the write lands on, if any - it narrows
    * which declared inputs this write is expected to draw on (see
    * `StructuralVerifier`'s "Which inputs a write is checked against" doc).
    */
  def check(
      contract: Contract,
      facts: PlanFacts,
      scopedOutput: Option[Dataset],
      inputSchemas: List[(String, LogicalSchema)],
      options: VerificationOptions,
      caseSensitive: Boolean,
      lineageBoundaryTypes: Set[String]
  ): Findings = {
    val all = Declared(contract.inputs)
    val scoped = if (scopedOutput.isEmpty) all else Declared(contract.inputsFor(scopedOutput.get))
    val readLocations = facts.reads.map(_.dataset.location).distinct

    val declaredButNotRead = notRead(scoped, readLocations)
    val (missing, unverifiable) = classifyUnread(declaredButNotRead, facts.unknownPlans, scopedOutput, contract.outputs.size, lineageBoundaryTypes)

    val undeclared = if (options.rejectUndeclaredInputs) undeclaredReads(readLocations, all, scoped, scopedOutput) else Nil
    val schema = schemaFindings(all, inputSchemas, options.rejectUndeclaredFields, caseSensitive)
    val catalog = catalogFindings(all, facts.reads)

    Findings(missing ++ undeclared ++ schema ++ catalog, unverifiable)
  }

  /** The scoped declared inputs none of `readLocations` matches. */
  private[invaract] def notRead(scoped: Declared, readLocations: List[String]): List[Dataset] = {
    val read: Set[Int] = readLocations.flatMap(scoped.index.matchingIndices).toSet
    scoped.inputs.zipWithIndex.collect { case (input, i) if !read.contains(i) => input }
  }

  /** A declared input with no matching `Read` is normally confidently missing -
    * but not when there's real evidence it was read behind a lineage boundary
    * this plan can no longer see through (see `unverifiableEvidenceFor`). That
    * case is reported as `UnverifiableInput` instead: honest uncertainty, not a
    * false-positive `MISSING_INPUT` blocking a job that reads its input just fine.
    */
  private[invaract] def classifyUnread(
      unread: List[Dataset],
      unknownPlans: List[UnknownPlan],
      scopedOutput: Option[Dataset],
      declaredOutputCount: Int,
      lineageBoundaryTypes: Set[String]
  ): (List[Violation], List[UnverifiableInput]) = {
    // The evidence is about the plan, not the individual input: computed once,
    // and only if some declared input really is unread.
    lazy val evidence = unverifiableEvidenceFor(unknownPlans, lineageBoundaryTypes)
    val classified = unread.map(input => input -> evidence)
    val nudge = scopedOutput.exists(_.derivedFrom.isEmpty) && declaredOutputCount > 1
    val missing = classified.collect { case (input, None) => Violations.missingInput(input, nudge) }
    val unverifiable = classified.collect { case (input, Some(sourceTypes)) =>
      UnverifiableInput(input.name, input.location, sourceTypes)
    }
    (missing, unverifiable)
  }

  /** `Some(sourceTypes)` when `unknownPlans` contains an unresolved lineage
    * boundary - an `ir.UnknownPlan` whose `sourceType` is one of
    * `lineageBoundaryTypes`, the set the engine adapter declares (for Spark,
    * `CheckpointRegistry.BoundarySourceTypes`; an engine with no such concept
    * passes the empty set, and no unread input is ever "unverifiable") - the only thing
    * that makes an unread input unverifiable rather than missing; `None`
    * (confidently missing) otherwise. The evidence is about the plan, not the
    * individual input: an unresolved boundary hides *every* read made before it,
    * so any unread input might be behind it.
    */
  private[invaract] def unverifiableEvidenceFor(unknownPlans: List[UnknownPlan], lineageBoundaryTypes: Set[String]): Option[List[String]] = {
    val boundaries = unknownPlans.map(_.sourceType).filter(lineageBoundaryTypes.contains).distinct
    if (boundaries.isEmpty) None else Some(boundaries)
  }

  /** `UNDECLARED_INPUT` for every read that isn't one of the scoped declared
    * inputs. A read that IS a declared contract input, just not one the write's
    * own output is `derivedFrom`, is a different mistake from a wholly undeclared
    * read: the fix is to the mapping (or the transformation), not to add another
    * input - so it gets its own wording.
    */
  private[invaract] def undeclaredReads(readLocations: List[String], all: Declared, scoped: Declared, scopedOutput: Option[Dataset]): List[Violation] =
    readLocations
      .filterNot(scoped.index.matchesAny)
      .map { loc =>
        val notThisOutputsInput = for {
          declared <- all.index.first(loc)
          output   <- scopedOutput
        } yield (declared, output)
        notThisOutputsInput match {
          case Some((declared, output)) => Violations.undeclaredInputNotSourceOf(loc, declared, output)
          case None                     => Violations.undeclaredInput(loc)
        }
      }

  /** Schema findings for every declared input a supplied schema matches - the
    * first supplied schema (in the caller's order) whose location matches each
    * input. An input no schema was supplied for has nothing to check; its
    * existence was already reported by `classifyUnread`.
    */
  private[invaract] def schemaFindings(
      all: Declared,
      inputSchemas: List[(String, LogicalSchema)],
      rejectUndeclaredFields: Boolean,
      caseSensitive: Boolean
  ): List[Violation] = {
    val schemaForInput: Map[Int, LogicalSchema] =
      inputSchemas.foldLeft(Map.empty[Int, LogicalSchema]) { case (acc, (loc, schema)) =>
        all.index.matchingIndices(loc).foldLeft(acc)((m, i) => if (m.contains(i)) m else m + (i -> schema))
      }
    all.inputs.zipWithIndex.flatMap { case (input, i) =>
      schemaForInput.get(i) match {
        case Some(schema) => SchemaChecker.check(input.schema.fields, schema, SchemaChecker.Side.Input, input.location, rejectUndeclaredFields, caseSensitive)
        case None         => Nil
      }
    }
  }

  /** Catalog findings, read side: each `ir.Read` already carries its own resolved
    * `CatalogIdentity` (populated at translation time), matched to a declared
    * input by location - the first read (in plan order) whose location matches.
    * An input with no matching read at all was already reported as `MISSING_INPUT`:
    * nothing more useful to say about its catalog identity.
    */
  private[invaract] def catalogFindings(all: Declared, reads: List[Read]): List[Violation] = {
    val readForInput: Map[Int, Read] =
      reads.foldLeft(Map.empty[Int, Read]) { (acc, read) =>
        all.index.matchingIndices(read.dataset.location).foldLeft(acc)((m, i) => if (m.contains(i)) m else m + (i -> read))
      }
    all.inputs.zipWithIndex.flatMap { case (input, i) =>
      (input.catalog, readForInput.get(i)) match {
        case (Some(req), Some(read)) => CatalogChecker.check(req, read.catalog, input.location, SchemaChecker.Side.Input)
        case _                        => Nil
      }
    }
  }
}
