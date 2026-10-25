// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.verification

import com.invaract.ir.{Aggregate, ColumnRef, Filter, Join, Plan, Read, UnknownPlan}

import scala.collection.mutable

/** What the verifiers need to know about the *shape* of one `ir.Plan`, gathered
  * in a single traversal. Each of `StructuralVerifier` (reads, unknown nodes),
  * `PlanRuleVerifier` (aggregates, joins, filters, once per declared rule),
  * `StaticDataQualityVerifier`, `RoleConsistencyVerifier` and `ContractInference`
  * used to walk the whole plan itself — one walk per question, several questions
  * per rule — on every write the check rule sees. `ContractEnforcementRule` now
  * builds one of these per check and hands it to all of them.
  *
  * Every list is in pre-order, left to right — the same order the verifiers'
  * own recursive walks produced — so nothing that iterates them changes order.
  * The traversal is iterative (an explicit stack), so a very deep plan cannot
  * overflow the JVM stack here the way a recursive walk over it could.
  *
  * A plan may share a node between several parents: an adapter whose engine
  * describes a job as a graph (Beam: a collection read by two branches that
  * later rejoin) hands the same `Plan` object to both. Each node object is
  * visited once, where it is first reached, so a plan of `n` distinct nodes is
  * walked in `n` steps however many paths lead to them - walked as a tree, `k`
  * branches that rejoin in a row would be `2^k` paths. Nodes are told apart by
  * reference, not equality: a plan is a case class, so equality and hashing
  * would themselves walk every path, and two equal nodes that are not shared
  * (a self-join reading the same table twice) are two reads.
  */
private[invaract] final class PlanFacts private (
    val plan: Plan,
    val reads: List[Read],
    val unknownPlans: List[UnknownPlan],
    val aggregates: List[Aggregate],
    val joins: List[Join],
    val filters: List[Filter]
) {

  /** Every column referenced in any `Filter` or `Join` condition anywhere in the
    * plan — the "read only to gate or match rows, never to compute output" signal
    * `PlanRuleVerifier.collectConditionReferences` documents.
    */
  lazy val conditionReferences: Set[ColumnRef] = {
    val filterRefs = filters.flatMap(_.condition.references)
    val joinRefs = joins.flatMap(_.condition.toList.flatMap(_.references))
    (filterRefs ++ joinRefs).toSet
  }

  /** Every name the dataset at a read location answers to (`LocationMatching.namesOf`): the location itself and the table
    * name of each read there that resolved to a catalog table. A location no read reports answers to itself only - a
    * qualifier that `locationResolver` could not resolve, say. */
  lazy val namesOf: String => List[String] = {
    val byLocation: Map[String, List[String]] = reads.groupBy(_.dataset.location).map { case (location, at) =>
      location -> (location :: at.flatMap(r => r.catalog.flatMap(LocationMatching.tableName))).distinct
    }
    location => byLocation.getOrElse(location, List(location))
  }

  /** Turns a lineage/condition `ColumnRef.qualifier` into the location of the
    * `Read` it stands for — see `PlanRuleVerifier.locationResolver` for why that
    * resolution exists and why an ambiguous scope is left unresolved.
    */
  lazy val locationResolver: String => String = {
    val locationByScope: Map[String, String] = reads
      .map(r => r.alias.getOrElse(r.dataset.location) -> r.dataset.location)
      .distinct
      .groupBy(_._1)
      .collect { case (scope, occurrences) if occurrences.size == 1 => scope -> occurrences.head._2 }
    qualifier => locationByScope.getOrElse(qualifier, qualifier)
  }
}

private[invaract] object PlanFacts {

  def of(plan: Plan): PlanFacts = {
    val reads = List.newBuilder[Read]
    val unknownPlans = List.newBuilder[UnknownPlan]
    val aggregates = List.newBuilder[Aggregate]
    val joins = List.newBuilder[Join]
    val filters = List.newBuilder[Filter]

    // Pre-order with children visited left to right: push a node's children in
    // reverse so the leftmost is popped first. A node with children already
    // visited (shared with a parent walked earlier) is skipped.
    val visited = java.util.Collections.newSetFromMap(new java.util.IdentityHashMap[Plan, java.lang.Boolean]())
    val stack = mutable.ArrayStack[Plan](plan)
    while (stack.nonEmpty) {
      val node = stack.pop()
      if (node.children.isEmpty || visited.add(node)) {
        node match {
          case r: Read        => reads += r
          case u: UnknownPlan => unknownPlans += u
          case a: Aggregate   => aggregates += a
          case j: Join        => joins += j
          case f: Filter      => filters += f
          case _              => ()
        }
        node.children.reverseIterator.foreach(stack.push)
      }
    }
    new PlanFacts(plan, reads.result(), unknownPlans.result(), aggregates.result(), joins.result(), filters.result())
  }
}
