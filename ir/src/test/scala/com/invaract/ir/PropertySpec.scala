// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.ir

import org.scalatest.funsuite.AnyFunSuite

/** Direct, low-level coverage of `Property.Range`'s arithmetic
  * (`negate`/`shift`/`scale`/`tighten`/`widen`) and `ColumnPropertyState`'s
  * combinators — exercised through `PropertyAnalysis` end-to-end in
  * `PropertyAnalysisSpec`, but the tie-breaking branches inside `tighten`/
  * `widen` specifically (two *competing* bounds on the same side, equal
  * values with differing inclusivity) need bounds picked so each branch's
  * outcome is actually distinguishable, which a whole-plan test tends not
  * to naturally exercise.
  */
class PropertySpec extends AnyFunSuite {

  // --- negate ------------------------------------------------------------------

  test("negate: gte becomes lte, sign flipped") {
    assert(Property.Range(gte = Some(5)).negate == Property.Range(lte = Some(-5)))
  }
  test("negate: gt becomes lt, sign flipped") {
    assert(Property.Range(gt = Some(5)).negate == Property.Range(lt = Some(-5)))
  }
  test("negate: lte becomes gte, sign flipped") {
    assert(Property.Range(lte = Some(5)).negate == Property.Range(gte = Some(-5)))
  }
  test("negate: lt becomes gt, sign flipped") {
    assert(Property.Range(lt = Some(5)).negate == Property.Range(gt = Some(-5)))
  }
  test("negate: both sides at once, and an absent bound stays absent") {
    assert(Property.Range(gte = Some(0), lte = Some(10)).negate == Property.Range(gte = Some(-10), lte = Some(0)))
    assert(Property.Range().negate == Property.Range())
  }

  // --- shift -------------------------------------------------------------------

  test("shift: a positive constant moves both bounds up") {
    assert(Property.Range(gte = Some(0), lte = Some(10)).shift(5) == Property.Range(gte = Some(5), lte = Some(15)))
  }
  test("shift: a negative constant moves both bounds down") {
    assert(Property.Range(gte = Some(0)).shift(-5) == Property.Range(gte = Some(-5)))
  }

  // --- scale -------------------------------------------------------------------

  test("scale: a positive factor scales both bounds") {
    assert(Property.Range(gte = Some(2), lte = Some(10)).scale(3) == Property.Range(gte = Some(6), lte = Some(30)))
  }
  test("scale: rejects a negative factor") {
    assertThrows[IllegalArgumentException](Property.Range(gte = Some(2)).scale(-1))
  }

  // --- tighten (AND: narrows toward the more restrictive side) -----------------

  test("tighten: one side has no lower bound, the other's is kept") {
    assert(Property.Range().tighten(Property.Range(gte = Some(5))) == Property.Range(gte = Some(5)))
    assert(Property.Range(gte = Some(5)).tighten(Property.Range()) == Property.Range(gte = Some(5)))
  }
  test("tighten: the larger (more restrictive) lower bound wins, on either side") {
    assert(Property.Range(gte = Some(0)).tighten(Property.Range(gte = Some(5))) == Property.Range(gte = Some(5)))
    assert(Property.Range(gte = Some(5)).tighten(Property.Range(gte = Some(0))) == Property.Range(gte = Some(5)))
  }
  test("tighten: the smaller (more restrictive) upper bound wins, on either side") {
    assert(Property.Range(lte = Some(10)).tighten(Property.Range(lte = Some(3))) == Property.Range(lte = Some(3)))
    assert(Property.Range(lte = Some(3)).tighten(Property.Range(lte = Some(10))) == Property.Range(lte = Some(3)))
  }
  test("tighten: an equal lower bound prefers the exclusive (>) variant") {
    assert(Property.Range(gte = Some(5)).tighten(Property.Range(gt = Some(5))) == Property.Range(gt = Some(5)))
    assert(Property.Range(gt = Some(5)).tighten(Property.Range(gte = Some(5))) == Property.Range(gt = Some(5)))
  }
  test("tighten: an equal upper bound prefers the exclusive (<) variant") {
    assert(Property.Range(lte = Some(5)).tighten(Property.Range(lt = Some(5))) == Property.Range(lt = Some(5)))
    assert(Property.Range(lt = Some(5)).tighten(Property.Range(lte = Some(5))) == Property.Range(lt = Some(5)))
  }
  test("tighten: equal, both-inclusive bounds stay inclusive") {
    assert(Property.Range(gte = Some(5)).tighten(Property.Range(gte = Some(5))) == Property.Range(gte = Some(5)))
    assert(Property.Range(lte = Some(5)).tighten(Property.Range(lte = Some(5))) == Property.Range(lte = Some(5)))
  }
  test("tighten: combines a lower bound from one side with an upper bound from the other") {
    assert(Property.Range(gte = Some(0)).tighten(Property.Range(lte = Some(100))) == Property.Range(gte = Some(0), lte = Some(100)))
  }

  // --- widen (union: loosens to cover both) -------------------------------------

  test("widen: either side missing a lower bound makes the result unbounded below") {
    assert(Property.Range(gte = Some(5)).widen(Property.Range()) == Property.Range())
    assert(Property.Range().widen(Property.Range(gte = Some(5))) == Property.Range())
  }
  test("widen: either side missing an upper bound makes the result unbounded above") {
    assert(Property.Range(lte = Some(5)).widen(Property.Range()) == Property.Range())
    assert(Property.Range().widen(Property.Range(lte = Some(5))) == Property.Range())
  }
  test("widen: the smaller (looser) lower bound wins, on either side") {
    assert(Property.Range(gte = Some(0)).widen(Property.Range(gte = Some(5))) == Property.Range(gte = Some(0)))
    assert(Property.Range(gte = Some(5)).widen(Property.Range(gte = Some(0))) == Property.Range(gte = Some(0)))
  }
  test("widen: the larger (looser) upper bound wins, on either side") {
    assert(Property.Range(lte = Some(3)).widen(Property.Range(lte = Some(10))) == Property.Range(lte = Some(10)))
    assert(Property.Range(lte = Some(10)).widen(Property.Range(lte = Some(3))) == Property.Range(lte = Some(10)))
  }
  test("widen: an equal lower bound with differing inclusivity prefers the inclusive (looser) variant") {
    assert(Property.Range(gte = Some(5)).widen(Property.Range(gt = Some(5))) == Property.Range(gte = Some(5)))
    assert(Property.Range(gt = Some(5)).widen(Property.Range(gte = Some(5))) == Property.Range(gte = Some(5)))
  }
  test("widen: an equal upper bound with differing inclusivity prefers the inclusive (looser) variant") {
    assert(Property.Range(lte = Some(5)).widen(Property.Range(lt = Some(5))) == Property.Range(lte = Some(5)))
    assert(Property.Range(lt = Some(5)).widen(Property.Range(lte = Some(5))) == Property.Range(lte = Some(5)))
  }
  test("widen: equal, both-exclusive bounds stay exclusive") {
    assert(Property.Range(gt = Some(5)).widen(Property.Range(gt = Some(5))) == Property.Range(gt = Some(5)))
    assert(Property.Range(lt = Some(5)).widen(Property.Range(lt = Some(5))) == Property.Range(lt = Some(5)))
  }

  // --- Range's own well-formedness guard ----------------------------------------

  test("Range construction rejects both gte and gt set at once") {
    assertThrows[IllegalArgumentException](Property.Range(gte = Some(1), gt = Some(2)))
  }
  test("Range construction rejects both lte and lt set at once") {
    assertThrows[IllegalArgumentException](Property.Range(lte = Some(1), lt = Some(2)))
  }

  // --- ColumnPropertyState.tightenWith / unionWith ------------------------------

  test("tightenWith: Refuted wins over Proven (a genuine contradiction)") {
    val a = ColumnPropertyState(notNull = NullabilityFact.Proven)
    val b = ColumnPropertyState(notNull = NullabilityFact.Refuted)
    assert(a.tightenWith(b).notNull == NullabilityFact.Refuted)
    assert(b.tightenWith(a).notNull == NullabilityFact.Refuted)
  }
  test("tightenWith: OneOf intersects, not unions") {
    val a = ColumnPropertyState(oneOf = Some(Property.OneOf(Set("A", "B", "C"), "string")))
    val b = ColumnPropertyState(oneOf = Some(Property.OneOf(Set("B", "C", "D"), "string")))
    assert(a.tightenWith(b).oneOf.contains(Property.OneOf(Set("B", "C"), "string")))
  }
  test("unionWith: NotNull is Proven only when both sides are Proven") {
    val proven = ColumnPropertyState(notNull = NullabilityFact.Proven)
    val unknown = ColumnPropertyState(notNull = NullabilityFact.Unknown)
    assert(proven.unionWith(unknown).notNull == NullabilityFact.Unknown)
    assert(proven.unionWith(proven).notNull == NullabilityFact.Proven)
  }
  test("unionWith: NotNull is Refuted only when both sides are Refuted") {
    val refuted = ColumnPropertyState(notNull = NullabilityFact.Refuted)
    val proven = ColumnPropertyState(notNull = NullabilityFact.Proven)
    assert(refuted.unionWith(proven).notNull == NullabilityFact.Unknown)
    assert(refuted.unionWith(refuted).notNull == NullabilityFact.Refuted)
  }
  test("unionWith: OneOf unions, including promoting a bare EqualsConstant into the union") {
    val a = ColumnPropertyState(equalsConstant = Some(Property.EqualsConstant("A", "string")))
    val b = ColumnPropertyState(oneOf = Some(Property.OneOf(Set("B"), "string")))
    assert(a.unionWith(b).oneOf.contains(Property.OneOf(Set("A", "B"), "string")))
  }
  test("tightenWith: unsupported propagates if either side has it") {
    val a = ColumnPropertyState(unsupported = true)
    val b = ColumnPropertyState()
    assert(a.tightenWith(b).unsupported)
    assert(b.tightenWith(a).unsupported)
  }

  test("unionWith: unsupported propagates if either side has it") {
    val a = ColumnPropertyState(unsupported = true)
    val b = ColumnPropertyState()
    assert(a.unionWith(b).unsupported)
    assert(b.unionWith(a).unsupported)
  }

  // --- Property.Length: tighten/widen -----------------------------------------

  test("Length.tighten: one side has no lower bound, the other's is kept") {
    assert(Property.Length().tighten(Property.Length(min = Some(5))) == Property.Length(min = Some(5)))
    assert(Property.Length(min = Some(5)).tighten(Property.Length()) == Property.Length(min = Some(5)))
  }
  test("Length.tighten: the larger (more restrictive) min wins, on either side") {
    assert(Property.Length(min = Some(0)).tighten(Property.Length(min = Some(5))) == Property.Length(min = Some(5)))
    assert(Property.Length(min = Some(5)).tighten(Property.Length(min = Some(0))) == Property.Length(min = Some(5)))
  }
  test("Length.tighten: the smaller (more restrictive) max wins, on either side") {
    assert(Property.Length(max = Some(10)).tighten(Property.Length(max = Some(3))) == Property.Length(max = Some(3)))
    assert(Property.Length(max = Some(3)).tighten(Property.Length(max = Some(10))) == Property.Length(max = Some(3)))
  }
  test("Length.tighten: combines a min from one side with a max from the other") {
    assert(Property.Length(min = Some(0)).tighten(Property.Length(max = Some(100))) == Property.Length(min = Some(0), max = Some(100)))
  }
  test("Length.tighten: an exact value tightens a wider min/max envelope into itself") {
    assert(Property.Length(exact = Some(10)).tighten(Property.Length(min = Some(0), max = Some(100))) == Property.Length(exact = Some(10)))
    assert(Property.Length(min = Some(0), max = Some(100)).tighten(Property.Length(exact = Some(10))) == Property.Length(exact = Some(10)))
  }
  test("Length.tighten: two equal exact values collapse to that same exact value, not a min/max pair") {
    assert(Property.Length(exact = Some(10)).tighten(Property.Length(exact = Some(10))) == Property.Length(exact = Some(10)))
  }
  test("Length.tighten: two DIFFERENT exact values produce a self-contradictory (empty, min > max) envelope, not a crash") {
    val result = Property.Length(exact = Some(5)).tighten(Property.Length(exact = Some(10)))
    assert(result == Property.Length(min = Some(10), max = Some(5)), s"expected the mechanically-combined, self-contradictory envelope, got $result")
  }
  test("Length.widen: either side missing a min makes the result unbounded below") {
    assert(Property.Length(min = Some(5)).widen(Property.Length()) == Property.Length())
    assert(Property.Length().widen(Property.Length(min = Some(5))) == Property.Length())
  }
  test("Length.widen: either side missing a max makes the result unbounded above") {
    assert(Property.Length(max = Some(5)).widen(Property.Length()) == Property.Length())
    assert(Property.Length().widen(Property.Length(max = Some(5))) == Property.Length())
  }
  test("Length.widen: the smaller of two mins wins (loosens toward covering both)") {
    assert(Property.Length(min = Some(5)).widen(Property.Length(min = Some(2))) == Property.Length(min = Some(2)))
    assert(Property.Length(min = Some(2)).widen(Property.Length(min = Some(5))) == Property.Length(min = Some(2)))
  }
  test("Length.widen: the larger of two maxes wins (loosens toward covering both)") {
    assert(Property.Length(max = Some(5)).widen(Property.Length(max = Some(20))) == Property.Length(max = Some(20)))
    assert(Property.Length(max = Some(20)).widen(Property.Length(max = Some(5))) == Property.Length(max = Some(20)))
  }
  test("Length.widen: two equal exact values collapse to that same exact value") {
    assert(Property.Length(exact = Some(10)).widen(Property.Length(exact = Some(10))) == Property.Length(exact = Some(10)))
  }
  test("Length.widen: two different exact values widen to the enclosing min/max range") {
    assert(Property.Length(exact = Some(5)).widen(Property.Length(exact = Some(10))) == Property.Length(min = Some(5), max = Some(10)))
    assert(Property.Length(exact = Some(10)).widen(Property.Length(exact = Some(5))) == Property.Length(min = Some(5), max = Some(10)))
  }
  test("Length: exact combined with min/max is rejected at construction") {
    assertThrows[IllegalArgumentException](Property.Length(exact = Some(10), min = Some(1)))
    assertThrows[IllegalArgumentException](Property.Length(exact = Some(10), max = Some(20)))
  }

  // --- ColumnPropertyState: length combining -----------------------------------

  test("tightenWith: length combines via Length.tighten, not left untouched") {
    val a = ColumnPropertyState(length = Some(Property.Length(min = Some(0))))
    val b = ColumnPropertyState(length = Some(Property.Length(max = Some(100))))
    assert(a.tightenWith(b).length.contains(Property.Length(min = Some(0), max = Some(100))))
  }
  test("tightenWith: length falls back to whichever side has it when only one does") {
    val a = ColumnPropertyState(length = Some(Property.Length(exact = Some(10))))
    val b = ColumnPropertyState()
    assert(a.tightenWith(b).length.contains(Property.Length(exact = Some(10))))
    assert(b.tightenWith(a).length.contains(Property.Length(exact = Some(10))))
  }
  test("unionWith: length combines via Length.widen, collapsing to None if only one side has it") {
    val a = ColumnPropertyState(length = Some(Property.Length(exact = Some(5))))
    val b = ColumnPropertyState(length = Some(Property.Length(exact = Some(10))))
    assert(a.unionWith(b).length.contains(Property.Length(min = Some(5), max = Some(10))))
    val c = ColumnPropertyState()
    assert(a.unionWith(c).length.isEmpty, "a length known on only one Union branch must not survive as a false guarantee")
  }
}
