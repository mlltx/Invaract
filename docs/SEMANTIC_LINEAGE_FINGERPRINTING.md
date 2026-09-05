# Semantic Lineage Fingerprinting — Design

**Status:** Design only. Nothing in this document is implemented. It
specifies a canonicalisation and fingerprinting scheme over the existing
transformation IR (`ir/`, see [docs/TRANSFORMATION_IR.md](TRANSFORMATION_IR.md))
precise enough to be built from directly, but no `fingerprint` module,
build wiring, or code exists yet.

**Scope.** This document covers exactly two of the six stages in the
architecture below — canonicalisation and fingerprinting — and nothing
else:

```
Spark Logical Plan
        ↓
Semantic Lineage Model        (exists: ir.Plan / ir.Expr / ir.Lineage)
        ↓
Canonical Representation      ← this document
        ↓
Fingerprint                   ← this document
        ↓
Compare with previous fingerprint     (future work, not designed here)
        ↓
Semantic Change Detection             (future work, not designed here)
```

Persistence, publication, remote comparison services, CI/CD workflows,
Spark plan extraction, and Virtual Data Environment functionality are all
explicitly out of scope. Where a decision here has a consequence for one
of those later stages (e.g. "the fingerprint must carry its own version
number so a future comparator can refuse to compare across versions"),
that consequence is noted, but the comparator itself is not designed.

**Grounding.** Every design decision below is written against the actual
types in `ir/src/main/scala/com/invaract/ir/` (`Expr.scala`, `Plan.scala`,
`Identifiers.scala`, `Lineage.scala`) as they exist today, not a
hypothetical or idealized IR. Where the real IR has a gap that limits what
fingerprinting can do (e.g. no first-class non-determinism flag), that gap
is named as a limitation rather than papered over.

---

## 1. Recommended fingerprinting architecture

A new module, conceptually `fingerprint/` (sibling to `contract`/`ir`/
`spark-adapter`), depending only on `ir` — no Spark dependency, the same
engine-independence `ir` itself has from `contract`. This keeps
fingerprinting usable by any future front end that produces `ir.Plan`
(not just Spark), exactly as the IR's own design intends downstream
consumers to be written once against the IR rather than once per engine.
Because a fingerprint is a claim about the transformation's public
meaning, this module would join `contract`/`ir`/`spark-adapter` under
CLAUDE.md's Mutation Testing and API Compatibility requirements once
implemented — noted here as a consequence of the architecture, not
designed in this document.

The module has three responsibilities, kept as separate concerns so each
can be tested independently:

1. **Canonicalisation** — a pure function from `ir.Plan`/`ir.Expr` (and,
   for the per-output lineage layer, `ir.Lineage.ColumnLineage`) to a
   small, engine-agnostic tree type, `CanonicalNode`, that contains only
   information that should affect a fingerprint.
2. **Encoding** — a pure function from `CanonicalNode` to a byte string,
   using an unambiguous, length-prefixed, tagged encoding — never
   `.toString`, never delimiter-based concatenation.
3. **Hashing** — wraps the encoded bytes in a versioned digest,
   `Fingerprint(version, algorithm, value)`.

```scala
package com.invaract.fingerprint

sealed trait CanonicalNode
final case class CTag(tag: String, fields: List[CanonicalNode]) extends CanonicalNode
final case class CLeaf(bytes: Array[Byte]) extends CanonicalNode

object Canonicalizer {
  /** One walk over the whole plan, building the alias/self-join scope
    * table (see §2.3) once, before canonicalizing any subtree. */
  def buildScopeTable(plan: ir.Plan): Map[String, String]

  def canonicalizePlan(plan: ir.Plan, scope: Map[String, String]): CanonicalNode
  def canonicalizeExpr(expr: ir.Expr, scope: Map[String, String]): CanonicalNode
  def canonicalizeLineage(cl: ir.Lineage.ColumnLineage, scope: Map[String, String]): CanonicalNode
}

case class Fingerprint(version: Int, algorithm: String, value: String)

object FingerprintHasher {
  def hash(node: CanonicalNode): Fingerprint
}
```

A `TransformationFingerprinter` composes these into the hierarchy in §3.
The design deliberately does **not** produce one monolithic
"fingerprint the whole plan" function as the only entry point: the
canonicalizer works on any subtree, so the hierarchy in §3 is just "call
it at the granularities that matter," not a separate algorithm per level.

---

## 2. Canonicalisation strategy

### 2.1 Node representation

Every `Plan` and `Expr` case class becomes a `CTag(tag, fields)`, where
`tag` is a short, stable, registered string (`"Arithmetic"`, `"Read"`,
...) and `fields` is an ordered list of that node's own fields, each
itself canonicalized. This directly answers the "node representation"
requirement: the canonical form of `amount * 1.20` is

```
CTag("Arithmetic", [
  CLeaf(utf8("*")),
  CTag("ColumnReference", [ CTag("ColumnRef", [ CLeaf(utf8("amount")), CLeaf(utf8("src0")) ]) ]),
  CTag("Literal", [ CLeaf(decimal(120, 2)), CLeaf(utf8("decimal")) ])
])
```

never `node.toString()` (which would depend on `PlanPrinter`'s display
formatting — a separate, presentation-only concern per its own module
doc — and could change for cosmetic reasons without the IR itself
changing).

**Tag table.** A fixed, versioned map from every `Plan`/`Expr` case class
to its tag string (`ColumnReference`, `Literal`, `Alias`, `Cast`,
`Arithmetic`, `Comparison`, `BooleanExpr`, `Conditional`, `Function`,
`UDF`, `AggregateCall`, `UnknownExpression`, `Read`, `Write`, `Project`,
`Filter`, `Join`, `Aggregate`, `Union`, `Sort`, `Limit`, `Window`,
`UnknownPlan`, plus small structural helpers `NamedExpr`, `SortOrder`,
`ColumnRef`, `DatasetRef`). Adding a tag is append-only within a version;
changing what an existing tag means, or its field order, requires a
version bump (§10).

### 2.2 Which values are stable, which are excluded or normalised

This is the central table the "deterministic" requirement asks for.

| Model value | Stable across runs? | Fingerprint treatment |
|---|---|---|
| `ColumnRef.name` | Yes | Hashed as-is |
| `ColumnRef.qualifier` | Yes, but **relabelable** (an alias is a chosen label, not an identity) | Hashed via the scope-substitution table (§2.3), not the raw string |
| `ColumnRef.id` | **No** — populated from Spark's `exprId.id`, a per-session, per-run assigned integer (see `Identifiers.scala`'s own doc: "never exposed as anything but an integer this IR doesn't interpret") | **Always excluded.** This is exactly the "runtime-generated identifier" the determinism requirement warns about, and the IR's own documentation confirms it: it "strengthens" equality for a translator with real per-attribute identity but "never replaces" name/qualifier — fingerprinting relies on name/qualifier alone and never reads `id` |
| `DatasetRef.location` | Yes | Hashed as-is |
| `Read.alias` | Yes, but **relabelable** | Never hashed directly; replaced everywhere by a positional occurrence index (§2.3) |
| `Literal.value` / `literalType` | Yes | Canonical per-runtime-type encoding (§4) |
| `Cast.targetType` | Yes | Hashed as an opaque string (§5) |
| `Arithmetic`/`Comparison`/`BooleanExpr.operator` | Yes | Hashed as-is |
| `Function.name` / `UDF.name` / `AggregateCall.function` | Yes | Hashed as-is (`UDF.name` is `Option[String]`; see §7 for `None`) |
| `UDF.engineType` | Best-effort, translator-classification-dependent, not a stable business-logic fact | Surfaced as metadata, **excluded** from the hash (§7) |
| `UnknownPlan`/`UnknownExpression.description` | Free text — may reword across translator versions for the same unrecognized construct | Surfaced as metadata, **excluded** from the hash (§8) |
| `UnknownPlan`/`UnknownExpression.sourceType` | Yes — a stable class/kind name | Hashed as-is (§8) |
| `Write.format` / `Write.saveMode` | Yes | Included in the **overall** fingerprint only, never in a per-output fingerprint (§3) |
| List order where SQL semantics make it observable (see §2.4) | Yes | Preserved exactly as declared |
| List order where it's set-like (`groupBy`, `partitionBy`) | N/A — no ordering exists in the semantics | Canonically sorted (§2.4) |
| `Set[ColumnRef]` / `Set[AggregationDetail]` from `Lineage.trace` | **No** — Scala's `Set` iteration order is hash-based and not guaranteed stable across JVM/Scala versions | Canonically sorted by each element's own encoded bytes before hashing (§2.4) |
| JVM object identity, memory addresses | N/A | Never touched — canonicalization is a pure structural fold over immutable case class fields, nothing else is observed |

### 2.3 Stable source/output identity (the self-join / alias problem)

Requirement 6 gives a concrete example: `orders.id` and `customers.id`
must never collapse to `id`, `id`. The IR already carries the identity
needed for this — a `ColumnRef`'s qualifier, resolved through `Read`, is
exactly `alias.getOrElse(dataset.location)` (see `Lineage.resolveInScope`'s
`Read` case) — so as long as the qualifier string participates in the
hash, two different `DatasetRef.location`s can never collapse. That part
requires no new mechanism; it falls out of hashing `ColumnRef.qualifier`
and `Read.dataset` at all.

The harder case the design brief also raises is the opposite direction:
**"different aliases with equivalent meaning" should not automatically
create a fingerprint change.** Renaming a self-join's alias from `o1`/`o2`
to `a`/`b`, with every reference to it updated consistently, changes no
observable behaviour — it is exactly alpha-renaming (relabelling a
bound identifier used only for internal disambiguation), the same
well-established, safe equivalence that lets you rename a local variable
in a program without changing what it computes. This is the **one**
identity-level normalisation this design recommends, and it is safe
specifically because it is total and mechanical, not a semantic
judgement about what two different-looking expressions "really mean."

**Mechanism.** Before canonicalizing anything, walk the whole `Plan`
being fingerprinted once, in the same pre-order the IR's own `children`
traversal already defines (the order `PlanPrinter`/`Lineage` use — never
re-sorted), and for every `Read(dataset, alias)` encountered, record its
scope string `s = alias.getOrElse(dataset.location)` in encounter order,
assigning it a positional id `"src0"`, `"src1"`, ... This produces
`scope: Map[String, String]` (raw scope string → positional id), built
**once** for the entire plan being fingerprinted, then threaded into
every subtree canonicalization call — including a later, isolated
recomputation of just one output column's subtree, so the same physical
`Read` always gets the same positional id regardless of which subtree is
being canonicalized in isolation.

Two rules make this safe rather than merely convenient:

- The **positional id replaces only the label** used to refer to a
  `Read` occurrence (in the `Read` node's own canonical form, and in
  every `ColumnRef.qualifier` that matches that scope string elsewhere in
  the tree). It never replaces `DatasetRef.location`, which is always
  hashed in full — two different physical tables can never collapse, no
  matter what they're aliased to.
- The table is built from a **single deterministic traversal that is
  never itself reordered**. Swapping which physical `Read` occupies
  `Join.left` vs. `Join.right` (even for an `Inner` join, where that
  swap is relationally sound) changes encounter order and therefore
  still produces a different canonical tree. This is a deliberate,
  accepted conservatism — see §11's risk about it — not an oversight:
  detecting that an `Inner` join's sides were swapped, and that this
  particular swap happens to be safe for this particular join type, is
  exactly the kind of "clever but unsafe" equivalence this design
  chooses not to attempt.

A qualifier string that matches no `Read`'s scope (e.g. an unqualified
`ColumnRef` in a `Filter` condition that hasn't been resolved against its
producing `Project`) is left untouched — it isn't a relabeling target,
it's an unresolved reference, and resolving it is exactly what the
separate lineage-summary layer in §3 does via `Lineage.trace`, not
something the structural canonicalizer invents its own resolution pass
for.

### 2.4 Field ordering

The governing rule: **preserve declared order wherever SQL/relational
semantics make that order observable; canonically sort only where the
model's own semantics are already order-independent.** Never sort
"to be safe" — an unjustified sort is itself a silent equivalence claim.

| Field | Order meaningful? | Why | Treatment |
|---|---|---|---|
| `Function.args`, `UDF.args`, `AggregateCall.arg` | Yes (assumed generically) | Argument position is meaningful for most functions (`SUBSTRING(str, start, len)`), and this IR has no per-function metadata saying which specific functions are commutative | Preserve |
| `Arithmetic.operands`, `BooleanExpr.operands` | Yes, even where the operator is abstractly commutative | Reordering `+`/`*` risks floating-point rounding differences under a 3+-operand fold (associativity ≠ commutativity for floats); reordering `AND`/`OR` risks masking a translator/engine difference in evaluation order when a non-deterministic or erroring expression sits on one side. See §6 | Preserve — no commutative normalisation by default |
| `Comparison.left`/`right` | Yes | `a > b` ≠ `b > a`; fixed fields, not a list, so there is nothing to reorder in the first place | Preserve |
| `Conditional.branches` | Yes | First matching branch wins; reordering branches changes which value a row gets when more than one condition could hold | Preserve |
| `Join.left`/`right` | Yes | Not symmetric in general (`LeftOuter`/`RightOuter`/`LeftSemi`/`LeftAnti`), and even for `Inner`/`Cross`, swapping sides changes the plan's declared structure (see §2.3) | Preserve, never reordered |
| `Union.inputs` | Yes | The IR's own doc: "Output column names follow the first branch" — branch order is part of the plan's declared meaning | Preserve |
| `Project.columns`, `Aggregate.aggregates`, `Window.windowExprs` (the node's *own* list) | Yes, for the **plan-level** fingerprint (output position is part of the declared schema shape) | A real positional output format (CSV, a fixed-width sink) can make column order observable | Preserve for the overall/plan fingerprint. **But** each named output also gets its own fingerprint (§3) computed purely from its own `NamedExpr.expr`, independent of its siblings' position — so reordering two unrelated output columns changes the overall fingerprint (correctly — the schema's shape changed) without falsely flagging either individual column's own fingerprint as changed |
| `Sort.order`, `Window.orderBy` (`List[SortOrder]`) | Yes | `ORDER BY a, b` ≠ `ORDER BY b, a` — different tie-breaking, different row order | Preserve |
| `Aggregate.groupBy` (`List[Expr]`) | **No** | Plain `GROUP BY` (this IR models no `ROLLUP`/`CUBE`/`GROUPING SETS`) partitions rows into groups; `GROUP BY a, b` and `GROUP BY b, a` produce identical groups. Unlike `Arithmetic`, there is no per-key rounding/evaluation-order compounding across the list — each key is evaluated independently and only used for equality-partitioning | **Canonically sort** — by each key's own encoded canonical bytes, ascending, after encoding (so the sort itself doesn't depend on `Expr`'s Scala `hashCode`/`equals`, only on the same deterministic bytes the hash will use) |
| `Window.partitionBy` (`List[Expr]`) | **No** | Same reasoning as `groupBy` — partitioning keys, not an evaluation order | Canonically sort, same rule |
| `Lineage.ColumnLineage.sources: Set[ColumnRef]` | **No — and not even ordered to begin with** | It's a Scala `Set`; iterating it directly for hashing would make the fingerprint depend on hash-bucket layout, not semantics | Canonically sort by each element's encoded bytes before hashing |
| `Lineage.ColumnLineage.aggregations: Set[AggregationDetail]` | Same as above | Same as above | Same as above |

Sorting "by encoded canonical bytes" (rather than by, say, `Expr`'s
natural Scala ordering, which doesn't exist for most of these types
anyway) means the sort key is exactly the same byte string that will be
hashed — no separate, potentially-inconsistent comparator to keep in
sync with the encoder.

---

## 3. Fingerprint hierarchy

```
TransformationFingerprint
├── version: Int
├── overall: Fingerprint                        // canonicalize(whole Plan, incl. Write)
├── inputs: Map[String, Fingerprint]             // key = "<location>#<occurrenceIndex>"
│                                                 // value = canonicalize(that Read node alone)
└── outputs: Map[String, OutputFingerprint]      // key = output column name
        ├── expression: Fingerprint              // canonicalize(that NamedExpr.expr alone)
        ├── lineage: Fingerprint                 // canonicalize(Lineage.trace's ColumnLineage for this output)
        ├── combined: Fingerprint                // hash(expression ++ lineage) — the practical "did this column change" signal
        └── nonDeterministic: Option[Boolean]    // metadata only, see §9 — never affects any hash above
```

Plus, **on demand, not eagerly precomputed** (to avoid materializing a
fingerprint for every node of a large plan when nothing asks for that
granularity):

- `Canonicalizer.canonicalizeExpr` / `canonicalizePlan` applied to any
  subtree, keyed by a stable structural path (e.g. `outputs.value.expr`,
  `outputs.value.expr.operands[1]`, `plan.filter.condition`) — the
  **expression fingerprints** and **operator/subtree fingerprints** the
  brief's illustrative hierarchy names. These are not a separate
  algorithm; they're the same canonicalizer called on a smaller subtree,
  so they exist "for free" once the canonicalizer and hasher exist. This
  design does not mandate precomputing and storing all of them — that's
  a policy decision for whatever consumes this hierarchy (out of scope
  here), not a property of the canonicalization itself.

**Why two hashes per output, not one.** `expression` and `lineage`
answer genuinely different questions and can disagree usefully:

- `expression` changes for *any* syntactic change to that column's own
  declared computation, including one that a future, more aggressive
  normalisation pass might one day decide is cosmetic (a renamed local
  alias inside the expression, for instance).
- `lineage` is coarser and reuses `Lineage.trace`'s already-tested
  resolution: it changes only when the column's *resolved* source set,
  derivation kind (`Direct`/`Constant`/`Computed`/`Opaque`), or
  aggregation set changes — so it stays stable across a change that
  doesn't affect what the column structurally depends on (e.g. a
  `Filter` elsewhere in the plan gaining an unrelated condition doesn't
  touch `value`'s lineage), while still flipping to `Opaque` the moment
  a `UDF` or `UnknownExpression` enters that column's resolved chain,
  independent of exactly where in the expression tree it sits.
- `combined` is what a change-detection consumer should treat as the
  primary "did column X change" signal — it changes whenever either
  input does, and its own hash is stable/reproducible from the other
  two (`hash(encode(expression) ++ encode(lineage))`), not recomputed
  from raw model data a second time.

`Write.format`/`Write.saveMode`/`Write.dataset` feed only `overall` —
they describe how the result is persisted, not any column's business
logic, and per this design's own principle (report vs. hash — see §7,
§8) that distinction is enforced structurally: an output's `expression`/
`lineage` fingerprints are computed strictly from the subtree rooted at
that `NamedExpr`, never from the enclosing `Write`.

`inputs` is keyed by `"<location>#<occurrenceIndex>"` rather than by the
raw alias, for the same alpha-renaming reason as §2.3 — but
`<location>` itself, the real identifying half, is always present, so
"which physical datasets does this transformation read from, and how
many distinct occurrences of each" is legible directly from the map's
keys without decoding anything.

---

## 4. What is included and excluded

**Included (identity- and logic-bearing):** every `Plan`/`Expr` field
enumerated in §2.2's table as "hashed", plus, for the lineage layer,
`ColumnLineage.sources`/`derivation`/`aggregations`.

**Excluded (never influences any hash):**

- `ColumnRef.id` (§2.2) — the one field the IR itself documents as
  engine-runtime-derived.
- The raw text of `Read.alias` (replaced by positional identity, §2.3).
- `UDF.engineType` (§7) — surfaced, not hashed.
- `UnknownPlan`/`UnknownExpression.description` (§8) — surfaced, not
  hashed.
- Anything not reachable through `Plan`/`Expr`/`Lineage`'s own case
  class fields — there is no separate "extra metadata" channel to
  accidentally leak into a hash, because the canonicalizer only ever
  walks these fields.

This "hash the shape and identity, report but don't hash the prose"
split is applied uniformly (§7, §8 are the same rule applied to two
different node kinds), rather than as two unrelated special cases.

---

## 5. Literal and type normalisation rules

`Literal(value: Any, literalType: String)` is the one place the IR
stores an untyped JVM value, so canonicalization must dispatch on
`value`'s actual runtime type — never on `literalType` (a free-form
string matching the contract module's own type vocabulary, not a closed
enum this module should hardcode assumptions about). `literalType`
itself is always hashed alongside the value (as its own field in the
`Literal` tag), so a translator declaring the same underlying number as
`"integer"` in one version and `"long"` in another is a real, visible
difference — a translation-consistency concern for `spark-adapter`, not
something fingerprinting should paper over by treating the two as equal.

| Runtime type | Canonical encoding | Rationale |
|---|---|---|
| `Int`/`Long`/`java.math.BigInteger`/similar integral types | Exact canonical decimal ASCII digits, explicit `-` only when negative, no leading zeros, no leading `+` | A stable, encoding-width-independent form; avoids "does 1 fit in 4 bytes or 8" ambiguity from a fixed-width binary encoding |
| `scala.math.BigDecimal`/`java.math.BigDecimal` | The exact `(unscaledValue, scale)` pair, each canonicalized as above — **never** a display-normalized value | `1.20` and `1.2` are different declared precisions (`DECIMAL(_,2)` vs `DECIMAL(_,1)`), which can carry real rounding-behaviour meaning; stripping trailing zeros to make them equal is exactly the "canonicalisation based on display formatting" the brief warns against |
| `Double`/`Float` | The IEEE-754 bit pattern (`java.lang.Double.doubleToLongBits`/`floatToIntBits`, the non-raw variant so all NaN payloads canonicalize identically) | Avoids ambiguity between different textual renderings of the same float (`0.1` vs `1.0E-1`); using the non-raw bit conversion is the one deliberate literal normalisation here, justified narrowly: distinguishing NaN *payloads* is not a plausible business-logic signal, while `-0.0` vs `0.0` is preserved bit-exact (not folded together) since IEEE-754 sign matters to some computations |
| `Boolean` | A single canonical byte, `0x00`/`0x01` | Trivial, unambiguous |
| `String` | UTF-8 bytes of the string after Unicode **NFC** normalisation, length-prefixed | NFC is the one endorsed string normalisation: two byte-different Unicode encodings of the identical rendered text (e.g. a precomposed vs. decomposed accented character) are the same string by any reasonable definition, and NFC is a standard, narrowly-scoped transform — not case-folding, not trimming, not locale-aware comparison, none of which this design applies |
| `null` (typed SQL `NULL`, `literalType` still populated) | A canonical `NULL` tag, no value bytes, `literalType` still hashed | Distinct from `UnknownExpression` per the IR's own doc — "fully understood, just empty" — and the fingerprint reflects that: a typed-null literal has a real, stable canonical form, not an opaque one |
| Anything else (`value`'s runtime type isn't one of the above — `Literal.value: Any` is otherwise unconstrained) | A distinct `UNRECOGNIZED_LITERAL_VALUE_TYPE` tag, then `value.toString`'s UTF-8 bytes, length-prefixed | Best-effort and explicitly labeled as such — never silently reusing the `String` encoding, so a future reader of the canonical form can tell "this was genuinely a string literal" from "this was some other runtime value the fingerprinter didn't have a dedicated encoder for." Flagged as a real, open limitation in §11, not a design gap this document pretends to close |

**Dates/timestamps.** The IR has no dedicated date/timestamp node; a
translator represents one as a `Literal` with an appropriate
`literalType` (e.g. `"date"`, `"timestamp"`) and a runtime value that is
presumably one of the integral types above (epoch days/millis/micros).
This falls out of the table above with no special case needed — the
value is hashed by its actual runtime type, and `literalType` is hashed
alongside it — **provided** the translator represents it as an exact
numeric value rather than a display string. If some future translator
path ever produces a formatted date/time string instead, this design
explicitly recommends *against* the fingerprinter re-parsing or
timezone-normalising it: that would require inventing calendar semantics
this module has no business owning, and get it wrong silently. Such a
string would fall through to the plain `String` encoding, verbatim.

### Types

`Cast.targetType` and `Literal.literalType` are both hashed as opaque
strings — no separate type system is introduced for fingerprinting.
Whatever precision the model records is exactly the precision the
fingerprint has: if `spark-adapter`'s translator ever emits
`"decimal(18,2)"` as `targetType` for one cast and `"double"` for
another, those two casts already produce different canonical bytes with
no extra logic required, correctly resolving the brief's own example
(`CAST(amount AS DECIMAL(18,2))` vs `CAST(amount AS DOUBLE)`). If a
translator instead collapses both to a coarser string (e.g. both become
`"decimal"` with no precision), that is a translation-fidelity gap in
`spark-adapter`, not something the fingerprinting stage can or should
compensate for — this design is deliberately "driven by the semantic
lineage model," per the brief's own instruction, not by reverse-engineered
Spark type internals.

---

## 6. Semantic equivalence

**Default: none, beyond the one identity-level normalisation in §2.3.**
No arithmetic or boolean operand is ever reordered on the theory that the
operator is "commutative" — see §2.4's table for the concrete
floating-point-rounding and evaluation-order reasons this is unsafe as a
blanket default, not merely unproven.

`a + b` vs. `b + a`, and `a AND b` vs. `b AND a`, therefore fingerprint
**differently** by default, even though both pairs are truth-table/
arithmetic-identical in the idealized case. This is a deliberate,
named trade-off (§11): a rewrite that only reorders operands will show
up as a "changed" fingerprint. The brief is explicit that this is the
right default ("Correctly detecting a real business-logic change is more
important than performing clever but unsafe equivalence rewriting"), and
in practice a pure operand-swap with no other change is a narrow, easy-
to-recognize false positive for a human reviewing a diff, whereas a
false *negative* (two genuinely different expressions silently declared
equal) is not recoverable after the fact.

If this project later wants operand-order-insensitive comparison for a
specific, provably-safe case, this design's recommendation is: introduce
it as an **explicitly named, separately versioned canonicalization mode**
(not a silent change to the default encoding), scoped as narrowly as
possible (e.g. "sort `AND`/`OR` operands, and only those, never
`Arithmetic`"), and backed by the same property-based and golden-test
rigor §12 describes for the rest of this design — never introduced
incidentally while implementing something else.

**Null behaviour, floating-point behaviour, non-determinism.** These are
exactly why the default stays conservative rather than "normalise
commutative operators, they're obviously safe": SQL's three-valued logic
makes `AND`/`OR` truth-table-symmetric even with `NULL` operands, but an
operand containing a non-deterministic call or a UDF that could error
means "the operator is abstractly commutative" and "swapping these two
subtrees never changes anything observable" are not the same claim — the
latter is false in general, and this design only ever makes the former
claim, never the latter.

---

## 7. UDF strategy and limitations

`UDF(name: Option[String], args: List[Expr], engineType: Option[String])`
is hashed as:

```
CTag("UDF", [ optionField(name), listField(args) ])
```

`engineType` is **not** one of the two fields fed into the hash — it is
attached to the `CTag` as separate, surfaced-but-non-hashed metadata
(mirroring §8's treatment of `UnknownPlan.description`), for a specific
reason: it is documented as "purely diagnostic metadata" and there is no
guarantee it is classified identically for the same real UDF across
translator versions or code paths (e.g. `"ScalaUDF"` vs. a more specific
future classification for what is, underneath, the same registered
function). Mixing a possibly-noisy classification into the hash risks a
false-positive "this column changed" purely from translator churn — the
opposite of what a foundational fingerprint should do. It is still
reported (never silently dropped, per requirement 5's spirit applied
here too), just not hash-affecting.

**What this fingerprint can detect for a UDF:**

- The UDF's `name` changing (`Some("a") → Some("b")`, or `Some(_) ↔
  None`).
- Its declared `args` changing — including a source column being swapped
  for a different one, an extra argument being added, or a nested
  expression inside an argument changing.
- A UDF appearing where a `Function`/built-in used to be, or vice versa
  (different tags entirely).
- Via the lineage layer (§3): the resolved `derivation` for any output
  column whose computation passes through this UDF is `Opaque`, and
  that classification participates directly in the `lineage` hash — so
  "a column now depends on a UDF where it didn't before" is visible even
  if a consumer only looks at the coarse lineage fingerprint, not the
  raw expression tree.

**What this fingerprint cannot detect, and does not claim to:**

- A change to the UDF's *implementation* with the same name and the
  same arguments. The IR carries no version, hash, bytecode reference,
  or any other implementation identity for a UDF — `name`, `args`, and
  `engineType` are the entirety of what the model exposes. If a job
  author edits a registered UDF's body without renaming it or changing
  its call site, **this fingerprint stays identical**, correctly
  reflecting that the model it was built from contains no information
  about the change. This is a limitation of the underlying semantic
  lineage model, not something this design works around — per the
  brief's own instruction, inventing a synthetic "UDF version" out of
  nothing here would be worse than naming the gap.
- Two structurally-identical-but-actually-different anonymous UDFs
  (`name = None` on both). With no name to distinguish them, they
  fingerprint identically if their `args` also happen to match. Also an
  honestly-reported limitation, not a solved problem.

A future, out-of-scope improvement this design does not attempt: if
`ir.UDF` ever gains a real implementation identity field (a hash of the
UDF's bytecode, a registry version, anything Catalyst's own analysis
could in principle expose), the canonicalizer would hash it directly —
but this document does not propose that IR change, since inventing one
is outside its stated scope (canonicalisation and fingerprinting of the
*existing* model).

---

## 8. Unknown-node strategy

`UnknownPlan(description, sourceType, children)` and
`UnknownExpression(description, sourceType, children)` are hashed as:

```
CTag("UnknownPlan", [ CLeaf(utf8(sourceType)), listField(children) ])   // description excluded
CTag("UnknownExpression", [ CLeaf(utf8(sourceType)), listField(children) ])
```

- **`sourceType` is hashed** — it's a stable, structural label for *what
  kind* of construct wasn't understood (e.g. Catalyst's own
  `getClass.getSimpleName`), so a plan that starts hitting a different
  unrecognized construct than before produces a different fingerprint,
  as it should.
- **`description` is excluded from the hash**, for the same reason as
  `UDF.engineType` (§7): it is documented as "a human-readable summary,"
  free text that could be reworded by a translator change without the
  underlying unrepresented construct itself changing. It is still
  carried into the fingerprint hierarchy's metadata (not silently
  dropped) so a human investigating a "this column is opaque due to an
  unknown node" result can read *why*, matching the IR's own existing
  philosophy that an unsupported construct "must always be visible... ,
  never silently dropped" — this design extends that same guarantee
  through to the fingerprint, it doesn't relax it.
- **`children` are canonicalized and included recursively** — the IR's
  own doc: an unrecognized node's "still-resolvable children" carry real
  understood structure and must not be hidden. A change nested inside an
  `UnknownPlan`/`UnknownExpression`'s children is therefore still
  detected even though the node itself is opaque.

**How an unknown node affects the fingerprint hierarchy overall:** it
never disappears at any level. It contributes its own canonical bytes to
the `overall`/subtree hash it's part of (via `sourceType` + children, as
above); and, through `Lineage.trace`, every output column whose resolved
computation passes through it gets `derivation = Opaque` — the same
"opaque wins, even nested arbitrarily deep" rule the IR already applies
for `UDF`, unmodified here, just now also feeding a fingerprint. There is
no code path in this design where an `UnknownPlan`/`UnknownExpression`
contributes zero bytes to any fingerprint it's reachable from — that
would be the "unknowns silently disappear" failure mode requirement 5
forbids.

---

## 9. Non-deterministic expression strategy

The IR has **no first-class non-determinism flag** today — unlike
Catalyst, which carries a real `deterministic: Boolean` on every
`Expression`, `ir.Function`/`ir.UDF` carry only a `name`/`args` (and
`engineType` for `UDF`). This is a real, named gap this design works
within rather than pretends isn't there.

**Core principle, directly from the brief:** fingerprint the
transformation's *definition*, never its *runtime result*. This falls
out of the design with no special handling required: canonicalization
never evaluates an expression, it only walks the static AST — a call
like `Function("rand", Nil)` canonicalizes to the same bytes every time
it appears with the same (here, empty) argument list, regardless of what
`rand()` would actually return if executed. The "fingerprint changes
between runs just because `rand()` produced a different number" failure
mode the brief warns about **cannot occur** in this design, because
nothing in it ever reads a runtime value.

**What's still worth doing:** surfacing *that* a transformation contains
a non-deterministic construct, per the brief's explicit ask. Given the
IR's gap above, this design recommends a small, explicit, versioned
allowlist of known non-deterministic **function names**
(`rand`/`random`/`randn`, `uuid`, `current_timestamp`/`current_date`/
`now`, `unix_timestamp` in its no-argument form, `monotonically_increasing_id`,
`input_file_name`, `spark_partition_id`, and similar), maintained
alongside the fingerprinting code and bumped through the same version
counter as everything else in §10 (growing this list changes what a
fingerprint's *metadata* says, even though — critically — it never
changes the hash bytes themselves, since the underlying `Function` node
is hashed identically either way).

Classification is **tri-state**, not boolean, to avoid the same
"invented certainty" failure mode as everywhere else in this design:

- `Function(name, _)` with `name` in the allowlist → `Some(true)`.
- `Function(name, _)` with a recognized, known-deterministic name (this
  design does not attempt to enumerate every deterministic built-in
  exhaustively) or any other named node kind (`Arithmetic`, `Cast`, ...)
  → `Some(false)`.
- `Function(name, _)` with an unrecognized name, and any `UDF` → `None`
  ("unknown" — an opaque UDF body could do anything, including calling
  a non-deterministic primitive internally, and this IR has no way to
  know either way).

This flag is attached as the `nonDeterministic: Option[Boolean]`
metadata field on each output in §3's hierarchy (computed as "does any
node along this output's resolved computation classify as
`Some(true)`/`None`, combining conservatively — a single `None` anywhere
makes the column's own flag `None`, a single `Some(true)` with no `None`
makes it `Some(true)`, otherwise `Some(false)`"), and — consistent with
§7/§8's pattern — **never influences any hash**. Two definitions that are
byte-identical except for which non-deterministic function name they
call already differ in their `Function.name` field and are correctly
flagged as changed by the ordinary hash; the allowlist only adds the
"and by the way, this one is non-deterministic" annotation on top, it is
never the thing doing the change-detection.

**Named limitation:** this classification is only as good as the
hand-maintained allowlist; it will not catch a non-deterministic
built-in this design's authors didn't think to list, and (as with §7)
would ideally be replaced by reading a real `deterministic` flag off the
model directly. That would require `ir.Function`/`ir.UDF` gaining an
optional field populated from Catalyst's own flag — an IR change, and
therefore out of this document's scope — but the canonicalizer's
tri-state design is written so that, if such a field appears later, it
is a strict improvement in the same slot (metadata only, never hash
input) with no restructuring needed.

---

## 10. Hashing and versioning recommendation

**Algorithm:** SHA-256. Widely available in every runtime without an
extra dependency, collision-resistant well beyond any practical need
here, and fast enough that hashing even a large canonicalized plan is
negligible next to the Spark job it describes. The brief is correct that
the algorithm choice is the least important decision in this design; the
canonical bytes fed into it are what matters, and every section above is
about getting those bytes right.

**Input encoding.** Every `CanonicalNode` is encoded as
`TAG ++ LEN ++ PAYLOAD`:

- `TAG`: the node's registered tag id (a single byte or short integer
  from the version's tag table — see §2.1).
- `LEN`: a varint byte length of `PAYLOAD`.
- `PAYLOAD`: for a `CTag`, the concatenation of each field's own full
  `TAG ++ LEN ++ PAYLOAD` encoding, in the field order §2's tables
  specify; for a `CLeaf`, the literal's own encoded bytes from §5.

This length-prefixed scheme is the reason canonicalization never risks
the classic "concatenation collision" bug (`"a" ++ "bc"` producing the
same bytes as `"ab" ++ "c"`): every field's length is explicit, so no
two different trees can ever encode to the same byte string by
coincidence of where one field's text happened to end and the next
began.

**Fingerprint shape:**

```scala
case class Fingerprint(version: Int, algorithm: String, value: String)
// value: lowercase hex SHA-256 digest, e.g.
// Fingerprint(version = 1, algorithm = "SHA-256", value = "4f2a...c9")
```

**Versioning.** A single monotonically increasing integer,
`fingerprint_version`, bumped on **any** change that could change output
bytes for some input: a new/renamed/reordered tag, a changed field-order
rule, a changed literal or type encoding, a changed alias-substitution
algorithm, a change to the non-deterministic-function allowlist (even
though it never changes hash bytes today, per §9 — bump it anyway,
since the *contents* a consumer should trust to mean "computed under
ruleset N" includes the metadata rules, not only the hash rules), or a
change to the hash algorithm itself. This is deliberately coarse and
conservative — an integer that changes "too often" costs nothing, while
one that fails to change when it should silently corrupts every future
comparison. Comparing two `TransformationFingerprint`s with different
`version`s is a decision for the (out-of-scope) comparison stage, but
this design's responsibility is making sure that stage always has the
information to detect the mismatch rather than assume comparability —
which is why `version` is part of the fingerprint's own shape, not
carried out-of-band.

**Collision considerations.** SHA-256's collision resistance
(~2^128 for a birthday-bound preimage) is not the practical risk in this
design — a bad canonicalization rule producing the same bytes for two
genuinely different plans is. §12's testing strategy is aimed
overwhelmingly at that risk, not at hash-algorithm cryptanalysis.

**Multiple hashes at different levels.** Yes — §3's hierarchy is the
answer: `overall`, one per `inputs` entry, `expression`/`lineage`/
`combined` per output, plus subtree/operator hashes computed on demand.
All share the same `version`/`algorithm`, since they're all produced by
the same canonicalizer and hasher, just applied to different subtrees.

---

## 11. Key risks and design trade-offs

- **Conservatism produces some false positives, by design.** A pure
  `Join` operand swap on a relationally-commutative `Inner` join, or a
  pure `Arithmetic`/`BooleanExpr` operand reorder, changes the
  fingerprint even though the two plans could be argued equivalent. This
  design accepts that trade-off deliberately (§6, §2.3) rather than
  attempt equivalence detection that could produce a false *negative*
  instead — judged the worse failure mode per the brief's own framing.
- **UDF and unknown-node coverage is honest, not complete.** §7 and §8
  are explicit about exactly what changes these nodes can and cannot
  make visible. A consumer of this fingerprint must not be told (and
  this design does not claim) that an unchanged UDF fingerprint proves
  an unchanged UDF implementation.
- **`engineType` and `description` exclusion is a judgement call, not a
  provable safety property.** Unlike `ColumnRef.id` (provably unstable —
  the IR's own doc says so) or NFC string normalisation (a standard,
  narrowly-scoped transform), the decision to exclude `UDF.engineType`
  and `Unknown*.description` from the hash rests on a prediction that
  they are more translator-noise-prone than signal-bearing. If that
  prediction turns out wrong in practice (e.g. `engineType` proves
  perfectly stable and a real project wants it to gate a fingerprint
  change), that's a one-line, version-bumped change to move a field from
  "metadata" to "hashed" — flagged here so it's a deliberate, visible
  decision if it's ever revisited, not a silent one.
- **Fingerprint determinism is guaranteed only relative to a fixed
  `ir.Plan` value.** This design proves "the same `Plan` always
  canonicalizes to the same bytes." It does *not* prove "two separate
  Spark runs of the syntactically identical job always produce
  byte-identical `ir.Plan` values" — that is `spark-adapter`'s
  translation-determinism concern, a real dependency this design relies
  on but does not itself verify. `UDF.engineType`'s exclusion (above) is
  partly a hedge against exactly this dependency being imperfect.
- **The non-deterministic-function allowlist (§9) is inherently
  incomplete** and will need maintenance as new built-ins are
  discovered; it is metadata-only specifically so an incomplete
  allowlist degrades to "a non-deterministic call isn't flagged as such"
  rather than "a non-deterministic call is missed by the hash too" (it
  never was — the hash doesn't depend on the allowlist at all).
- **The `Literal.value: Any` fallback encoding (§5) is best-effort.**
  Because the IR itself leaves `value` unconstrained, a value of an
  unanticipated runtime type falls back to `toString`-based hashing,
  which loses the length-prefix-based collision safety the rest of the
  design has for recognized types (`toString` output for two different
  objects could theoretically coincide). This is flagged as a real,
  narrow gap rather than silently accepted; tightening `Literal.value`'s
  type in the IR itself would close it, but that's an IR change outside
  this document's scope.
- **Positional alias substitution (§2.3) assumes the IR's existing
  guarantee that a self-join requires distinct scope strings.** This
  design does not add new validation for that; it inherits whatever
  guarantee (or lack of one) already exists in `ir`/`spark-adapter` for
  well-formed plans, the same way `Lineage`'s own resolution does.

---

## 12. Detailed testing strategy

All tests target the `fingerprint` module directly, over hand-constructed
`ir.Plan`/`ir.Expr` values — the same style `PlanSpec`/`LineageSpec`
already use in `ir`'s own suite, no Spark session required, since
canonicalization never touches Spark.

**1. Determinism.**
- Canonicalize and hash the same hand-built `Plan` (including at least
  one `Join`, one `UDF`, one `UnknownPlan`) 100+ times in the same JVM
  process; assert every `Fingerprint.value` is identical.
- Canonicalize the same `Plan` value built two different ways that
  should be `==`-equal as Scala case classes (e.g. constructed via two
  different helper functions producing structurally identical trees);
  assert identical fingerprints — guards against any accidental reliance
  on object identity rather than structural equality.
- A property-based test (ScalaCheck, already a project dependency per
  `spark-adapter`'s `SparkPlanAdapterFuzzSpec`): a recursive
  `Gen[Plan]`/`Gen[Expr]` generator with a depth bound, generating across
  every node kind including `UnknownPlan`/`UnknownExpression`/`UDF`;
  property: `canonicalize(p) == canonicalize(p)` for the same generated
  `p`, and hashing twice yields the same `Fingerprint`.

**2. Meaningful changes — one test per bullet in requirement 3,** each
structured as: build plan A, derive plan B by changing exactly one
element, assert the relevant fingerprint differs and (where applicable)
an unrelated fingerprint in the same hierarchy does not:
- `amount * 1.20` → `amount * 1.25`: `outputs("value").expression`
  differs; `outputs("value").lineage` does **not** (same source, same
  `Computed` derivation) — asserting this explicitly demonstrates the
  two-hash split (§3) is pulling real weight, not redundant.
- A source column changes (`ColumnReference(ColumnRef("amount"))` →
  `ColumnReference(ColumnRef("quantity"))` in the same position):
  `expression` and `lineage` both differ (`sources` changed).
  changes.
- `SUM(amount)` → `AVG(amount)`: `AggregateCall.function` differs, both
  `expression` and `lineage`'s `aggregations` differ.
- `status = 'ACTIVE'` → `status = 'INACTIVE'`: the `Literal` differs
  (via §5's string encoding), `expression` differs; if this sits in a
  `Filter.condition`, assert `overall` differs while every `outputs`
  entry *unaffected by that filter's presence* stays stable.
- `INNER` → `LEFT OUTER` join: `Join.joinType` differs, `overall` and
  every output on both sides of the join differ (a join type change is
  observable in row multiplicity/nulls for every downstream column).
- A join condition changes (`o.id = c.id` → `o.id = c.customer_id`):
  `overall` differs; assert this specifically alongside the "aliases
  renamed" test below to show the alias-substitution table doesn't
  accidentally mask a real condition change.
- A function changes (`UPPER(name)` → `LOWER(name)`), function arguments
  change (`SUBSTRING(s, 1, 3)` → `SUBSTRING(s, 1, 5)`), a filter's
  comparison operator flips (`>` → `>=`), a `CASE WHEN` branch's
  condition or result changes, a UDF's name or args change (§7's own
  dedicated tests below), an output expression changes, a cast's target
  type changes (`DECIMAL(18,2)` → `DOUBLE`) — one focused test each,
  same pattern.

**3. Locality.** Build a `Project` with at least three `NamedExpr`
outputs; change only the middle one's expression; assert:
- That output's `expression`/`lineage`/`combined` fingerprints all
  differ from before.
- Every other output's `expression`/`lineage`/`combined` fingerprints
  are **byte-identical** to before (not merely "probably unaffected" —
  assert equality directly, per output, in the same test).
- `overall` differs (since the whole-plan fingerprint always reflects
  any change anywhere).

Repeat the same shape once for a plan with a `Join` (change one side's
projection, assert the other side's outputs are unaffected) and once
for `Aggregate` (change one aggregate, assert `groupBy`-derived and
sibling aggregate outputs are unaffected).

**4. Incidental differences do not change fingerprints.**
- Two `Read`/self-join plans identical except for the literal alias
  strings used (`"o1"`/`"o2"` vs. `"a"`/`"b"`), with every downstream
  `ColumnRef.qualifier` updated consistently: assert identical `overall`
  and identical `inputs`/`outputs` fingerprints.
- Two otherwise-identical `ColumnRef`s differing only in `id`
  (`Some(101L)` vs. `Some(202L)`, or `Some(_)` vs. `None`): assert
  identical fingerprints — the direct regression test for the "must not
  depend on Spark exprId values" requirement.
- Two `UDF` nodes identical in `name`/`args` but differing `engineType`
  (`Some("ScalaUDF")` vs. `Some("PythonUDF")`): assert identical
  fingerprints (hash), but differing reported `engineType` metadata in
  the fingerprint hierarchy's surfaced (non-hash) fields.
- Two `UnknownPlan`s identical in `sourceType`/`children` but differing
  `description` text: assert identical fingerprints, differing surfaced
  `description` metadata.
- A negative-zero vs. positive-zero `Double` literal: assert these
  **do** differ (documented exception in §5 — a positive assertion that
  the "no incidental differences" principle stops exactly where §5 says
  it does, not a blanket "all floats normalize").

**5. Unknown nodes.**
- `UnknownPlan`/`UnknownExpression` at the root, mid-tree, and nested
  inside an otherwise-fully-understood expression (e.g. inside one
  branch of a `Conditional`): assert the node's `sourceType` and
  `children` are reachable in the encoded bytes (a targeted structural
  assertion on the `CanonicalNode` tree, not just "the hash differs from
  some baseline") for every position.
- Two plans differing only in `UnknownPlan.sourceType` (same
  `description`, same `children`): assert fingerprints differ.
- An output whose resolved computation passes through an
  `UnknownExpression` nested two levels deep beneath an otherwise-
  understood `Cast`/`Arithmetic`: assert that output's `lineage`
  fingerprint reflects `derivation = Opaque` (i.e., differs from an
  otherwise-identical plan where that node is fully understood), proving
  "opaque wins even nested arbitrarily deep" survives into the
  fingerprint, not just into `Lineage.trace`'s own return value.

**6. UDFs — the four bullets requirement 3/4 name, individually:**
- UDF identity is represented: two plans differing only in `UDF.name`
  produce different `expression` fingerprints for the affected output.
- Dependencies affect the fingerprint: two plans differing only in one
  element of `UDF.args` (a swapped source column) produce different
  `expression` and `lineage` fingerprints.
- Changes to arguments/dependencies are detected: an added/removed `UDF`
  argument changes the fingerprint (covered by the `listField` encoding
  naturally including arity — assert it explicitly, since a length-
  prefixed list encoding must be shown, not assumed, to distinguish
  different arities).
- The design does not over-claim: a test that builds two `UDF` nodes
  with **identical** `name`/`args`/`engineType` (standing in for "same
  declared call site, implementation silently edited") and asserts their
  fingerprints are **equal** — a test that documents the limitation from
  §7 as an executable, intentional assertion, not an accidental gap a
  future contributor might "fix" without realizing it's a deliberate,
  documented property of this design.

**7. Property-based testing.** Beyond determinism (test 1), two more
properties worth encoding as ScalaCheck properties over the `Gen[Plan]`/
`Gen[Expr]` generators:
- **Injectivity-in-practice / no accidental collisions on structural
  change:** for pairs of generated plans that differ in exactly one
  randomly chosen leaf (a small, targeted mutation generator — flip an
  operator, change a literal, swap a `ColumnRef` name), assert the
  fingerprints differ. This is the property-based generalization of
  test 2's individual hand-written cases, and is valuable precisely
  because it can find a field the encoder forgot to include (a common
  real bug class in hand-rolled encoders: a case class field that exists
  but was never wired into the canonicalizer, so two structurally
  different values silently canonicalize the same way).
- **Ordering invariants from §2.4, generalized:** for generated
  `Aggregate`/`Window` nodes, assert `canonicalize(plan-with-groupBy-in-
  order-X) == canonicalize(plan-with-groupBy-permuted)` for every
  permutation of a generated `groupBy`/`partitionBy` list — and the
  *negation* for a generated `Sort.order`/`Window.orderBy`/`Join`-sides/
  `Arithmetic`-operands permutation (assert these are **not** treated as
  equal in general, i.e. that no field in that second group is
  accidentally being sorted).

**8. Golden tests, with structural/behavioural assertions, not
string-snapshot-only.** For each worked example in §13, freeze the
literal `Fingerprint.value` hex strings as regression fixtures — these
catch an accidental, unintended change to the canonicalization/encoding
rules between commits (the same role a golden `PlanPrinter` snapshot
plays for rendering, per `PlanPrinterSpec`'s own precedent). But every
golden test is paired with the corresponding structural/behavioural
assertion from tests 1-7 in the same test (e.g. "the value fingerprint
equals this frozen hex string, **and** differs from the 1.25-literal
variant's frozen hex string, **and** the customer_id fingerprint equals
its own frozen hex string in both variants") — a golden string alone
proves nothing about *why* it changed or didn't; pairing it with the
explicit comparison is what this brief's "not exclusively string
snapshots" instruction is asking for.

---

## 13. Worked examples

### Example A — the brief's own literal-change example

**Input (`ir` values, both variants):**

```scala
val orders = Read(DatasetRef("raw.orders"))
def plan(rate: BigDecimal) = Write(
  DatasetRef("gold.customer_values"),
  Project(orders, List(
    NamedExpr("customer_id", ColumnReference(ColumnRef("customer_id", Some("raw.orders")))),
    NamedExpr("value", Arithmetic("*", List(
      ColumnReference(ColumnRef("amount", Some("raw.orders"))),
      Literal(rate, "decimal")
    )))
  ))
)
val before = plan(BigDecimal("1.20"))
val after  = plan(BigDecimal("1.25"))
```

**Canonical representation (human-readable view; §10 shows the actual
length-prefixed byte encoding this maps to) — `before`'s `value` output:**

```
Arithmetic(*,
  ColumnReference(ColumnRef(amount, src0)),
  Literal(decimal:120/scale2, decimal))
```

(`raw.orders` is the sole `Read`, so it gets scope id `src0`; the decimal
is shown as `unscaledValue/scale` per §5, not as display text.)

`after` differs only in the `Literal` leaf: `decimal:125/scale2`.

**Resulting fingerprint hierarchy (illustrative hex, not real digests):**

| | `before` | `after` |
|---|---|---|
| `overall` | `f1a2...` | `9c3d...` (differs) |
| `inputs("raw.orders#0")` | `77bb...` | `77bb...` (**identical** — the source table itself didn't change) |
| `outputs("customer_id").expression` | `55ee...` | `55ee...` (**identical**) |
| `outputs("customer_id").lineage` | `de01...` | `de01...` (**identical**) |
| `outputs("value").expression` | `a001...` | `b902...` (differs) |
| `outputs("value").lineage` | `c777...` | `c777...` (**identical** — same source `{raw.orders.amount}`, same `Computed` derivation) |
| `outputs("value").combined` | `2f4e...` | `9a11...` (differs) |

This is exactly the report shape the brief asks for: `value`'s
`combined` fingerprint flags `CHANGED`, `customer_id`'s stays
`UNCHANGED`, and `value.lineage` staying identical alongside
`value.expression` changing is itself informative — the column's
*shape of dependency* (which columns, what kind of derivation) didn't
change, only the specific arithmetic performed on it did.

### Example B — self-join alias invariance and stable source identity

**Input, variant 1** (a self-join comparing each order against the
customer's most recent prior order, aliased `o1`/`o2`):

```scala
val o1 = Read(DatasetRef("raw.orders"), Some("o1"))
val o2 = Read(DatasetRef("raw.orders"), Some("o2"))
val cust = Read(DatasetRef("raw.customers"))
val joined = Join(
  Join(o1, o2, JoinType.Inner, Some(Comparison("=",
    ColumnReference(ColumnRef("customer_id", Some("o1"))),
    ColumnReference(ColumnRef("customer_id", Some("o2")))))),
  cust, JoinType.Inner, Some(Comparison("=",
    ColumnReference(ColumnRef("customer_id", Some("o1"))),
    ColumnReference(ColumnRef("id", Some("raw.customers"))))))
```

**Variant 2**: identical structure, `o1`/`o2` renamed to `a`/`b`
everywhere (including inside both `Comparison` conditions).

**Scope table (§2.3), variant 1:** pre-order traversal encounters
`Read("raw.orders", Some("o1"))` first → `src0`; `Read("raw.orders",
Some("o2"))` second → `src1`; `Read("raw.customers")` third → `src2`.
Variant 2 encounters the same three `Read`s in the same order (renaming
an alias doesn't change traversal order), producing the identical table
shape, just keyed by different raw strings (`"a""→"src0"`,
`"b"→"src1"`) that map to the same positional ids.

**Canonical form of the inner join's condition, both variants (identical):**

```
Comparison(=,
  ColumnReference(ColumnRef(customer_id, src0)),
  ColumnReference(ColumnRef(customer_id, src1)))
```

**Result:** `overall` (and every input/output fingerprint) is
**identical** between variant 1 and variant 2 — the "different aliases
with equivalent meaning" case the brief names explicitly. Meanwhile,
`inputs` is keyed `{"raw.orders#0": ..., "raw.orders#1": ..., "raw.customers#0": ...}`
in both variants — `raw.orders.id`-style collapsing across the two
self-join occurrences never happens, because each retains its own
positional key and its own resolved `ColumnRef.qualifier` (`src0` vs.
`src1`) throughout every downstream expression, exactly satisfying
requirement 6's `orders.id`/`customers.id` example (here, the
self-join variant of the same hazard).

A **variant 3**, differing only in swapping which physical `Read` is
`o1` vs. `o2` (i.e., literally swapping the two `Read(DatasetRef(
"raw.orders"), ...)` values at the tree positions currently held by
`o1`/`o2`, without touching the condition's `Comparison`), changes
traversal-encounter order and therefore **does** produce a different
`overall` fingerprint from variants 1/2 — the accepted conservatism from
§2.3/§11: this happens to be a relationally-meaningless swap for this
particular self-join, but this design does not attempt to prove that in
general, and flags it as a fingerprint difference rather than silently
declaring it equivalent.

### Example C — UDF and unknown-node conservatism

**Input:**

```scala
val plan = Write(DatasetRef("gold.scored_events"),
  Project(Read(DatasetRef("raw.events")), List(
    NamedExpr("event_id", ColumnReference(ColumnRef("event_id", Some("raw.events")))),
    NamedExpr("risk_score", UDF(
      Some("score_risk"),
      List(ColumnReference(ColumnRef("amount", Some("raw.events"))),
           ColumnReference(ColumnRef("country", Some("raw.events")))),
      Some("PythonUDF"))))))
```

**Canonical form of `risk_score`'s expression:**

```
UDF(Some(score_risk), [
  ColumnReference(ColumnRef(amount, src0)),
  ColumnReference(ColumnRef(country, src0))
])
```

(`engineType = Some("PythonUDF")` is recorded as surfaced metadata on
this node, per §7 — not part of the bytes above.)

**Lineage layer:** `Lineage.trace` reports
`ColumnLineage(risk_score, Set(raw.events.amount, raw.events.country), Opaque, Set())`
(after applying the same `src0` substitution to the reported sources for
consistency with the expression layer) — canonicalized as a sorted-by-
bytes `Set` per §2.4, tagged with `Opaque`.

**Three follow-up variants, each demonstrating a specific claim from §7:**

- **Rename the UDF** (`Some("score_risk")` → `Some("score_risk_v2")`,
  same args): `outputs("risk_score").expression` differs (name is
  hashed); `.lineage` is **unchanged** (`sources`/`derivation` didn't
  move — the UDF is still opaque, over the same two columns). Shows the
  two layers disagreeing usefully: something about this column changed,
  but not its dependency shape.
- **Swap an argument** (`country` → `country_code`, a different column):
  both `.expression` and `.lineage` differ (`sources` set changed).
- **Change only `engineType`** (`Some("PythonUDF")` → `Some("ScalaUDF")`,
  name/args identical): **both** `.expression` and `.lineage`
  fingerprints are unchanged — the documented, deliberate exclusion from
  §7, verified here as a worked case rather than only asserted in prose.
- **Edit the UDF's actual implementation with no change to the IR at
  all** (not representable as an IR diff, since the IR carries none of
  that information): by construction, **every** fingerprint in the
  hierarchy is identical to the original. This is the concrete
  illustration of §7's named limitation: this design correctly reports
  "no detectable change" here, because none of the information the
  brief allows this stage to use (name, args, dependencies) changed —
  and it does not claim otherwise.

If `score_risk`'s call had instead not been resolvable at all (e.g. a
translator encountering a construct with no IR equivalent), the same
plan with `UDF(...)` replaced by
`UnknownExpression("unrecognized Python UDF wrapper", "PythonUDFWrapper", List(ColumnReference(...), ColumnReference(...)))`
would canonicalize its `sourceType` and `children` exactly as in Example
C's `UDF` case (the two node kinds are structurally parallel by design,
per §8), with `description` similarly excluded from the hash — the same
worked numbers apply with `"UnknownExpression"` in place of `"UDF"` as
the tag.

---

## Non-goals (explicit)

Restated from the top of this document, because they bound every
decision above: no persistence format for a stored fingerprint, no
publication or transport mechanism, no remote comparison service, no
CI/CD integration, no Spark-plan-extraction changes, and no Virtual Data
Environment functionality. Those are all real future work this design
deliberately sets up for (versioned fingerprints, a hierarchy with
locality, honest metadata alongside hashes) without attempting to solve
here.
