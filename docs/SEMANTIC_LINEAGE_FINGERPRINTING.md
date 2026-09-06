# Semantic Lineage Fingerprinting — Design

**Status:** Implemented. The `fingerprint/` sbt module (depending only on
`ir`, per §1) implements the canonicalisation/encoding/hashing scheme this
document specifies, and `spark-adapter` surfaces it per §14. See
"Implementation notes" at the end of this document for exactly what
shipped, its real measured mutation-testing score, and what's still
outstanding (Maven Central publishing, MiMa, and CI wiring for
`fingerprint` itself — tracked in ROADMAP.md). The design below is
otherwise unchanged from the original proposal; treat it as the accurate
description of what the code does, not a superseded plan.

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

Persistence, remote comparison services, CI/CD workflows, Spark plan
extraction, and Virtual Data Environment functionality are all explicitly
out of scope. Where a decision here has a consequence for one of those
later stages (e.g. "the fingerprint must carry its own version number so
a future comparator can refuse to compare across versions"), that
consequence is noted, but the comparator itself is not designed.

**One narrow exception, added after initial review:** *emitting* a
computed fingerprint through the Spark contract extension's two existing
output channels — the human-readable validation message
(`ContractEnforcementRule.explain`) and the already-existing
`NotificationSink` publishing mechanism — is in scope, and specified in
§14. This is not the "publication" this document otherwise excludes: it
adds no new transport, no new sink, no storage of a fingerprint's
history, and no comparison logic. It reuses two channels that already
exist today for unrelated reasons (the violation-explanation text and
`ContractValidationEvent`), and simply carries the already-computed
fingerprint value through them, the same way those channels already
carry other structured data (`Violation`s). Persisting a fingerprint
somewhere queryable, and comparing it against a *prior* fingerprint, both
remain out of scope, since they require design decisions (storage format,
retention, what "prior" means across branches/environments) this
document does not make.

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
├── overall: Fingerprint                        // canonicalize(whole Plan, incl. Write) —
│                                                 // or, when rowMutation is Some, hash(Transformation(that plan node, the row mutation node))
├── inputs: Map[String, Fingerprint]             // key = "<location>#<occurrenceIndex>"
│                                                 // value = canonicalize(that Read node alone)
├── outputs: Map[String, OutputFingerprint]      // key = output column name
│       ├── expression: Fingerprint              // canonicalize(that NamedExpr.expr alone)
│       ├── lineage: Fingerprint                 // canonicalize(Lineage.trace's ColumnLineage for this output)
│       ├── combined: Fingerprint                // hash(expression ++ lineage) — the practical "did this column change" signal
│       └── nonDeterministic: Option[Boolean]    // metadata only, see §9 — never affects any hash above
└── rowMutation: Option[Fingerprint]             // canonicalize(ir.RowMutation), only for MERGE/UPDATE/DELETE — see "Row mutation facts" below
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

**Row mutation facts (`rowMutation`).** `ir.Plan` alone cannot represent
a MERGE's `ON` condition or a conditional DELETE/UPDATE's predicate:
`spark-adapter`'s `WriteCommandSupport` translates a MERGE's `query` as
its `source` side only, and an UPDATE/DELETE's `query` as a bare
reference to the `target` — the condition/predicate itself has no `ir.Plan`
representation at all (this is also why `RuleVerifier`'s DML rules, e.g.
`merge_condition`/`forbid_unconditional_delete`, are checked against a
*separately* extracted `ir.RowMutation`, not against `translated.plan`;
see docs/SPARK_ADAPTER.md). Fingerprinting `translated.plan` alone would
therefore be blind to exactly the part of a MERGE/UPDATE/DELETE that
usually matters most — *which rows* get touched, not just what the
touched columns are computed from.

`Canonicalizer.canonicalizeRowMutation(mutation: ir.RowMutation, scope)`
closes this gap by canonicalizing the same `RowMutation` value
`RuleVerifier` already checks — `matchCondition` (via the ordinary
`Expr` canonicalizer), `delete` (a 3-way tag over `DeleteScope.
NotApplicable`/`Unconditional`/`Conditional(condition)`), and
`updatedColumns` (set-like, canonically sorted like `Aggregate.groupBy` —
an UPDATE's assigned-column list carries no meaningful order).
`TransformationFingerprinter.fingerprint` takes this as a second, optional
parameter (`fingerprint(plan: Plan, rowMutation: Option[RowMutation] =
None)`); when absent (the default, and every ordinary INSERT/overwrite
`Write`), `overall`/`rowMutation` are byte-identical to a version of this
module with no `RowMutation` support at all. When present, `overall`
becomes `hash(Transformation(planNode, rowMutationNode))` — wrapping,
not replacing, the plan's own canonical form — so a MERGE's `ON`
condition changing moves `overall` (and the standalone `rowMutation`
fingerprint) even though `translated.plan`'s own shape (`source`, in
`ir.Write.input`) is byte-identical. `inputs`/`outputs` are computed from
`plan` exactly as before and never see `rowMutation` — the per-column
layer answers "what does this column compute," which a MERGE's `ON`
condition or a DELETE's predicate never changes.

---

## 4. What is included and excluded

**Included (identity- and logic-bearing):** every `Plan`/`Expr` field
enumerated in §2.2's table as "hashed", plus, for the lineage layer,
`ColumnLineage.sources`/`derivation`/`aggregations`, plus — when the
transformation is a MERGE/UPDATE/DELETE and a `RowMutation` is supplied —
`RowMutation.matchCondition`/`delete`/`updatedColumns` (§3's "Row
mutation facts").

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
| `Array[Byte]` (a `BinaryType` literal — confirmed directly against a real Spark session: `Literal.value.getClass.getName == "[B"`) | The raw byte content itself, length-prefixed — never `value.toString` | `Array`'s own `toString`/`equals`/`hashCode` are JVM-identity-based (`[B@1a2b3c4d`), not content-based, unlike every other literal runtime type this design falls through to the generic fallback for (e.g. Catalyst's `GenericArrayData` for an `ArrayType` literal has a stable, content-based `toString`, confirmed directly too) — the one literal runtime type that would silently break "same model → same fingerprint" (a different hash on every JVM run, for the exact same literal) if it were ever hashed via the generic fallback below instead of its own dedicated, content-based case |
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
- `Function(name, _)` with `name` **not** in the allowlist, and any other
  named node kind (`Arithmetic`, `Cast`, ...) → `Some(false)`. This does
  **not** claim to have verified every non-allowlisted name is truly
  deterministic — it leans on `Function`'s own documented meaning in `ir`
  ("a claim this IR understands what the named operation computes," i.e.
  an ordinary per-row computation): absence from this allowlist is read
  as "not flagged as non-deterministic," the practical, useful default,
  rather than `None` for every unrecognized name — which would make this
  classification collapse to "unknown" for nearly every real
  transformation and defeat its purpose. Named explicitly as a judgement
  call in §11, not a proven safety property.
- Any `UDF`, regardless of its arguments' own classification → `None`
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
  guarantee that a self-join requires distinct scope strings — a real gap
  was found (and fixed) where that assumption used to be false.** This
  design itself adds no new validation for that; it inherits whatever
  guarantee already exists in `ir`/`spark-adapter` for well-formed plans,
  the same way `Lineage`'s own resolution does. That guarantee used to
  fail for one concrete, reproduced case: an unaliased DataFrame-API
  self-join of the same catalog table (`spark.table("t")` on both sides,
  no `.as()` anywhere) — Spark's analyzer wraps both physical `Read`
  occurrences in a `SubqueryAlias` using the table's own name, the
  identical string on both sides, and `SparkPlanAdapter` used to carry
  that collision straight into both `ir.Read.alias` fields, with nothing
  in `ColumnRef.qualifier` left to tell the two occurrences apart (Spark's
  own disambiguation lives entirely in per-session `exprId` values, which
  this design deliberately never hashes — see `ColumnRef`'s own doc);
  `buildScopeInfo` then collapsed both to the same positional id,
  indistinguishable from a legitimate consistent-alias-rename. Confirmed
  concretely reachable via `.toDF(colNames*)` (a positional rename over
  the join's own already-`exprId`-distinct output attributes — Spark's
  `Dataset.join` deduplicates the right side's `exprId`s internally
  specifically to make self-joins usable at all) — no special Spark config
  needed, an ordinary, everyday pattern. (A tempting *broader*-looking
  repro — referencing a specific side's column via a `left(...)`/
  `right(...)` Dataset-column handle in a `.select()` after the join — is
  *not* actually reachable: Spark's own `DetectAmbiguousSelfJoin` rule
  blocks it by default, and confirmed empirically, even with that guard
  turned off Spark's column-object resolution collapses `left("x")` and
  `right("x")` to the identical `exprId` anyway, since those handles were
  captured before the join-triggered deduplication — there is no
  genuinely different query being conflated there, so this is not a
  reachable false negative and isn't what the fix targets.)
  `canonicalizeLineage` (§3's `lineage` layer) resolves
  `ColumnLineage.sources` through the exact same `scope` substitution
  table via `canonicalizeColumnRef`, so this was not an `expression`-layer-
  only gap — the `lineage` and `combined` hierarchy levels for an affected
  output collapsed identically too, for the same reason. **Fixed** by
  `SparkPlanAdapter.computeAliasDisambiguation`: one pass over the whole
  plan (via Catalyst's own `TreeNode.collect`) that groups every
  `SubqueryAlias` occurrence by its default name and, for any name shared
  by more than one occurrence, assigns each a distinct, deterministic
  `"<name>#<index>"` suffix (in encounter order — the same convention
  `buildScopeInfo`'s own `ReadOccurrence` already uses), keyed by each
  occurrence's output attributes' `exprId`s so both `ir.Read.alias` and
  every `ColumnRef.qualifier` referencing it agree. A name with only one
  occurrence (an ordinary read, or an explicitly-aliased self-join with
  two different names) is left completely unchanged, so this is a
  no-op — byte-for-byte identical translation output — for every
  already-correct case. Verified against a real Spark session
  (`SparkPlanAdapterSpec`'s/`ContractEnforcementRuleSpec`'s self-join
  translation tests); scoped Stryker mutation testing on the touched
  method per CLAUDE.md's Mutation Testing Requirement is tracked
  separately below rather than asserted here with an unconfirmed number.

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

## 14. Integration: surfacing fingerprints in the Spark contract extension

This section specifies how a computed `TransformationFingerprint` (§3)
reaches the two places a user of the verification engine already looks —
the exception text a rejected write raises, and whatever
`NotificationSink` a deployment has configured — grounded in
`spark-adapter`'s real `ContractEnforcementRule.scala`,
`StructuralVerifier.scala`, and `notification/NotificationEvent.scala` as
they exist today, the same way every other section of this document is
grounded in `ir`.

### 14.1 Enablement — one new opt-in flag, no new toggle for publishing

`VerificationOptions` already carries two opt-in toggles
(`rejectUndeclaredInputs`, `rejectUndeclaredFields`), documented as "off
by default, matching how most contract/schema tooling treats an unlisted
extra column: permitted unless a caller opts into strict mode." This
design adds a third, following the same convention:

```scala
case class VerificationOptions(
  rejectUndeclaredInputs: Boolean = false,
  rejectUndeclaredFields: Boolean = false,
  computeFingerprint: Boolean = false
)
```

Off by default, for the same reason the existing two are: canonicalizing
and hashing a whole plan on every check is real, non-trivial additional
work this design should not impose on every existing caller the moment
it ships. A caller opts in explicitly by constructing
`VerificationOptions(computeFingerprint = true)`.

There is deliberately **no separate "publishing enabled" flag.**
"Publish if enabled" (the user-facing ask this section answers) is
already exactly what `ContractEnforcementRule.publishValidation`
does today — `sink.foreach { s => s.publish(...) }`, a no-op unless a
`NotificationSink` was supplied to `forContract`. Once a fingerprint is
computed at all (`computeFingerprint = true`), it rides along
unconditionally on whichever of the two existing channels below are
already active; there is nothing to separately "enable" for publishing
that isn't already governed by "is a sink configured."

### 14.2 Where the fingerprint is computed, and where it deliberately isn't

`verifyOrThrow`'s `ir.Write` branch already produces `translated.plan`
(via `SparkPlanAdapter.translate(plan)`) before calling
`StructuralVerifier.verify`. This is the one place a real, complete
`ir.Plan` for the write being checked already exists — fingerprinting
reuses it directly, computing `TransformationFingerprinter.fingerprint
(translated.plan, mutation)` immediately alongside `StructuralVerifier.
verify`'s own call, only when `options.computeFingerprint` is true. No
second Spark-plan translation, and no fingerprinting of anything
`SparkPlanAdapter` hasn't already turned into IR.

`mutation` here is not a second, independent extraction: this same branch
already calls `RowMutationSupport.classify(plan)` once, to feed
`RuleVerifier.verify`'s DML rule checks (`merge_condition`,
`forbid_unconditional_delete`, ...) — the classification result is
computed exactly once and reused for both purposes, so `ruleViolations`
and the fingerprint can never see a different view of the same MERGE/
UPDATE/DELETE. Only `RowMutationSupport.Classification.Extracted`'s own
`RowMutation` value is ever passed through; `Unverifiable` (recognized as
DML of some kind, but this module couldn't extract everything a rule of
that kind needs) and `None` (not row-level DML at all) both mean "no
`RowMutation` to fold in" for fingerprinting purposes — the fingerprint in
that case is exactly what it would have been before `RowMutation` support
existed, not a silently-degraded one.

The state-changing-CALL branch and the invalid-contract branch
(`requireValidContract`) do **not** get a computed fingerprint, even with
the flag on. Both already fall back to a synthetic `describedPlan =
UnknownPlan(...)` stand-in purely so `explain()` has something to render
(see `verifyOrThrow`'s and `requireValidContract`'s own code) — it is not
a real transformation plan, just a placeholder description. Fingerprinting
it would canonicalize to "one `UnknownPlan` node holding a human-authored
message string, with `description` itself excluded from the hash per
§8" — a fingerprint that is technically well-defined but carries no real
information, since there is no `ir.Plan` behind it to begin with. Rather
than manufacture a misleadingly-present-looking `Some(fingerprint)` for a
case with nothing genuine to fingerprint, `VerificationResult.fingerprints`
(§14.3) stays `None` here, honestly reflecting "not applicable to this
kind of check" — the same "unknowns must stay visible, never invented"
discipline the rest of this document applies to the model itself, applied
here to what counts as a fingerprintable transformation at all.

### 14.3 Carrying the fingerprint on `VerificationResult`

`VerificationResult(status, contract, violations)` gains one new,
appended, defaulted field:

```scala
case class VerificationResult(
  status: String,
  contract: String,
  violations: List[Violation],
  fingerprints: Option[TransformationFingerprint] = None
)
```

populated exactly once, in `verifyOrThrow`'s `ir.Write` branch, from the
computation in §14.2, and left `None` everywhere else (`computeFingerprint
= false`, or either of the branches in §14.2 that have no real plan).
Both `explain()` (§14.4) and `publishValidation` (§14.5) read this one
field rather than each recomputing or independently deciding whether to
fingerprint — a single source of truth for "was this fingerprinted, and
with what," so the printed message and the published event can never
disagree about the value.

This is the same reasoning `ContractViolationException`'s own doc already
states about `result` generally: a caller inspecting `result` directly
(not just reading `getMessage`) gets full, structured access to
`fingerprints`, the same way it already gets full, structured access to
`violations` rather than only their rendered text.

### 14.4 The human-readable validation message

`explain()` builds `ContractViolationException`'s message deterministically
from `result.violations` and the plan's own rendering (see its own doc:
"the same violation always produces the same message, byte for byte").
A fingerprint section is appended on the same deterministic basis,
**only when `result.fingerprints` is `Some`**, after the existing
"How to correct it" section:

```
Fingerprints (v1, SHA-256):
  overall: 9c3d1a...
  outputs:
    customer_id: 55ee0f... (unchanged shape n/a — no prior fingerprint available here)
    value:       9a11b2...
```

Two things are deliberately restrained here, both to keep the exception
text readable rather than dumping the entire hierarchy:

- Only each output's **`combined`** hash (§3) is printed, not its
  separate `expression`/`lineage` components — those remain available on
  `result.fingerprints` for any caller that wants them, unprinted here.
- `explain()` never attempts to say "changed" or "unchanged" for any
  fingerprint — this document's own architecture diagram places
  "compare with previous fingerprint" as a later, out-of-scope stage
  (§ "Scope"/"Non-goals"). `explain()` only ever prints the current
  check's own fingerprint values, exactly as it only ever prints the
  current check's own violations — it has no access to, and this
  section does not give it, any prior fingerprint to diff against.

This section is appended only to `explain()`'s output, i.e. it appears in
`ContractViolationException.getMessage` for a **failed** check. A
passing check never raises an exception at all today, so there is no
analogous "print" for a PASS — `result.fingerprints` remains available
to a caller inspecting a passing `VerificationResult` directly (e.g. from
`verifyOrThrow`'s callers, or a future harness surfacing it in a report),
the same as any other field on that already-returned value; this document
does not add a new printed side channel for the PASS case, since none
exists today for anything else `VerificationResult` carries.

### 14.5 Publishing through the existing `NotificationSink` mechanism

`ContractValidationEvent` gains one new, appended, defaulted field,
following the exact precedent `applicationId` already set on this same
case class:

```scala
case class ContractValidationEvent(
  contract: String,
  status: String,
  violations: List[Violation],
  timestamp: Long,
  metadata: Map[String, Any],
  applicationId: Option[String] = None,
  fingerprints: Option[TransformationFingerprint] = None
) extends NotificationEvent { val eventType: String = "CONTRACT_VALIDATION" }
```

`publishValidation` passes `result.fingerprints` straight through
unchanged:

```scala
ContractValidationEvent(
  contract = result.contract,
  status = result.status,
  violations = result.violations,
  timestamp = System.currentTimeMillis(),
  metadata = contract.extensions,
  applicationId = applicationId,
  fingerprints = result.fingerprints
)
```

exactly the "publish if enabled" the user-facing ask names: `sink` being
configured is the existing, only gate (§14.1), and — following
`publishValidation`'s existing behaviour — this reaches a subscriber for
**every** check, PASS or FAIL, not only failures, since a PASS's
fingerprint (the shape of an unchanged, passing transformation) is
exactly the baseline a future comparison stage would need, and
`publishValidation` already publishes both today for that reason.

**`NotificationJson`.** `TransformationFingerprint`/`Fingerprint`/
`OutputFingerprint` (§3, §10) each gain a `toMap: Map[String, Any]`
method, following the exact precedent `Violation.toMap` already
establishes for crossing this same JSON boundary. `NotificationJson.fields`'s
`ContractValidationEvent` case gains exactly one new line —
`"fingerprints" -> e.fingerprints.map(_.toMap)` — since `anyToJson`
already recurses through `Map`/`Iterable`/`Option`/`String`/`Number`
generically (see its own doc) and needs no changes at all to render
whatever shape `toMap` produces. `invaract-notification-kafka`'s
`KafkaNotificationSink`, and any other custom sink, gets this for free
the same way it already gets `violations`, `applicationId`, and every
other field for free — no per-sink change required.

### 14.6 Binary compatibility

Both changes in this section — a new field on `VerificationOptions`, and
a new field on `ContractValidationEvent` — are real, deliberate MiMa
breaks under CLAUDE.md's "API Compatibility Requirement," in exactly the
same way `ContractValidationEvent.applicationId` and every one of
`WriteEvent`'s later-appended `Option[...] = None` fields already were:
appending a defaulted field to an existing case class changes its
constructor's arity. This document's recommendation, consistent with
that established precedent, is not to contort the API to avoid the break
(e.g. a parallel overload class), but to take it as a normal, expected,
documented break when implemented — the exact `mimaBinaryIssueFilters`
entries `sbt mimaReportBinaryIssues` reports against a real
`publishLocal` of the pre-change module, added with a comment pointing
back to this section, per CLAUDE.md's "the break is deliberate" path.

### 14.7 Testing consequences

Two additions to §12's testing strategy follow directly from this
section, both belonging to `spark-adapter`'s own test suite (not the new
`fingerprint` module's) since they test integration, not
canonicalization:

- `computeFingerprint = false` (the default): `verifyOrThrow` produces a
  `VerificationResult` with `fingerprints = None`, and `explain()`'s
  output contains no `Fingerprints` section at all — a direct regression
  test that this feature is genuinely opt-in, not merely defaulted-quiet.
- `computeFingerprint = true`: a `ContractValidationEvent` published to a
  test double `NotificationSink` (the existing pattern
  `ContractEnforcementRuleSpec`-style tests already use for the sink
  overload) carries a non-`None` `fingerprints` field whose `overall`
  value matches `TransformationFingerprinter.fingerprint` computed
  directly and independently over the same hand-built `ir.Plan` — proving
  `ContractEnforcementRule` doesn't merely attach *some* fingerprint, but
  the correct one for the plan it just checked.

## Non-goals (explicit)

Restated from the top of this document, because they bound every
decision above: no persistence format for a stored fingerprint, no new
transport or sink mechanism, no remote comparison service, no CI/CD
integration, no Spark-plan-extraction changes, and no Virtual Data
Environment functionality. §14's addition does not change this — it
carries an already-computed fingerprint through two channels the Spark
contract extension already has for unrelated reasons (a thrown
exception's message text, and whatever `NotificationSink` a deployment
already configured); it does not add a place fingerprints are stored,
queried, or compared against history. Those remain real future work this
design deliberately sets up for (versioned fingerprints, a hierarchy with
locality, honest metadata alongside hashes) without attempting to solve
here.

---

## Implementation notes

What actually shipped, kept separate from the design sections above so
those stay an accurate *specification* rather than a build log.

**Module.** `fingerprint/` — pure Scala, one real dependency
(`com.invaract:invaract-ir`), no Spark. `CanonicalNode.scala` (the
`CTag`/`CLeaf` tree and the length-prefixed `Encoding`, including a
test-only `decode` used for round-trip testing — §10's own note that
decodability isn't required, only unambiguity, still holds; `decode`
exists to *prove* that property, not because production code needs it),
`LiteralEncoding.scala` (§5), `NonDeterminism.scala` (§9),
`Canonicalizer.scala` (§2/§3's node canonicalisation, the alias
scope-substitution table, and the deep passthrough-resolution described
below), `Fingerprint.scala` (§10), `TransformationFingerprint.scala`
(§3's hierarchy + `TransformationFingerprinter`).

**One implementation detail this document underspecified.** §3 says an
output's `expression` fingerprint is "`canonicalize(that NamedExpr.expr
alone)`" — read literally, that's the *outermost* declared expression at
whatever `Project`/`Aggregate`/`Window` boundary a `Write` sits on
directly. For a realistic plan built from a chain of `.withColumn()`
calls, Invaract translates Spark's *analyzed* (not optimized) plan, which
nests a nested `Project` per call — so the outermost declaration for an
untouched column is frequently just a bare passthrough reference to an
inner `Project`'s real computation (`ir.Lineage`'s own "Derivation
classification" section describes the identical shape). Canonicalizing
only the outermost syntax would have missed exactly the `amount * 1.20 →
1.25` change this document's own §13 Example A depends on detecting.
`Canonicalizer.resolveExprDeep`/`resolveRefDeep` (and the
`resolvedOutputs` entry point `TransformationFingerprinter` calls)
resolve straight through any number of passthrough hops, structurally
mirroring `ir.Lineage`'s own resolution walk but reconstructing an actual
(passthrough-inlined) `Expr` rather than a summary — a descriptive move
(finding what one column's one computation actually is), not an
equivalence claim, so it doesn't compromise §6's conservatism. One
narrower-than-`Lineage` limitation this introduces: where `ir.Lineage`
merges every plausible candidate's *summary* for a genuinely ambiguous
unqualified reference (both `Union` branches matching, or both `Join`
sides), reconstructing a single concrete `Expr` means picking one
candidate deterministically (the first `Union` branch in declared order;
the `Join`'s left side) rather than representing the ambiguity itself.
The `lineage` fingerprint layer is unaffected — it's built from
`ir.Lineage.trace` directly and still unions every candidate's real
sources — so this narrows only the `expression`/`combined` layers'
precision on that one, real but narrow, edge case.

**Encoding**, concretely: every node is `kind byte (Tag=0/Leaf=1) ++
varint length ++ payload`, with a `CTag`'s tag string itself encoded as
its own length-prefixed field rather than mapped through a compact
numeric registry — a deliberate simplification of §10's "short, stable
tag id" suggestion, chosen for implementation simplicity; it costs a few
bytes per node and changes nothing about the unambiguity property the
numeric-table version would also have provided.

**Mutation testing.** A real whole-module `sbt stryker` run currently
scores **96.27%** (129/134 non-static mutants killed, of 146 generated —
12 are `[Ignored]`/static and don't count toward the score), now covered
by CI's own `mutation-testing-fingerprint` job (see "MiMa/mutation-testing
CI wiring, closed" below) rather than only run by hand. The 5 survivors
are the same categories CLAUDE.md's Mutation Testing Requirement already
names as legitimate to leave, not new ones invented for this module:

- Four `StringLiteral` mutants on `require`/exception message text
  (`CanonicalNode.scala`'s malformed-input diagnostics) and one on
  `"NoExpression"` (`TransformationFingerprint.scala`, the tag for a
  defensive fallback branch that is structurally unreachable for any real
  plan — see the code comment at its call site: `resolvedOutputs` and
  `ir.Lineage.trace` are two parallel top-level dispatches over the same
  plan shape, so every name the latter produces already has a matching
  entry in the former by construction). The message-text mutants are the
  exact human-readable-prose category `spark-adapter`'s own mutation-
  testing history documents as not worth chasing (asserting exact
  exception text is brittle and doesn't verify real behavior); the *type*
  of exception thrown for each malformed-input case is tested directly
  instead. `"NoExpression"` is kept as a fail-safe rather than a bare
  `.get`, not chased for coverage, the same "genuinely unreachable given
  how it's called" reasoning `ir.Lineage.scala`'s own documented
  `found.isEmpty` survivor uses.
- One `ConditionalExpression` mutant forcing `Encoding.writeNode`'s
  `fields.nonEmpty` check to always `true` — a documented genuine
  equivalent (see that method's own doc comment, added alongside the
  stack-safety fix below): for empty `fields`, the "forced" branch pushes
  and then immediately pops back out on the very next iteration, with zero
  difference in the bytes written.

Every genuinely load-bearing survivor category from the first,
naive test pass — every canonical tag string (`"Read"`, `"Join"`,
`"Arithmetic"`, ...) silently foldable to `""` with nothing detecting it —
was real and is now fixed (`NodeStructureSpec.scala` pins the exact
`CanonicalNode` structure, tag included, for every `Plan`/`Expr` case),
not waved away: those tags are structural identity, not display prose,
and are exactly what §12's "golden tests, with structural assertions, not
exclusively string snapshots" guidance is for.

**Verification against the real toolchain.** `sbt test` for `fingerprint`
(128 tests) and `spark-adapter`'s full suite (413 tests, run after wiring
§14's integration in) both pass against this repository's real sbt/Scala/
Spark/Delta/Iceberg toolchain, not just a syntax check. `spark-adapter`'s
own assembled fat jar was inspected directly (`unzip -l`) to confirm
`com.invaract.fingerprint`'s compiled classes are bundled into it via
`sbt-assembly`'s ordinary dependency-bundling — the same mechanism that
already bundles `ir`/`contract` into that jar — so a consumer installing
only `invaract-spark-adapter-*.jar` gets fingerprinting for free, with no
separate jar or CI publishing step required for it to reach that
artifact.

**Gap-closing pass.** A follow-up self-review after the initial
implementation above surfaced six real gaps — five since closed, one
confirmed and deliberately left open as scoped follow-up work (see
below):

- **Union/ambiguous-Join resolution paths had zero test coverage** —
  `resolveRefDeepT`'s `Union`/ambiguous-`Join` branches (the "pick the
  first/left candidate deterministically" cases described above) were
  exercised by no test at all. `CanonicalizerSpec.scala` now covers both
  directly, including the "a Union branch whose qualifier can't match is
  skipped in favor of one that does" case and order-sensitivity for both
  `Union` and `Join`.
- **No stack-safety testing — a real, not theoretical, risk.** Every
  genuinely recursive traversal in `Canonicalizer.scala` was plain
  recursion at first, despite the "hundreds of chained `.withColumn()`
  calls" shape being an ordinary, documented translation output (see
  docs/TRANSFORMATION_IR.md's "Derivation classification"), not an edge
  case. Measured directly: a plain-recursive version stack-overflowed on
  a forked default-stack JVM at roughly 700-1700 nested nodes. Fixed by
  trampolining every recursive function in `Canonicalizer.scala` via
  `scala.util.control.TailCalls` (public signatures unchanged — the
  trampoline is purely internal, run via `.result` at each entry point),
  rewriting `CanonicalNode.scala`'s `Encoding.writeNode` as an explicit-
  stack iterative pre-order walk (a `TailRec` trampoline is unnecessary
  there — it only ever appends bytes, never combines children's results,
  so a flat worklist of "remaining fields at this level" suffices), and —
  the same root cause reached through a different module — rewriting
  `ir.Lineage`'s own `outputsOf`/`resolveExpr`/`resolveInScope` the same
  way, since `TransformationFingerprinter.fingerprint` calls `Lineage.
  trace` directly. `StackSafetySpec.scala` regression-tests all of this at
  50,000 levels of depth (two orders of magnitude past the original
  failure point), verified under both a generous and a forked small-stack
  JVM.
- **Narrow property-based test generators.** `PropertyBasedSpec.scala`'s
  `genExpr`/`genPlan` originally covered only a handful of `Expr`/`Plan`
  node kinds. Rewritten to generate every kind of each (including
  `Conditional`, `UDF`, `AggregateCall`, `UnknownExpression`, `Union`,
  `Window`, `UnknownPlan`), so the determinism/round-trip/injectivity
  properties actually exercise the whole canonicalisation surface.
- **`RowMutation` (MERGE/UPDATE/DELETE facts) was invisible to
  fingerprinting entirely** — see "Row mutation facts" in §3 above for the
  fix (`Canonicalizer.canonicalizeRowMutation`, `TransformationFingerprint.
  rowMutation`, `TransformationFingerprinter.fingerprint`'s new optional
  parameter) and §14.2's updated description of how `ContractEnforcementRule`
  now reuses one `RowMutationSupport.classify(plan)` call for both rule
  verification and fingerprinting. Proven end to end (not just at the
  canonicalizer's unit-test level): a real Delta `MERGE INTO`, unchanged
  in every respect except its `ON` condition, now produces two different
  published `TransformationFingerprint`s — `ContractEnforcementRuleSpec`'s
  "computeFingerprint = true: a MERGE's ON condition changing moves the
  published fingerprint" test.
- **A `BinaryType` literal's fingerprint was silently non-deterministic
  across JVM runs, for the exact same literal.** Confirmed directly, not
  assumed: a real Spark `BinaryType` literal's `Literal.value` is a raw
  `Array[Byte]` (`getClass.getName == "[B"`), which fell through
  `LiteralEncoding`'s dispatch to the generic `UNRECOGNIZED_LITERAL_VALUE_
  TYPE` fallback — hashed via `value.toString`, which for a raw JVM
  `Array` is `[B@<identity-hash>`, not content-based, unlike every other
  runtime type this fallback ever actually sees (`ArrayType`/`StructType`/
  `MapType` literals are Catalyst-internal wrapper types — `GenericArrayData`
  and friends — with a stable, content-based `toString`, confirmed
  directly too, e.g. `[1,2,3]` for the identical array twice). This is
  exactly the "same model → same fingerprint" guarantee this whole module
  exists to provide, silently broken for any job with a fixed binary
  literal (a masking key, a hash salt, a default byte-array value).
  `LiteralEncoding.encodeValue` now has a dedicated `Array[Byte]` case
  hashing the actual byte content via `CanonicalNode`'s own `CLeaf`
  wrapper, never `toString`; `LiteralEncodingSpec`/`NodeStructureSpec`
  pin both the content-based determinism and that it's never silently
  routed through the `toString`-based fallback.
- **Every `Expr`/`Plan` match in `Canonicalizer.scala` could silently
  regress into a runtime `scala.MatchError`** the first time a future `ir`
  case class was added without a corresponding case here — Scala compiles
  a non-exhaustive match on a sealed trait with only a warning, not an
  error, by default, so this module's own doc's claim to be "pure and
  total over every node kind in `ir`" was true only by discipline.
  Confirmed directly (not assumed): temporarily removing one real case
  and recompiling reproduced exactly this — a clean compile with an
  easy-to-miss warning, no test failure, until something actually hit
  that node kind at runtime. `fingerprint/build.sbt` now compiles with
  `-Xfatal-warnings`, turning that specific warning into a build failure
  — re-verified with the same temporarily-removed-case test, which now
  fails to compile instead of just warning.
- **An unseeded `rand()`/`random()`/`randn()` call made the fingerprint of
  the exact same, unchanged code different on every single run — a real,
  confirmed false POSITIVE.** Confirmed directly: Catalyst's
  `ResolveRandomSeed` analyzer rule bakes a fresh random `Long` into an
  unseeded call as a genuine child `Expression` at analysis time, not
  Spark-session-scoped state excluded from the plan — analyzing the
  identical `.withColumn("r", rand())` twice in the same JVM produces two
  different seed literals every time, and `SparkPlanAdapter`'s generic
  expression-translation fallback (`ir.Function(name, children)`) faithfully
  carries that seed into the translated, hashed arguments. This is exactly
  the "same model to same fingerprint" guarantee this whole module exists
  to provide, broken for what is likely the single most common
  non-deterministic construct in practice - despite `rand`/`random`/`randn`
  already being correctly classified as non-deterministic in §9's metadata,
  which (by design, see §9) is never consulted by the hash itself.
  `current_timestamp()`/`current_date()`/`now()`/`unix_timestamp()`
  (Catalyst's `ComputeCurrentTime` rule runs at optimization, not analysis
  - Invaract translates the analyzed plan, so these stay zero-value,
  unevaluated function nodes) and `uuid()`/`shuffle()` (their Catalyst
  classes never expose their analyzer-assigned seed via `.children` in the
  first place) were confirmed directly, by the same empirical method, to
  **not** share this bug. Fixed with a targeted exclusion in
  `Canonicalizer.canonicalizeExprT`'s `Function` case
  (`SeedBearingFunctionNames = Set("rand", "random", "randn")`, matched
  case-insensitively since Spark reports different `prettyName` casing per
  call-site alias for the identical class) that drops the argument list
  entirely for exactly these three names - an accepted trade-off is that
  an explicit seed change (`rand(42)` to `rand(43)`) is no longer detected
  either, since nothing post-analysis can distinguish "explicit, unchanged
  seed" from "analyzer-assigned, freshly different seed." Regression-tested
  both at the canonicalizer level (`CanonicalizerSpec.scala`'s "Seed-bearing
  functions: rand/random/randn" section) and end to end against a real
  Spark session (`ContractEnforcementRuleSpec`'s "computeFingerprint = true:
  an unseeded rand() call fingerprints identically across separate
  analyses of the identical code").
- **An unaliased DataFrame-API self-join of the same catalog table could
  make two genuinely different queries fingerprint identically - a real,
  confirmed false NEGATIVE.** See §11's expanded "Positional alias
  substitution" bullet above for the full mechanism, root cause, concrete
  (`.toDF(...)`-based) repro, and the fix
  (`SparkPlanAdapter.computeAliasDisambiguation`). Verified by
  `ContractEnforcementRuleSpec`'s "an unaliased self-join of the same
  catalog table translates the two physical occurrences distinctly" test
  (real Spark session: distinct `Read.alias`/`ColumnRef.qualifier` for
  both occurrences, and two genuinely different output columns —
  `lvalue`/`rvalue`, the left vs. right side's own `value` column —
  fingerprinting differently, where before the fix they collapsed to the
  same qualifier and fingerprint). Scoped Stryker mutation testing on the
  touched `SparkPlanAdapter.scala` method, per CLAUDE.md's Mutation
  Testing Requirement, is tracked in ROADMAP.md rather than asserted here
  with a number this repository's own toolchain hadn't yet confirmed at
  commit time. An earlier draft of this
  investigation also suspected a second repro (referencing a specific
  side's column via a `left(...)`/`right(...)` Dataset-column handle
  after the join, with Spark's `DetectAmbiguousSelfJoin` guard turned
  off) — closer empirical checking showed that one isn't actually
  reachable: Spark's own column-object resolution collapses both handles
  to the identical `exprId` regardless (they were captured before the
  join's own internal deduplication), so there is no genuinely different
  query being conflated there. Worth recording as a reminder that "the
  source code looks different" isn't sufficient evidence of a real
  fingerprint gap — only "Spark itself treats them as different queries"
  is.

Each of the gaps above was found and fixed with the same discipline this
document asks of the code itself: a clean/high mutation score does not,
by itself, prove behavioral coverage of a code path with nothing for
Stryker's own mutators to target (`case (Some(l), Some(_)) => Some(l)` has
no comparison/boolean operator to flip) — the Union/Join gap above is
exactly that shape, and was found by asking "what does this branch
actually do" rather than by trusting an aggregate score. The
binary-literal, exhaustiveness, `rand()`-seed, and self-join-alias gaps
were found the same way: not by running more tests against the existing
code, but by asking what a real Spark session actually produces — for a
literal runtime type, for an unseeded random-function call, for an
unaliased self-join — that this module's tests hadn't yet exercised, and
what the compiler or the canonicalizer would actually let slip through
un-flagged. The self-join gap is also the one place this same discipline
caught its own initial overreach: a first, plausible-looking repro turned
out not to be real once checked against actual Spark execution, and was
retracted rather than shipped as a documented "known limitation."

**MiMa/mutation-testing CI wiring, closed.** `fingerprint/build.sbt` now
sets `mimaPreviousArtifacts`/`versionScheme` (a new `fingerprint/project/
mima.sbt` adds the plugin), `fingerprint` joined `api-compatibility`'s
`for module in contract ir spark-adapter` MiMa-checked list, and a new
`mutation-testing-fingerprint` CI job (mirroring `mutation-testing-ir`
exactly — whole-module Stryker plus the PR-scoped incremental 70% check)
is wired into `summary`'s `needs:`/failure-check. This PR is the one that
first adds `fingerprint/` to the repository, so `api-compatibility` finds
no `base-ref/fingerprint` to diff against and skips it gracefully this one
time — the same position `contract`/`ir`/`spark-adapter`'s own introducing
PR was in (see CLAUDE.md's API Compatibility Requirement); the check runs
for real starting with the next PR that touches this module.

**What's still outstanding**, tracked in ROADMAP.md's fingerprinting
sub-phase rather than repeated here: `fingerprint` joining `contract`/`ir`/
`spark-adapter`'s own Maven Central publishing (Sonatype/PGP) — deferred
per `fingerprint/build.sbt`'s own "FOLLOW-UP" comment, since there is no
previous release to sign or publish against yet. Persistence, publication
(beyond §14's channels), remote comparison, and Spark-plan-extraction
integration remain out of this document's scope entirely, per "Non-goals"
above.

**Spark version upgrade risk (audit).** CLAUDE.md's own "Supporting
Multiple Spark Versions" section already names the underlying gap:
`spark-adapter`'s suite runs against exactly one pinned Spark version
(3.5.1), so anything a future Spark upgrade changes about the *analyzed*
plan's shape is invisible to CI until someone actually performs that
upgrade — it cannot show up as a failing test today. This is a review of
where a real Spark upgrade is likely to change fingerprinting behavior
specifically, based on how Spark's own analyzer has historically evolved,
not a list of things currently broken:

- **The `SeedBearingFunctionNames` exclusion (see the `rand()` gap-closing
  entry above) is a closed, three-name list, not a structural detection of
  "this argument is an analyzer-injected seed."** It works today because
  `ResolveRandomSeed` happens to name its rewritten function nodes exactly
  `rand`/`random`/`randn` (case-insensitively) and stores the seed as that
  node's sole child. A future Spark version adding a new seeded built-in
  (Spark has added several new non-deterministic functions across major
  releases — `uuid()` itself and `typedLit`-based helpers being past
  examples) would silently reintroduce the exact same false-positive bug
  for that new function unless someone remembers to extend this list by
  hand; nothing here would fail existing tests, since they only exercise
  the three names known today. The same applies in reverse if a future
  Spark version changes `Rand`/`Randn`'s internal representation to no
  longer expose the seed via `.children` at all (the way `Uuid`/`Shuffle`
  already don't) — the exclusion would become dead code, harmlessly, but
  silently.
- **ANSI mode becoming the default is a real, scheduled Spark change, not
  a hypothetical one** (Spark's own roadmap flips `spark.sql.ansi.enabled`
  to `true` by default starting with Spark 4.0). Historically, toggling
  ANSI mode changes how implicit casts and arithmetic overflow are
  represented in the *analyzed* plan (Catalyst's `Cast` expression already
  carries an eval-mode discriminator — legacy/ANSI/try — precisely because
  this behavior differs by mode). `SparkPlanAdapter`'s `Cast` translation
  does not currently distinguish eval modes. Upgrading Spark and picking
  up the new ANSI default would very plausibly change fingerprints for
  jobs whose own code never changed — arguably *correct* per this
  document's own "same model, same fingerprint" framing (the runtime
  overflow/null behavior genuinely did change), but worth calling out
  explicitly since a team upgrading Spark should expect fingerprint
  churn from this specific cause, not assume something in their job broke.
- **`UnknownPlan`/`UnknownExpression.sourceType` (`getClass.getSimpleName`
  on the untranslated Catalyst node) is hashed, and Catalyst's internal
  class names are not a stability contract Spark makes to anyone.**
  Internal Catalyst classes have been renamed and restructured across
  major versions before (e.g. `DataSourceV2Relation`'s own shape changed
  materially between Spark 3.0 and 3.3, which is why `spark-adapter`
  already has version-specific handling for it — see docs/SPARK_ADAPTER.md).
  Any node that currently falls through to `UnknownPlan`/`UnknownExpression`
  because `spark-adapter` has no dedicated case for it will silently change
  fingerprint whenever the underlying Catalyst class is renamed by a
  version upgrade, purely as a byproduct of the upgrade, not a real change
  to the job. This is inherent to the fallback's design (documented in §8
  as intentionally conservative) and is flagged here as the version-drift
  angle on that same, already-accepted trade-off.
- **`AttributeReference.qualifier.lastOption`'s truncation to the final
  namespace segment (see `SparkPlanAdapter`'s `AttributeReference` case)
  is a latent collision risk that gets more likely, not less, as Spark's
  own multi-catalog support matures.** Spark 3.x's catalog-plugin API
  (introduced as a preview in 3.0, matured across the 3.x line) makes
  three-and-more-part qualified names (`catalog.schema.table`) increasingly
  common in real deployments; `ColumnRef.qualifier` only ever keeps the
  last segment, so two distinctly-qualified tables that happen to share a
  final segment name (`catalog_a.sales.orders` vs. `catalog_b.sales.orders`)
  would collide the same way the unaliased-self-join case above does. This
  was raised during this audit but not empirically confirmed or fixed —
  flagged here as the concrete, upgrade-relevant version of a risk that
  already exists today and only grows as multi-catalog usage increases.
- **`SubqueryAlias`'s default-aliasing behavior for unaliased catalog
  references (the root mechanism behind the self-join alias-collision
  bug above, and the basis of `computeAliasDisambiguation`'s own fix for
  it) is a Catalyst analyzer implementation detail, not a documented
  Spark API contract.** `SparkPlanAdapter`'s translation of it, this
  design's reliance on `Read.alias` being distinct whenever two `Read`s
  are actually different, and the fix's own `plan.collect { case sa:
  SubqueryAlias => sa }` pre-pass all depend on that detail continuing to
  behave the way it does today. A future Spark version could change
  *when* it inserts a default `SubqueryAlias`, or use a different default
  name — Invaract would inherit whatever new behavior results without any
  code change on its side, for better or worse, and no current test would
  catch a regression since only one Spark version is under test.
- **Spark's `DetectAmbiguousSelfJoin` safeguard (`spark.sql.analyzer.
  failAmbiguousSelfJoin`) is itself version-sensitive** — its rule set and
  default have evolved since its introduction in Spark 3.0, and a future
  version could narrow or widen what it catches. Since the self-join
  collision bug above is only reachable in practice when this guard is
  off (by explicit user config) or doesn't cover a given query shape, a
  Spark upgrade that changes this rule's coverage directly changes how
  exposed real users are to that already-documented gap, independent of
  anything in this repository.

None of these are proposed as fixes to make now — the point, consistent
with the rest of this "Implementation notes" section, is that they would
not surface as a CI failure today (single pinned Spark version, no
compatibility matrix — see CLAUDE.md's "Supporting Multiple Spark
Versions") and so are worth a deliberate look the next time `spark-adapter`
takes on a new Spark version, not something to assume will announce itself.
