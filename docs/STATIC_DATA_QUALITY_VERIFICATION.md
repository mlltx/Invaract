# Static Data Quality Contract Verification — Design

**Status: proposal, not implemented.** This document answers the ten questions the
brief poses, proposes a domain model and analysis approach, defines the MVP scope,
and gives representative test cases. No production code accompanies it. It should be
read, argued with, and revised before any of it is built — the same role
`SEMANTIC_LINEAGE_FINGERPRINTING.md` played for that capability before it existed.

## 0. One-paragraph summary

A data-quality requirement (`NOT NULL`, `= 'GBP'`, `IN ('ACTIVE','INACTIVE')`,
`>= 0`) becomes a declaration on a contract's output field, exactly the way
`required`/`nullable` already are. A new, purely structural analysis — architecturally
a sibling of `ir.Lineage`, not a replacement for it — walks the same `ir.Plan`/`ir.Expr`
tree `Lineage.trace` already walks, propagating a small, closed algebra of provable
value-domain facts (not null, equals a constant, one of a finite set, numeric bound)
from `Read` columns (seeded by the *input* contract's own declarations, trusted as
axioms) through `Filter`/`Join`/`Aggregate`/`Cast`/`Arithmetic`/`Conditional`/... to
each output column. Each output-side DQ declaration is then compared against what was
proven, producing one of four verdicts — `GUARANTEED`, `NOT_GUARANTEED`, `VIOLATED`,
`NOT_STATICALLY_VERIFIABLE` — never collapsed to pass/fail. Only `VIOLATED` (a positive,
demonstrated contradiction) blocks a write the way an ordinary structural `Violation`
does; the other three are reporting-only, the same non-blocking role `Diagnostic`
already plays for translation gaps. The MVP supports four property kinds and a
deliberately small, hand-maintained set of transfer functions per plan/expression node
— unsupported constructs (UDFs included) fall through to `NOT_STATICALLY_VERIFIABLE`,
never to a guess.

---

## 1. Where this sits in the existing architecture

```
Spark Logical Plan
        │  SparkPlanAdapter.translate (unchanged)
        ▼
  ir.Plan / ir.Expr                     ← the one semantic IR; no new parallel model
        │
        ├─ ir.Lineage.trace              (existing: column-level provenance)
        │
        └─ ir.PropertyAnalysis.analyze   (new: value-domain provenance — this doc)
                        │
                        ▼
        spark-adapter.StaticDataQualityVerifier   (new: contract-aware glue)
                        │  reads contract.inputs[].schema.fields[].constraints as axioms
                        │  reads contract.outputs[].schema.fields[].constraints as obligations
                        ▼
              VerificationResult.dataQuality: List[DataQualityCheckResult]   (new field)
```

This is a direct extension of a pattern the codebase already uses twice:

- **`ir.Lineage`** (pure, engine- and contract-independent) traces which `Read` columns
  an output column derives from and how (`DerivationKind`). It never sees a `Contract`.
- **`spark-adapter.SensitivityLineage`** (contract-aware) cross-references `Lineage`'s
  output against `Contract.inputs[].schema.fields[].sensitivityTags`, propagating a
  *declared input property* forward to every output column that derives from it.

Static DQ verification is the same two-layer split, generalized: instead of one fixed
property (`sensitivityTags`, propagated unconditionally through any derivation), it
tracks several property *kinds*, each with its own operation-aware transfer function
(a `Cast` may preserve `NOT NULL` but not `EqualsConstant`; a `UDF` preserves nothing).
That is genuinely new work — `SensitivityLineage`'s "any transitive touch is enough"
model is not sound for a nullability or range guarantee — but the *shape* of the
solution (pure `ir`-level structural walk + a `spark-adapter`-level `Contract`-aware
layer that seeds axioms and checks obligations) is not new to this codebase, and this
design deliberately does not deviate from it.

`ir.PropertyAnalysis` does **not** reuse `Lineage.trace`'s output directly —
`DerivationKind` (`Direct`/`Constant`/`Computed`/`Opaque`) is too coarse for this: two
`Computed` columns built from different operations can have completely different
provable properties (`ABS(x)` establishes `x >= 0`; `x || y` establishes nothing about
range). `PropertyAnalysis` is its own bottom-up walk, structurally parallel to
`resolveExprT`/`outputsOfT` (same node coverage, same trampolined-recursion
stack-safety concern — see `Lineage`'s own doc on why a plain-recursive version of this
class of walk has already stack-overflowed once in this codebase, for `Lineage`
itself), but computing a different summary per node.

---

## 2. Domain model

### 2.1 The four-state verdict

```scala
sealed trait DataQualityVerdict
object DataQualityVerdict {
  /** The transformation's semantics prove the property holds for every
    * row the write can produce, given the input contract's own axioms. */
  case object Guaranteed extends DataQualityVerdict

  /** Analysis completed — every construct on the path to this column was
    * understood — but the known facts don't entail the required property.
    * This is the default when there simply isn't enough information, not
    * a claim that the property is false. */
  case object NotGuaranteed extends DataQualityVerdict

  /** Analysis proves the transformation CAN produce a value that fails
    * the required property, given only what the input contract itself
    * guarantees — a positive, demonstrated contradiction, not merely an
    * absence of proof. */
  case object Violated extends DataQualityVerdict

  /** Analysis reached a construct it does not have a trusted semantic
    * model for (a UDF, an unsupported `Function`, an `UnknownExpression`,
    * a join/plan shape this module doesn't reason about) on the path
    * that would be needed to decide the property either way. */
  case object NotStaticallyVerifiable extends DataQualityVerdict
}
```

These are **not** a linear pass/fail scale and must never be collapsed to one. The
ordering that matters is: `Guaranteed` and `Violated` are both *positive claims*
(the analysis is confident either way); `NotGuaranteed` and `NotStaticallyVerifiable`
are both *absence of a positive claim*, differing only in whether the absence is
because the facts genuinely don't entail it (`NotGuaranteed`) or because analysis
couldn't reach a conclusion at all (`NotStaticallyVerifiable`) — a distinction worth
keeping separate for reporting (a contract author fixing a `NotStaticallyVerifiable`
by rewriting a UDF into an understood expression is a different action than fixing a
`NotGuaranteed` by adding a filter), even though both are equally "not proven" for
enforcement purposes (§6).

### 2.2 The property algebra

A deliberately small, closed set — **not** a general constraint language, and not
extensible by a contract author (the brief is explicit: "design around rule semantics,"
not "let people write arbitrary predicates for Invaract to prove"). Each kind has its
own transfer functions per plan/expression node (§4); adding a fifth kind later is a
new, reviewed piece of work, not a configuration change.

```scala
sealed trait Property
object Property {
  /** The value is never SQL NULL. */
  case object NotNull extends Property

  /** The value is always exactly one literal (a `EqualsConstant`-required
    * rule is a special case of `OneOf` with one element, but kept as its
    * own case since "equals a constant" is a common, simply-stated rule
    * worth a direct, unambiguous representation rather than always going
    * through a one-element set). */
  case class EqualsConstant(value: Any, literalType: String) extends Property

  /** The value is always one of a known, finite set of literals. */
  case class OneOf(values: Set[Any], literalType: String) extends Property

  /** A numeric bound, each side independently optional and independently
    * inclusive/exclusive — `amount >= 0` is `Range(gte = Some(0))`;
    * `age BETWEEN 0 AND 120` is `Range(gte = Some(0), lte = Some(120))`.
    * At most one of `gte`/`gt` and at most one of `lte`/`lt` is ever
    * populated for one Property value (a normalized representation, not a
    * general interval-with-both-a-closed-and-open-bound-on-one-side
    * shape) — normalization happens once, at construction.
    */
  case class Range(gte: Option[BigDecimal] = None, gt: Option[BigDecimal] = None,
                    lte: Option[BigDecimal] = None, lt: Option[BigDecimal] = None) extends Property
}
```

A column's analyzed state is a **conjunctive set** of `Property` values it's been
proven to satisfy (`NotNull` and `Range(gte=0)` can both hold at once; `EqualsConstant`
and a *different* `EqualsConstant` cannot, and the analysis never constructs that —
see §4.7 for how a self-contradiction earlier in the pipeline is handled) — plus,
separately, a **refuted set**: properties the analysis has positively shown do NOT
universally hold (used only to derive `Violated`, never surfaced as a `Property` a
column "has"). This mirrors `NonDeterminism`'s existing `Option[Boolean]` tri-state
idiom in this codebase, generalized from one boolean fact to several independent
property kinds each independently provable, refuted, or unknown.

```scala
/** One column's analyzed value-domain state. `equalsConstant`/`oneOf`/
  * `range` are each `None` when nothing is known (absence of proof is not
  * proof of absence). Only `notNull` carries a separate positive "refuted"
  * state at this layer — whether an `equalsConstant`/`oneOf`/`range`
  * envelope escapes a required obligation (`Violated`) is derived once, by
  * the caller comparing this state against the contract's declared
  * obligation (§3.6), not pre-computed and carried through every
  * intermediate plan node.
  */
case class ColumnPropertyState(
  notNull: NullabilityFact,                  // Proven | Refuted | Unknown - see NonDeterminism precedent
  equalsConstant: Option[Property.EqualsConstant] = None,
  oneOf: Option[Property.OneOf] = None,
  range: Option[Property.Range] = None        // the *envelope*: tightest proven superset of possible values
)
```

### 2.3 Where a DQ rule lives in the contract

**`NOT NULL` needs no new syntax.** `Field.nullable = false` (parsed today, default
`!required`) already states exactly this requirement. Static DQ verification is an
*additional, complementary* attempt to prove what `nullable: false` already declares —
it doesn't replace `StructuralVerifier`'s existing schema-level nullability check (which
reads Spark's own reported `StructType` nullable flag, a different and still-useful
signal — see §6 for how the two coexist), and it doesn't require touching a single
existing contract. Every contract that already declares `nullable: false` gets a free
attempt at a stronger proof the moment static DQ verification is turned on.

The other three kinds have no existing representation and need a new, field-level,
optional list — mirroring `sensitivityTags`' precedent of "a small, open list attached
directly to `Field`," not a new top-level contract section (these are properties of a
*column*, and `Field` is already where column-level contract facts live):

```yaml
schema:
  fields:
    - name: currency
      type: string
      constraints:
        - type: equals
          value: GBP
    - name: status
      type: string
      constraints:
        - type: oneOf
          values: [ACTIVE, INACTIVE]
    - name: amount
      type: double
      nullable: false
      constraints:
        - type: range
          gte: 0
```

```scala
case class FieldConstraint(constraintType: String, properties: Map[String, Any])

object FieldConstraintType {
  val Equals = "equals"
  val OneOf  = "oneOf"
  val Range  = "range"
  val All: Set[String] = Set(Equals, OneOf, Range)
}

sealed trait InterpretedFieldConstraint
object InterpretedFieldConstraint {
  case class Equals(value: Any, literalType: String) extends InterpretedFieldConstraint
  case class OneOf(values: Set[Any], literalType: String) extends InterpretedFieldConstraint
  case class Range(gte: Option[BigDecimal], gt: Option[BigDecimal],
                    lte: Option[BigDecimal], lt: Option[BigDecimal]) extends InterpretedFieldConstraint
}
```

`Field` gains one new field: `constraints: List[FieldConstraint] = Nil`. `FieldConstraint`
decodes into `InterpretedFieldConstraint` the same way `ContractRule.interpret` already
decodes a rule — same "closed, hand-maintained set; malformed properties decode to
nothing and `ContractValidator` reports it as an `Error`" pattern as `RuleType`/
`InterpretedRule` (§5 has the exact validator rules).

**The same `Field`/`constraints` shape is used on both `inputs` and `outputs`** — role
is determined purely by which side of the contract the field sits on, exactly like
`required`/`nullable` already work today. On `inputs`, a `constraints` entry (and
`nullable: false`) is an **axiom**: trusted as given, seeding the analysis, *never*
independently re-verified against real input data by this module (that would be runtime
DQ against the input, explicitly out of scope — see §9). On `outputs`, a `constraints`
entry (and `nullable: false`) is an **obligation**: the thing `StaticDataQualityVerifier`
attempts to prove.

This trust boundary is the same one `StructuralVerifier`'s existing input-schema
checking already has (a declared input type/nullability is trusted, not re-derived from
real data) — worth stating explicitly rather than leaving implicit, since it's the
crux of why this is *static* verification and not a promise about actual data (§8).

---

## 3. Analysis approach

### 3.1 Seeding axioms from the input contract

For each `Read(dataset, alias, _)` node in the plan, `StaticDataQualityVerifier` builds
an initial `ColumnPropertyState` per column **by name**, from whichever `contract.inputs`
entry's `location` matches that `Read`'s dataset (the same `StructuralVerifier.locationsMatch`
normalized-suffix rule every other location-based lookup in this codebase already uses —
not a second copy of it):

- `nullable: false` on the input field → `notNull = Proven`.
- Each `constraints` entry on the input field → the corresponding `Property` in `proven`.
- No matching input dataset, or no matching field, or the field declares nothing → every
  slot starts `Unknown` (not `Refuted` — the *absence* of a declared axiom is not
  evidence of anything; only an explicit `nullable: true` combined with *no* other
  information stays `Unknown`, never `Refuted`, since "may be null" is not "proven
  nullable" — see §4.6 for the one case that *does* prove nullable: a join that
  structurally introduces it).

This is `ir.PropertyAnalysis.analyze`'s one and only external input beyond the plan
itself: `axioms: Map[ColumnRef, ColumnPropertyState]`, keyed by the `Read`'s own
resolved qualifier (dataset location or alias) exactly the way `ir.Read.catalog`/
`Lineage`'s own `Read` case already key by scope — `PropertyAnalysis` itself never sees
a `Contract`; `StaticDataQualityVerifier` builds this map and passes it in, the same
division of labor `SensitivityLineage` already has with `Lineage`.

### 3.2 Bottom-up propagation through `Plan`

Structurally identical traversal shape to `Lineage.outputsOfT`/`resolveExprT` — same
node coverage, same trampolined recursion for the same stack-safety reason — computing
`ColumnPropertyState` instead of `Provenance`:

| Plan node | Effect |
|---|---|
| `Read` | Bottoms out at the seeded axiom for that column (§3.1), or `Unknown` everywhere. |
| `Project` | Each output column's state is its expression's resolved state (§3.3). |
| `Aggregate` | **MVP: every aggregate output is `Unknown` in every slot**, deliberately — see §4.4. `groupBy` columns passed through unchanged when they're a bare `ColumnReference` (the common case), since grouping doesn't alter a key column's own domain. |
| `Filter` | Recurses into `input`, then *narrows* the result using `condition` — see §3.3. |
| `Join` | Recurses into both sides, then demotes (to `Unknown` in every slot, never `Refuted`) every column sourced from a side the `JoinType` can synthetically null-fill — see §4.4. |
| `Sort` / `Limit` | Pure pass-through — recurse into `input` unchanged (neither can introduce or remove a violating value; `Limit` narrows the *row count*, never a column's value domain). |
| `Union` | Per output column, the **union** of each branch's state (§4.5's union rule — the same "weakest branch wins, except a jointly-refuted fact stays refuted" logic `Conditional`'s branches already need). |
| `Window` | **MVP: `Unknown` for every windowed output column**; a plain pass-through input column keeps its own state, mirroring `Aggregate`'s treatment (window functions share `AggregateCall`'s expression node — see §4.4). |
| `UnknownPlan` | `Unknown` for anything that would need to resolve through it — the same "nothing to trace past it" rule `Lineage` already has. |

### 3.3 Expression-level transfer functions, and predicate narrowing

Each `Expr` node's resolved `ColumnPropertyState` (mirroring `resolveExprT`'s per-node
match):

| `Expr` node | Rule |
|---|---|
| `ColumnReference` | Resolves in-scope exactly like `Lineage`'s own `resolveInScopeT` (same passthrough-following, same `Join`/`Union` ambiguous-name handling) and returns that column's current state. |
| `Literal(v, t)` | `EqualsConstant(v, t)` and, for a numeric literal, `Range(gte=v, lte=v)`; `notNull = Proven` unless `v == null`, in which case `notNull = Refuted` (a typed SQL `NULL` literal — see `Literal`'s own doc — genuinely, unconditionally produces null). |
| `Alias` | Inherits the inner expression's state verbatim — a rename proves and refutes nothing new. |
| `Cast(inner, targetType)` | `notNull` passes through unchanged (a cast of a non-null value is non-null; Spark's *unsafe* cast producing null on failure — e.g. a bad string→int parse — is a real gap, explicitly flagged as a known unsoundness risk in §7 unless the source type/target type pair is on a small "known-safe" allowlist, e.g. any numeric widening, a widening decimal cast). `equalsConstant`/`oneOf`/`range` are **dropped** (`Unknown`) in the MVP — even though some casts trivially preserve them (widening an `int` `Range` to `long` preserves the same bound), computing the target-type-aware transformation correctly for every (`equals`/`oneOf`/`range`) × (source type, target type) pair is exactly the kind of broad-but-fragile inference this design's own principle rules out for a first pass; a later, explicitly-reviewed pass can add a small allowlist of provably-safe cast pairs. |
| `Arithmetic(op, operands)` | **MVP: only a small, explicit whitelist of transfer functions**, everything else `Unknown`: <br>• unary `NEGATE`: `Range(gte=a, lte=b)` → `Range(gte=-b, lte=-a)` (swap and negate both bounds; an absent bound stays absent, i.e. negating `Range(gte=0)` — no upper bound — gives `Range(lte=0)` — no lower bound). <br>• `+`/`-` where **one** operand is a `Literal` and the other carries a `Range`: shift the bound by the literal's value. <br>• `*` where one operand is a **non-negative literal constant** and the other carries a `Range`: scale both bounds by it (a *negative* literal constant is handled via the `NEGATE` rule composed with positive scaling, not a separate case). <br>Every arithmetic result's `notNull` is `Proven` only if **every** operand's `notNull` is `Proven` (SQL arithmetic on a null operand is null) — this holds regardless of whether the specific operator is on the range-whitelist. `equalsConstant`/`oneOf` are dropped for any non-trivial (non-both-literal) arithmetic. |
| `Comparison` | Produces a `Boolean`-typed value — no `Range`/`OneOf`/`EqualsConstant` of its own kind (a `Comparison`'s *result* isn't itself a `Range`). Its role is entirely in **narrowing** (below), not in describing its own value's domain — the MVP does not track a `Property.OneOf(Set(true,false))`-shaped boolean domain, since no example rule needs it. `notNull`: `Proven` only if both operands are `Proven` non-null (SQL's two-valued-logic subtlety — `NULL = NULL` is `NULL`, not `true`/`false` — makes this the only sound rule without deeper 3-valued-logic modeling, itself out of MVP scope). |
| `BooleanExpr` | Same non-domain-bearing treatment as `Comparison`; its narrowing role is described below. |
| `Conditional(branches, elseValue)` | **No `elseValue` → every slot `Unknown`, unconditionally** (a CASE with no ELSE produces SQL `NULL` for a non-matching row; proving branch exhaustiveness is out of MVP scope — see §8). With an `elseValue`: each branch's *value* expression is resolved **narrowed by that branch's own condition, conjoined with the negation of every earlier branch's condition** (§3.4) — this is what makes Example 4 (a clamp) provable: the `ELSE amount` branch is resolved knowing `NOT(amount < 0)`, i.e. `amount >= 0`, holds. The overall result is the **union** (§4.5) of every branch's (narrowed) value state, `elseValue`'s included. |
| `Function(name, args)` | **MVP: a small, explicit, hand-maintained allowlist** (mirroring `NonDeterminism.knownNonDeterministicFunctionNames`'s precedent exactly) of functions with a *trusted* semantic definition, e.g. `abs` (establishes `Range(gte=0)` regardless of the argument's own range — a rare case where the transfer function doesn't need the operand's own bound at all), `coalesce(x, <non-null literal>)` (establishes `notNull = Proven` regardless of `x`'s own nullability). Every other function name → `Unknown` in every slot, `notNull` computed the same "Proven only if every arg is Proven" rule as `Arithmetic`. **Not** a `NotStaticallyVerifiable`-forcing case on its own (a `Function` is still a claim this IR understands the *shape* of the computation — see `Expr.scala`'s own doc — just not one this analysis has a value-domain rule for); it only becomes the reason a specific rule can't be proven, exactly like any other `Unknown`. |
| `UDF` | **Always `Unknown` in every slot, `notNull` included** — opaque by design (`ir.UDF`'s own doc), and per the brief's explicit instruction, never assumed to preserve anything without "an explicit, trusted semantic definition for that function," which a UDF by construction doesn't have. |
| `AggregateCall` | Only reachable when resolving an `Aggregate`/`Window` output directly (§3.2 already makes those `Unknown` wholesale in MVP) — not separately handled. |
| `UnknownExpression` | `Unknown` in every slot — the expression-level counterpart to `UnknownPlan`. |

### 3.4 Predicate narrowing (`Filter`, `Join` conditions, `CASE` branches)

The mechanism `Filter` and `Conditional` both need — "given that predicate `P` holds
(or fails, for a `CASE` branch already ruled out), what new facts can be added to the
columns it constrains" — is the exact same **polarity-aware, De Morgan-/`NOT`-aware
top-level-`AND` walk** `spark-adapter.EqualityConditions` already implements for
`merge_condition`/`required_join_columns`'s narrower "which columns are equality-paired"
question. This module needs a **broader** vocabulary of established facts (not just
equality pairing), so it is proposed as a **new, sibling `ir`-level module**
(`ir.PredicateFacts`, pure — `EqualityConditions` itself stays in `spark-adapter` since
it's used by `RuleVerifier`/`PlanRuleVerifier`, but the AND/OR/NOT polarity recursion it
already contains has no `spark-adapter`/`contract` dependency and is the right pattern
to generalize, not the right *code* to import into `ir`, which cannot depend on
`spark-adapter`) with the identical recursive shape:

```
requiredFacts(expr, negated = false): Map[ColumnRef, ColumnPropertyState]

AND(a, b) asserted true   → union of requiredFacts(a) and requiredFacts(b) (both hold)
AND(a, b) asserted false  → {} (De Morgan: only "at least one failed," not which)
OR(a, b)  asserted true   → {} (only "at least one holds," not which)
OR(a, b)  asserted false  → union of requiredFacts(a, negated) and requiredFacts(b, negated)  (De Morgan)
NOT(x)                    → requiredFacts(x, !negated)

Comparison("=", ColumnReference(c), Literal(v, t)) asserted true
                           → { c -> EqualsConstant(v, t) [+ Range(gte=v, lte=v) if numeric] }
Comparison(">"/">="/"<"/"<=", ColumnReference(c), Literal(v, t)) asserted true
                           → { c -> Range(<the corresponding single bound>) }
  and asserted false      → { c -> Range(<the negated bound — see the small inversion
                                table below; this is what makes Example 4's ELSE
                                branch provable>) }
Function("ISNOTNULL"/"ISNULL", [ColumnReference(c)]) asserted true/false
                           → { c -> NotNull Proven } / { c -> NotNull Refuted }
                             (confirmed empirically — SparkPlanAdapterSpec already
                             has a passing assertion for `Function("ISNULL", ...)`;
                             `Function("ISNOTNULL", ...)` confirmed directly via a
                             throwaway probe, since deleted)
Function("IN", ColumnReference(c) :: literals) asserted true
                           → { c -> OneOf(literals.map(_.value).toSet, type) }
                             (confirmed empirically via a throwaway probe, since
                             deleted: `IN (...)` stays as its own `Function("IN", ...)`
                             node in the analyzed plan — Spark does not desugar it to
                             an OR-of-equalities until the optimizer, which this
                             translator never sees — so this needs no OR-chain
                             special case)
everything else            → {}
```

Comparison-negation table (used only for the "asserted false" rows above — a small,
explicit, hand-written table, the same "conservative and closed" idiom
`EqualityConditions`'s own De Morgan handling already uses, not a general algebraic
solver):

| Asserted-true operator | Negation adds |
|---|---|
| `>` | `<=` |
| `>=` | `<` |
| `<` | `>=` |
| `<=` | `>` |
| `=` | *(nothing — "not equal to a specific value" doesn't tighten a `Range`/`OneOf`)* |

`Filter(input, condition)`'s own resolved state, per output column, is: `input`'s
already-resolved state, with `requiredFacts(condition)` **merged in** (a narrowing
merge only ever *adds* a proven fact or *tightens* a `Range`'s existing bound toward the
new one — it never removes an already-proven fact, and a `Range` merge takes the
*tighter* of the two bounds on each side independently).

### 3.5 Join semantics

`Example 6`'s intuition — an `INNER JOIN` structurally excludes non-matching rows, so
a column from either side keeps whatever the side itself already established, while a
`LEFT OUTER JOIN` can synthetically null-fill the *entire* right-side row for an
unmatched left row — generalizes to a per-`JoinType`, per-side rule that demotes
**every** proven fact about a nullable-side column to `Unknown` (never to `Refuted` —
demotion means "no longer provably X," not "provably not-X"; a `LeftOuter` join's right
side genuinely *can* still always be non-null if the underlying data happens to have a
match for every row, this analysis simply can no longer prove it structurally):

| `JoinType` | Left side | Right side |
|---|---|---|
| `Inner`, `Cross` | preserved | preserved |
| `LeftOuter` | preserved | demoted to `Unknown` |
| `RightOuter` | demoted to `Unknown` | preserved |
| `FullOuter` | demoted to `Unknown` | demoted to `Unknown` |
| `LeftSemi`, `LeftAnti` | preserved | *(right-side columns don't appear in the output at all — Spark's own semantics; nothing to demote)* |

This directly reproduces Example 6: `customer_id`/any column sourced from `customers`
through an `INNER JOIN` keeps whatever `NotNull` fact the `customers` input contract
itself established; the same column through a `LEFT JOIN` is demoted to `Unknown`,
so a `NOT NULL` obligation on it comes back `NotGuaranteed` (not `Violated` — the
analysis has no evidence it's *ever* actually null, only that it can no longer prove it
isn't).

**Deliberately not modeled**, per the brief's own caution: row-level cardinality or
uniqueness. `Inner` demonstrates "this column's *value*, where present, inherits its
source side's proven facts" — it says nothing about *how many* output rows exist per
input row, and this design does not attempt to prove "at most one customer per
transaction" or anything else that depends on a uniqueness assumption the contract
doesn't itself separately declare (a possible, explicitly out-of-MVP future extension:
a `unique` field-level declaration on an *input*, usable as a further axiom — not
proposed here).

### 3.6 From a column's analyzed state to a verdict

Given a required obligation `R` (one `Property`, or `NotNull`) on an output field, and
that column's analyzed `ColumnPropertyState`:

1. **`NotNull` required:**
   - `notNull == Proven` → `Guaranteed`.
   - `notNull == Refuted` → `Violated`.
   - `notNull == Unknown` → `NotGuaranteed`, **unless** the analysis actually hit an
     unsupported construct (a `UDF`/`Function` outside the allowlist/`UnknownExpression`)
     on the resolution path for *this specific column* — in which case
     `NotStaticallyVerifiable` (§3.7 distinguishes the two `Unknown` causes).

2. **`EqualsConstant(v)` required:**
   - Proven `equalsConstant` matches `v` exactly → `Guaranteed`.
   - Proven `equalsConstant` is a *different* literal → `Violated` (structurally,
     every possible output value is the wrong one).
   - Proven `oneOf`/`range` demonstrably excludes `v` entirely (e.g. `oneOf` is a
     finite set not containing `v`, or `range` is entirely on one side of `v`) →
     `Violated`.
   - Otherwise → `NotGuaranteed` / `NotStaticallyVerifiable` per the same "hit an
     unsupported construct" test as above.

3. **`OneOf(values)` required:**
   - Proven `oneOf` is a **subset** of `values` → `Guaranteed`.
   - Proven `equalsConstant`/`v` ∈ `values` → `Guaranteed` (a proven single value is
     trivially a subset-of-one).
   - Proven `oneOf` (or `equalsConstant`) contains **any** value outside `values` →
     `Violated` — the transformation can structurally produce something the contract
     forbids.
   - Otherwise → `NotGuaranteed` / `NotStaticallyVerifiable`.

4. **`Range(...)` required:**
   - Proven `range` (the analyzed envelope) is a **subset** of the required range
     (compare each present bound; an absent bound on either side is treated as
     `±∞` for the subset/overlap tests below) → `Guaranteed`.
   - Proven `range`'s overlap with the *complement* of the required range is
     **non-degenerate** — more than a single shared boundary point, using each
     bound's own inclusive/exclusive-ness to decide whether a shared endpoint counts
     as overlap or not (Example 7's own case: proven envelope `(-∞, 0]` against
     required `[0, +∞)` — the only shared point is exactly `0`, which the required
     range's own `gte` (inclusive) *does* satisfy at that single point, but the proven
     envelope also contains every value strictly less than `0`, which the required
     range's complement `(-∞, 0)` *does* fully contain — non-degenerate overlap,
     `Violated`) → `Violated`.
   - A proven envelope that overlaps only at a single shared boundary point (and no
     further) is conservatively **not** `Violated` — it falls to `NotGuaranteed`,
     consistent with "a false `Violated` (wrongly blocking a legitimate write) is a
     real cost too, not just a false `Guaranteed`."
   - Otherwise → `NotGuaranteed` / `NotStaticallyVerifiable`.

### 3.7 Distinguishing `NotGuaranteed` from `NotStaticallyVerifiable`

`PropertyAnalysis` tracks, per column, not just the final `ColumnPropertyState` but
whether resolution touched an **unsupported node** anywhere on the path (a `UDF`, a
`Function` outside the allowlist, an `UnknownExpression`, an `Aggregate`/`Window`
output in MVP scope, a `Cast` whose source/target pair isn't on the safe list for the
specific property in question) — a boolean carried alongside the state, the same
"was this ever opaque" question `Lineage`'s own `DerivationKind.Opaque` already answers
for provenance, computed independently here since (per §1) `DerivationKind` itself
isn't reused. When that flag is set for the specific property kind a rule needs,
an otherwise-`Unknown` result reports `NotStaticallyVerifiable` instead of
`NotGuaranteed` — "we don't know because we couldn't look," not "we looked and it
isn't there."

### 3.8 Struct/nested fields

`Field.properties` (docs/CONTRACT_MODEL.md) already lets a contract author declare
`nullable`/`constraints` on a *nested* field of a struct/record-typed column — the
schema model doesn't distinguish a top-level field from a nested one at all.

Struct member access is now a real, first-class part of the IR: `ir.Expr` gained
`StructField(struct: Expr, fieldName: String)` and `StructConstruct(fields: List[(String,
Expr)])`, `SparkPlanAdapter` translates Catalyst's `GetStructField`/`CreateNamedStruct`
into them (matched *before* the generic `Function`/`UnknownExpression` fallback — both
constructs' own `prettyName`s are unhelpful/generic, `"getstructfield"` and
`"named_struct"`, which would otherwise lose the actual field name entirely), and
`ir.Lineage`/`ir.PlanPrinter`/`fingerprint`'s `Canonicalizer`/`NonDeterminism` all carry
real cases for both. This closes a genuine, independent mistranslation bug (a struct
field access or construction used to be silently absorbed into a lossy `Function`/
`UnknownExpression` node), regardless of what it enables for data-quality verification.

`PropertyAnalysis` builds on this with one deliberately bounded transfer function: the
*"construct, then extract one of its own fields, in the same plan"* pattern —
`StructField(struct, fieldName)` where `struct` statically *reduces* to a `StructConstruct`
(`reduceToStructConstruct`) — resolves straight through to that field's own value
expression, exactly like Example 3's `CASE WHEN` resolution but for a struct field instead
of a flat column. The reduction is recursive, not just single-level: `struct` can itself be
a `StructField` chain extracting a field that was built as a further `StructConstruct`, to
any nesting depth (`struct(geo = struct(code = "XYZ")).geo.code` resolves the same way a
single-level `struct(...).field` does) — this matters for both a top-level output column
and a `Field.properties`-declared nested obligation reaching two or more levels deep (see
below). A freshly-built `StructConstruct` is also unconditionally `notNull = Proven`
(constructing a struct is never itself SQL `NULL`, independent of any individual field's
own nullability). Both are exercised automatically by `StaticDataQualityVerifier.verify`'s
existing top-level `PropertyAnalysis.analyze` call — no `StaticDataQualityVerifier` code
change was needed for a *top-level* output field whose own expression happens to take this
shape (e.g. `struct(col("zip"), col("city")).getField("zip").as("just_zip")` now resolves
to a real `Guaranteed`/`Violated`/`NotGuaranteed` verdict, not `NotStaticallyVerifiable`).

Any *other* struct-valued expression — a bare reference to a struct-typed column (e.g.
one read straight from a `Read`, with no `StructConstruct` in the same expression), the
result of a UDF, or a nested `StructField` reached through one of those — still resolves
to `Unknown`-with-`unsupported`: this analysis has no axiom representation for a struct's
own *internal* fields (axioms are seeded per flat `Read`-scoped column only, never per
nested field of a struct-typed one), so nothing here can prove or refute anything reached
through it.

**`StaticDataQualityVerifier.checksForField`'s own recursion into `Field.properties` is
now connected to real tracing**, for exactly the case the `StructField(StructConstruct(...),
...)` resolution above already covers: a struct built in the same plan (`StructConstruct`,
possibly renamed through one or more pass-through `Project`/`Filter`/`Sort`/`Limit` nodes
in between — the same `.withColumn(...).select(...)` shape a real job would write). `ir.
PropertyAnalysis` gained two small public entry points to make this possible —
`definingExpr(plan, name): Option[(Expr, Plan)]`, the raw `Expr` (and the `Plan` any
`ColumnReference` inside it resolves against) that defines a named column immediately
produced by `plan`, chasing a bare `ColumnReference` rename back to *its* own defining
`Expr` the same way `resolveExprT`'s own `ColumnReference` case would; and `analyzeExpr(expr,
input, axioms): ColumnPropertyState`, the same per-`Expr` resolution `analyze`'s own results
are computed from, exposed for a caller that already has an `Expr` in hand rather than a
whole `Plan`. `checksForField` uses the first to recover a top-level field's own defining
`Expr`, then, for each `field.properties` child, wraps that `Expr` in one more
`StructField(_, child.name)` access and resolves it with the second — recursing to
arbitrary depth by carrying the wrapped `Expr` (and its resolving `Plan`) down through each
further nesting level, exactly mirroring the dotted-path recursion `checksForField` already
had. A nested field declared on the contract but absent from the actual struct construction
resolves safely to `Unknown`-with-`unsupported` (`StructField(StructConstruct(fields), name)`'s
own `fields.find` returning `None`), not a crash.

Every case `definingExpr` doesn't reach — `Aggregate`/`Window`/`Union`/`Join` outputs (none
of these has one single defining `Expr` in the same sense a `Project` column does), or a
struct-typed column read straight from an input `Read` with **no** intervening `Project` at
all (a truly trivial passthrough write) — still resolves to `None`, and every nested field
under it still gets `ColumnPropertyState(unsupported = true)`, i.e. `NotStaticallyVerifiable`
— the same verdict an unsupported construct (a UDF, a non-allowlisted function) already
gets, applying §3.7's own principle: "we don't know because we couldn't look," never a false
`NotGuaranteed` implying analysis was attempted and simply inconclusive. The struct field
itself, at the top level, is unaffected either way — its own `nullable`/`constraints` still
go through real `PropertyAnalysis`, the same as any other top-level column (e.g. a `WHERE
address IS NOT NULL` filter still proves the whole struct column non-null, and now
`struct(...)` itself proves it too).

**What's still deferred, honestly**: propagating an *input* contract's own declared
nested-field obligation (`Field.properties` on an *input* dataset's field) through to an
output struct column that passes it through unchanged, with no `StructConstruct` anywhere
in the plan at all — the genuinely harder case §3.8's own earlier draft flagged. That needs
`buildAxioms` to seed axioms for nested `ColumnRef`s (not just flat, `Read`-scoped ones,
which is all it does today), *and* `resolveExprT`'s `StructField(struct, _)` catch-all case
to consult them when `struct` resolves back to a `ColumnReference` naming an axiomed input
column, rather than discarding to `unsupported` unconditionally. Both are real, scoped, and
left for a future pass — not a signal the feature above is incomplete for the case it
actually targets (a struct the transformation's own logic constructs).

### 3.9 String length constraints

The brief's own worked example, `length(identifier) = 10`, named string length as a
category to *investigate* — investigated and, for the `length` case specifically
(pattern/regex remain out of scope; see §8/§9), now implemented: a new `Property`
kind, `Property.Length(exact: Option[Int], min: Option[Int], max: Option[Int])`,
mirroring `Range`'s own `exact`-or-`min`/`max` shape but simpler — a length is always a
non-negative integer, so there's no `gt`/`lt` exclusive-bound variant to carry. The
contract-facing side is `InterpretedFieldConstraint.Length`, decoded from a `length`
`FieldConstraint` the same "closed vocabulary, malformed → `None`" way `Equals`/`OneOf`/
`Range` already are (§5) — `exact` combined with `min`/`max` is rejected as
contradictory, and (unlike `Property.Length` itself — see below) a declared `min > max`
is rejected too, since a *human-authored* impossible range is a real mistake worth an
`Error`, not something to represent and silently propagate.

**The primary value path needed no new transfer function at all.** `ColumnPropertyState`
gained a `length: Option[Property.Length]` field, combined by `tightenWith`/`unionWith`
the same way `range` already is (`Length.tighten`/`Length.widen`, mirroring
`Range.tighten`/`Range.widen`'s own "AND narrows, Union/CASE widens" roles). Since every
existing plan/expression combinator (`Filter` narrowing, `Join` demotion, `Union`/
`Conditional` widening, a pure passthrough `ColumnReference`/`Alias`) already threads the
*whole* `ColumnPropertyState` through generically, an input contract's own declared
`length` constraint on a field — seeded as an axiom by `StaticDataQualityVerifier`'s
`fieldAxiomState`, the same way `notNull`/`range`/`oneOf`/`equalsConstant` already are —
is provable through a pure passthrough for free (Example 5's own shape, now for length).
`StaticDataQualityVerifier` gained one matching piece: `lengthVerdict`, structurally
identical to `rangeVerdict` (§3.6) — `Guaranteed` when the proven envelope is already at
least as tight as required (`p.tighten(required) == p`), `Violated` only when `p`
provably *escapes* required on either side, `NotGuaranteed`/`NotStaticallyVerifiable`
otherwise — simpler than `rangeVerdict` only in that there's no inclusive/exclusive
distinction to carry through the escape check, so (unlike `Range`'s own boundary-tie
case) an *exact* numeric tie between a proven and a required bound genuinely proves the
constraint here, not merely a `NotGuaranteed` near-miss.

A handful of expression-level transfer functions add real, deeper analysis beyond pure
passthrough: a string `Literal` is provably its own exact length; `LENGTH`/
`CHAR_LENGTH`/`CHARACTER_LENGTH` bridge a known `Length` fact on their argument to a
numeric `Range` fact on their own result (a length-envelope fact and a value-range fact
about a *different*, derived integer column, connected by one honest rule); `UPPER`/
`LOWER` preserve length exactly (case conversion never changes character count); `TRIM`/
`LTRIM`/`RTRIM` narrow to an upper bound only (trimming can only shrink a string, never
grow it, and the amount actually removed is never statically knowable). `Cast` drops
`length` entirely, the same "only `notNull` survives" MVP rule every other property
already follows. This is deliberately not the full string-function transfer-function
table §8/§9 originally scoped as a *second* MVP slice (`SUBSTRING`, `CONCAT`, `REPLACE`,
...) — six functions covering the realistic "identifier normalization" shape, plus the
axiom-passthrough path that needed no new code at all, not an attempt at completeness.

---

## 4. Where this lives: module boundaries

- **`ir`** (new file, `Property.scala` + `PropertyAnalysis.scala`, mirroring
  `Lineage.scala`'s own split into data types + traversal object): `Property`,
  `ColumnPropertyState`, `PropertyAnalysis.analyze(plan: Plan, axioms: Map[ColumnRef,
  ColumnPropertyState]): List[ColumnPropertyResult]` (one entry per output column,
  `ColumnPropertyResult` pairing a `ColumnRef` with its `ColumnPropertyState` and the
  §3.7 "touched something unsupported" flag). Zero dependency on `contract` or Spark,
  same as `Lineage` and for the same reason (`ir`'s own module doc).
- **`ir`** (new file, `PredicateFacts.scala`): the §3.4 AND/OR/NOT polarity walk,
  parameterized purely over `Expr` — no `Contract` dependency either, reusable by
  `Filter` and `Conditional` handling inside `PropertyAnalysis` itself.
- **`contract`** (extends `ContractModel.scala`): `Field.constraints`,
  `FieldConstraint`, `FieldConstraintType`, `InterpretedFieldConstraint` — the exact
  same "closed vocabulary, `interpret: Option[...]`, malformed → `None`" pattern
  `RuleType`/`InterpretedRule` already establishes. Extends `ContractParser` (a new
  `parseConstraints` alongside `parseField`) and `ContractValidator` (malformed
  `constraints` entries reported as `Error`, the same way malformed `rules` are).
- **`spark-adapter`** (new file, `StaticDataQualityVerifier.scala`, sibling to
  `SensitivityLineage`/`RuleVerifier`/`PlanRuleVerifier`): builds the axiom map from
  `contract.inputs` (§3.1), calls `ir.PropertyAnalysis.analyze` on the translated plan,
  compares each `contract.outputs` field's declared obligations against the result
  (§3.6), and produces `List[DataQualityCheckResult]`.

No new module, no parallel plan representation — every new type either sits beside an
existing, structurally identical sibling (`Lineage`/`PropertyAnalysis`,
`RuleType`/`FieldConstraintType`) or extends an existing one (`Field`,
`VerificationResult`).

---

## 5. Contract parsing and validation

```scala
// ContractModel.scala
case class FieldConstraint(constraintType: String, properties: Map[String, Any]) {
  def interpret: Option[InterpretedFieldConstraint] = constraintType match {
    case FieldConstraintType.Equals =>
      properties.get("value").map(v => InterpretedFieldConstraint.Equals(v, literalTypeOf(v)))
    case FieldConstraintType.OneOf =>
      FieldConstraint.valueSet(properties.get("values")).map(vs => InterpretedFieldConstraint.OneOf(vs, literalTypeOf(vs.head)))
    case FieldConstraintType.Range =>
      // At least one of gte/gt/lte/lt must be present; gte+gt (or lte+lt) together is malformed.
      ...
    case _ => None
  }
}
```

`ContractValidator` gains:

| Check | Severity |
|---|---|
| `constraints` entry with empty/unrecognized `constraintType` | Error |
| `equals`/`oneOf`/`range` constraint with malformed/missing properties | Error |
| `range` declaring neither `gte` nor `gt` nor `lte` nor `lt` | Error |
| `range` declaring both `gte` and `gt` (or both `lte` and `lt`) | Error (redundant/contradictory — pick one) |
| An **output** field declaring `equals`/`oneOf` with a value whose `literalType`
  doesn't match the field's own declared `type` | Warning (same "informational,
  contract-authoring-error-shaped" severity `ContractValidator` already gives
  comparable mismatches elsewhere) |

This is exactly `ContractValidator`'s existing `RuleType.All`-generic malformed-rule
check, generalized the same way `ContractValidator` already generalizes over
`RuleType`/rule shape — no new architectural pattern in the validator itself.

---

## 6. Integration with `VerificationResult` / `ContractEnforcementRule`

```scala
case class DataQualityCheckResult(
  field: String,               // e.g. "output.customer_id"
  constraint: String,          // human-readable, e.g. "NOT NULL", "amount >= 0"
  verdict: DataQualityVerdict
)

case class VerificationResult(
  status: String,
  contract: String,
  violations: List[Violation],
  fingerprints: Option[TransformationFingerprint] = None,
  dataQuality: List[DataQualityCheckResult] = Nil      // new
)
```

- **`Guaranteed` / `NotGuaranteed` / `NotStaticallyVerifiable`** are always
  reporting-only — they populate `dataQuality` and (mirroring exactly how a
  `Diagnostic` already works for an unsupported translation construct) never affect
  `VerificationResult.status`/`passed` on their own. This is the load-bearing
  design choice the brief's closing principle demands: an absence of proof, however
  labeled, must never look like a failure to a caller only checking `passed`.
- **`Violated`** produces a real, ordinary `Violation` (`ViolationType.DataQualityViolation`,
  a new constant alongside the existing structural/rule ones — same shape:
  `message`/`remediation`/`expected`/`actual`), folded into `violations` the same way
  `PlanRuleVerifier`'s violations already are, so it blocks the write by default,
  through the exact same `ContractEnforcementRule.verifyOrThrow` abort path every other
  `Violation` already uses. A proven contradiction is not softer evidence than a
  schema mismatch; it doesn't get a separate, weaker enforcement mechanism.
- **New opt-in flag**, following `VerificationOptions`' own established precedent
  (`rejectUndeclaredInputs`/`rejectUndeclaredFields`/`computeFingerprint`, all
  off-by-default, all real, non-trivial additional work): `staticDataQuality: Boolean
  = false`. When `false`, `StaticDataQualityVerifier` never runs — no `dataQuality`
  entries, no possibility of a `DataQualityViolation`. This matters more here than it
  did for `computeFingerprint`: a brand-new, narrow-MVP analysis will legitimately
  return `NotGuaranteed`/`NotStaticallyVerifiable` for a large fraction of real
  contracts at first (most transformations won't hit the whitelist), and that's
  expected, not a regression — but it should never surprise an existing caller who
  didn't ask for it.
- **External Attachability**: per CLAUDE.md's own requirement, this needs a
  `spark.invaract.staticDataQuality=true` conf key wired through
  `ContractEnforcementRule.resolveVerificationOptions` — the exact same mechanism
  `spark.invaract.computeFingerprint` already uses, not a new conf-reading pattern.
- `StructuralVerifier`'s existing `nullable`/`required` schema check is **unchanged**
  and runs regardless of this flag — the two checks are complementary, not
  alternatives, and CLAUDE.md's Documentation Policy requires the docs-site to say so
  plainly once this ships (a "Static vs. runtime schema checks" note, likely on the
  new guide this capability would need — not attempted in this design pass).

---

## 7. Worked examples, walked through the mechanism

**Example 1 (NOT NULL via filter).** `Read(source)` — no input contract axiom shown,
so `customer_id`'s `notNull = Unknown`. `Filter(_, Function("ISNOTNULL",
[ColumnReference(customer_id)]))` merges in `{ customer_id -> NotNull Proven }` via
`requiredFacts` (confirmed translation shape, §3.4). `Project`'s
`customer_id = customer_id` (a plain passthrough) inherits that. Obligation `NOT NULL`
on `output.customer_id` → §3.6 rule 1 → **`GUARANTEED`**. Removing the filter leaves
`notNull = Unknown` with no unsupported-construct flag → **`NOT_GUARANTEED`**.

**Example 2 (constant).** `'GBP' AS currency` is `Literal('GBP', string)` →
`equalsConstant = EqualsConstant('GBP', string)`. Obligation `equals GBP` → rule 2 →
**`GUARANTEED`**. `source.currency AS currency` with no matching input-contract
`equals` axiom → `equalsConstant = Unknown` → **`NOT_GUARANTEED`**.

**Example 3 (allowed values via CASE).** Two branches, both literals, `elseValue`
present → `oneOf = OneOf({ACTIVE, INACTIVE})` (union of two singleton `EqualsConstant`s
promoted to `OneOf`, per §4.5). Obligation `oneOf {ACTIVE, INACTIVE}` → subset check
passes exactly → **`GUARANTEED`**. `source.status AS status` alone, no input axiom →
**`NOT_GUARANTEED`**.

**Example 4 (numeric clamp).** `CASE WHEN amount < 0 THEN 0 ELSE amount END`. Branch 1:
`0` → `Range(gte=0, lte=0)`. Branch 2 (`ELSE`): resolved under
`requiredFacts(NOT(amount < 0))` = `{ amount -> Range(gte=0) }` (via the negation
table, `<` negates to `>=`) merged into whatever `amount`'s own state already was;
`ColumnReference(amount)` under that narrowing resolves to (at least) `Range(gte=0)`.
Union of the two branches: `Range(gte=0)` (§4.5's union rule takes the loosest bound
that covers both — `[0,0]` and `[0,+∞)` union to `[0,+∞)`). Obligation `amount >= 0` →
proven range is a subset of required → **`GUARANTEED`**, with **no dependency on any
input-contract axiom for `amount` at all** — exactly matching the brief's own framing
of this example.

**Example 5 (propagation through a pure passthrough).** Input axiom
`source.customer_id: notNull = Proven`. `SELECT customer_id FROM source` is a
`ColumnReference` resolving straight through `Project` to the `Read` axiom, unchanged.
Obligation `NOT NULL` on the output → **`GUARANTEED`**, with zero output-side
declaration needed beyond the obligation itself — the input contract did the proving.

**Example 6 (join).** Per §3.5: `customer_id`/a `customers`-sourced column through an
`INNER JOIN` keeps `customers`' own input-contract axiom (say, `notNull = Proven` on
`customers.customer_id`) → **`GUARANTEED`** for a `NOT NULL` obligation on it. The
identical query with `LEFT OUTER JOIN` instead demotes that side to `Unknown` →
**`NOT_GUARANTEED`** — same input contract, same column, different verdict purely from
`JoinType`, exactly the distinction the brief calls out as the point of the example.

**Example 7 (impossible contract).** Input axiom `source.amount: range = Range(gte=0)`.
`SELECT amount * -1 AS amount`. `Arithmetic("*", [ColumnReference(amount),
Literal(-1, integer)])` — the `*`-by-a-literal rule doesn't apply directly to a
*negative* literal (§3.3 routes that through the `NEGATE` composition instead):
negating `Range(gte=0)` (no upper bound) gives `Range(lte=0)` (no lower bound), i.e.
the proven envelope is `(-∞, 0]`. Obligation `amount >= 0` (required range `[0, +∞)`)
→ §3.6 rule 4: overlap with the complement `(-∞, 0)` is every value strictly below
zero — non-degenerate → **`VIOLATED`**, and this becomes a real `Violation`
(`DataQualityViolation`) that aborts the write when `staticDataQuality = true`,
distinct from a `NotGuaranteed` result the same way the brief insists it must be.

---

## 8. MVP scope

**In scope:**

- Property kinds: `NotNull`, `EqualsConstant`, `OneOf`, `Range` (numeric), `Length`
  (string length — `exact` or `min`/`max`, added in a follow-up pass; see §3.9. Pattern/
  regex properties remain out of scope, see below).
- Plan nodes: `Read`, `Project`, `Filter`, `Join` (nullability-demotion only, per
  §3.5), `Union`, `Sort`, `Limit`, `Write`. `Aggregate`/`Window` deliberately return
  `Unknown` for every produced column in MVP (§3.2) — not because they're unsupported
  structurally (they translate fine), but because a *sound* per-aggregate-function
  domain rule (does `SUM` of two non-negative columns stay non-negative? yes; does
  `SUM` of a `NOT NULL` column over an empty group stay `NOT NULL`? no — SQL `SUM`
  over zero rows is `NULL`) is real, non-trivial, function-specific work intentionally
  deferred rather than rushed into a first pass.
- Expression nodes: `ColumnReference`, `Literal`, `Alias`, `Cast` (nullability only),
  `Arithmetic` (the whitelist in §3.3), `Comparison`/`BooleanExpr` (narrowing role
  only), `Conditional`, `Function` (the small allowlist in §3.3). `UDF`/
  `UnknownExpression`/`AggregateCall` always `Unknown` (`AggregateCall` only reachable
  through the already-`Unknown` `Aggregate`/`Window` path).
- Property propagation from input-contract axioms through every supported node
  (Example 5), including through `Filter` narrowing and `Join` demotion.
- The four-state verdict, with `Violated` wired into real enforcement
  (`ViolationType.DataQualityViolation`) and the other three reporting-only.
- `spark.invaract.staticDataQuality` conf key (External Attachability).
- A nested (`Field.properties`) field's own declared `nullable`/`constraints`
  obligations are recognized and reported, and now resolved for real (`Guaranteed`/
  `Violated`/`NotGuaranteed`) whenever the parent struct is built by a `StructConstruct`
  in the same plan (through any number of pass-through renames) — `NotStaticallyVerifiable`
  only for the cases that genuinely can't be traced, never silently skipped nor a false
  `NotGuaranteed`. See §3.8.
- Struct member access is now a real `ir.Expr` node (`StructField`/`StructConstruct`),
  translated by `SparkPlanAdapter` from Catalyst's `GetStructField`/`CreateNamedStruct`
  — a genuine translation-correctness fix independent of data-quality verification —
  and `PropertyAnalysis` resolves the "construct, then extract, in the same plan"
  pattern to a real `Guaranteed`/`Violated`/`NotGuaranteed` verdict for both a
  top-level output field and a `Field.properties`-declared nested one. See §3.8.
- String `length` constraints (`exact`/`min`/`max`) — axiom propagation through every
  existing combinator for free, plus real transfer functions for `LENGTH`/
  `CHAR_LENGTH`/`CHARACTER_LENGTH`/`UPPER`/`LOWER`/`TRIM`/`LTRIM`/`RTRIM`. See §3.9.

**Explicitly outside the MVP** (§9 gives the reasoning, not just the list):

- Propagating an *input* contract's own nested-field (`Field.properties`) obligation
  through to an output struct column that passes it through unchanged, with no
  `StructConstruct` anywhere in the plan — needs `buildAxioms` to seed nested `ColumnRef`
  axioms (not just flat, `Read`-scoped ones) and `resolveExprT`'s `StructField` case to
  consult them. §3.8 covers exactly what *is* now in scope (a struct the transformation's
  own logic constructs) and why this harder case is deferred.
- Any struct-valued expression that isn't a `StructConstruct` built in the same
  expression — a bare reference to an existing struct-typed column (e.g. one read
  straight from a `Read`), the result of a UDF, or a `StructField` reached through one of
  those — stays `Unknown`-with-`unsupported`: no axiom representation exists for a
  struct's own internal fields.
- `Aggregate`/`Window` value-domain rules (only their *nullability-safe-Unknown*
  treatment is in MVP).
- Cast-aware preservation of `EqualsConstant`/`OneOf`/`Range` (only `NotNull`
  survives a `Cast` in MVP).
- Pattern/regex string constraints — a genuinely different property shape (a finite
  automaton or a `java.util.regex.Pattern`, not a bound), needing its own
  representation entirely; not attempted alongside `length`.
- The *remaining* string-function transfer functions (`SUBSTRING`, `CONCAT`,
  `REPLACE`, `LPAD`/`RPAD`, ...) beyond the six §3.9 covers (`LENGTH` family, `UPPER`/
  `LOWER`, `TRIM` family) — each needs its own, individually-reasoned rule the same
  way `LENGTH`'s Length→Range bridge and `TRIM`'s upper-bound-only narrowing each
  were; a further slice, not attempted here.
- Expression-derived relationship properties (`total = quantity * price`) — this is
  a *different shape* of rule (a relationship between two output columns, or an
  output column and an input column, not a value-domain fact about one column in
  isolation) and needs its own representation (likely a new `FieldConstraint` kind
  referencing another field by name) and its own analysis pass; flagged as a natural
  second slice, not attempted here.
- A `customRuleTypes`-style pluggable extension point for property kinds or transfer
  functions. The brief's own closing principle ("false claims of guarantees are worse
  than failing to prove a guarantee") argues for keeping this a closed, reviewed set
  for longer than the DML-rule family's own `CustomRuleVerifier` escape hatch needed
  to stay closed — a third party's own "trusted" transfer function is exactly the kind
  of unreviewed soundness hole this design otherwise works hard to avoid.
- Any of the explicitly-runtime categories the brief itself lists (null *rate*,
  averages, uniqueness, row counts, freshness, referential integrity against an
  external dataset, distributional/statistical properties, pattern-match
  *percentage*) — these need actual data or execution statistics by definition and
  this module must never claim otherwise.
- A `unique`-shaped input axiom for strengthening join reasoning (§3.5's own note).
- Any general-purpose SQL/expression theorem proving, arbitrary UDF semantic
  annotation, or inference from sample/historical/current data — all explicitly
  forbidden by the brief and not reconsidered here.

---

## 9. Why these specific cuts (not just what's cut)

- **`Aggregate`/`Window` deferred, not because they're hard to translate but because a
  wrong per-function rule is a *false guarantee*, the one outcome this whole design
  exists to prevent.** `Unknown` is always sound; a rushed, subtly-wrong `SUM`/`AVG`
  rule is exactly the failure mode the brief's closing principle names.
- **Cast only preserves `NotNull`, not `Range`/`OneOf`/`EqualsConstant`, in MVP.**
  Nullability-preservation is type-independent (a cast doesn't invent nulls from
  nothing, modulo the unsafe-cast caveat in §3.3's own table). Range/value-set
  preservation is type-*dependent* (a `decimal(5,2)` → `decimal(3,0)` cast can
  overflow and truncate; a `string` → `int` cast can produce a different-looking
  failure entirely) — correctly modeling that per type pair is real work, and getting
  it wrong produces a false `Guaranteed`, again the one thing to avoid above all.
- **String length landed as a real fifth `Property` kind, once the first four
  proved the pattern out** — mirroring exactly how the row-level-DML rules shipped
  three types first, and the plan-shape rules shipped a second family only once that
  pattern held up (§3.9). It turned out to share more machinery with `Range` than
  originally assumed: `Length.tighten`/`Length.widen` mirror `Range.tighten`/
  `Range.widen` directly, and `ColumnPropertyState`'s generic combining meant the
  primary (axiom-passthrough) value path needed zero new transfer-function code —
  only the handful of real string-function rules (`LENGTH` family, `UPPER`/`LOWER`,
  `TRIM` family) were genuinely new work. Pattern/regex properties remain a real
  second MVP: they don't share `Range`'s bound-based shape at all.
- **Expression-derived relationships (`total = quantity * price`) are a different
  question from every other rule in scope** — every other rule in this design is
  "does this one output column's value always lie in a set," checked via the
  `Field.constraints` obligation-per-field model. A relationship rule is "do these two
  columns' values always satisfy an equation," which needs to name a second column,
  doesn't fit `Field.constraints`'s single-field shape without a real redesign of that
  shape, and deserves its own follow-up design rather than being wedged in here.

---

## 10. Representative test cases

Mirroring the existing test-file conventions this codebase already uses (a pure-Scala
spec for the `ir`-level analysis needing no Spark session, structurally like
`RuleVerifierSpec`/`PlanRuleVerifierSpec`; a handful of real-Spark end-to-end cases in
`ContractEnforcementRuleSpec` proving the wiring actually blocks a `Violated` write).

**`PropertyAnalysisSpec` (pure, `ir`), hand-built `Plan`/`Expr` — one test class per
property kind, each covering its own `GUARANTEED`/`NOT_GUARANTEED`/`VIOLATED`/
`NOT_STATICALLY_VERIFIABLE` shape:**

- `NotNull`: no axiom + no filter → `Unknown`/`NotGuaranteed`. Axiom `Proven` + plain
  passthrough → `Guaranteed` (Example 5). No axiom + `Filter(IS NOT NULL)` →
  `Guaranteed` (Example 1). Axiom `Proven` + `LeftOuter` join demotion → back to
  `Unknown`/`NotGuaranteed` (Example 6's negative case). A `Literal(null, t)` →
  `Refuted`/`Violated`. A `UDF` wrapping an otherwise-`Proven` column →
  `Unknown`-with-unsupported-flag/`NotStaticallyVerifiable`.
- `EqualsConstant`: a bare string literal → `Guaranteed` (Example 2). A passthrough
  with no axiom → `NotGuaranteed`. Two different `Union` branches each a *different*
  constant → neither `Guaranteed` nor a false `Violated` (§4.5's union: differing
  constants collapse to `Unknown`, not silently picking one) — a dedicated test for
  exactly this, since it's an easy place to get the union rule wrong.
- `OneOf`: the `CASE WHEN` two-branch-with-ELSE case → `Guaranteed` (Example 3).
  The same `CASE` **without** an `ELSE` → `NotGuaranteed` (never `Guaranteed` — a
  dedicated regression test, since forgetting the no-ELSE rule is exactly the kind of
  mistake that produces a false guarantee). A branch value outside the required set →
  `Violated`.
- `Range`: the clamp `CASE` → `Guaranteed` regardless of any input axiom (Example 4,
  and a variant *with* a contradicting input axiom to confirm the `CASE`'s own
  narrowing wins, not the axiom). The negation-of-a-nonneg-axiom case → `Violated`
  (Example 7), plus a boundary-only-touch variant (e.g. required `> 0`, proven
  envelope's boundary lands exactly on `0` with no further overlap) asserting
  `NotGuaranteed`, not `Violated` — the conservative tie-break from §3.6 rule 4,
  worth its own explicit test given how easy it is to get backwards.
- A representative "unsupported construct" sweep: a `Function` outside the allowlist,
  an `UnknownExpression`, and a `UDF`, each on an otherwise-fully-proven path, all
  asserted to report `NotStaticallyVerifiable` specifically (not `NotGuaranteed`) —
  the §3.7 distinction, tested directly rather than left implicit.

**`StaticDataQualityVerifierSpec` (`spark-adapter`, contract-aware):**

- Axiom-building from a real parsed input contract's `nullable`/`constraints`,
  matched against a `Read`'s actual reported location (mirroring
  `SensitivityLineage`'s own location-matching tests).
- End-to-end `VerificationResult.dataQuality` shape for a contract declaring a mix of
  `Guaranteed`/`NotGuaranteed`/`NotStaticallyVerifiable` fields in one pass, confirming
  none of the three affect `status`/`passed`.

**`ContractEnforcementRuleSpec` (real Spark, a handful of cases):**

- `staticDataQuality = false` (default): a plan whose semantics are `Violated` under
  this analysis still executes normally — the flag genuinely gates everything,
  including blocking.
- `staticDataQuality = true`, Example 1's PASS shape: write executes, output file
  created, `dataQuality` in the published event shows `Guaranteed`.
- `staticDataQuality = true`, Example 7's shape: write is aborted before any data is
  written, `ex.result.violations.exists(_.violationType ==
  ViolationType.DataQualityViolation)`, and `ex.result.dataQuality` shows `Violated`
  for the specific field — the same "abort before touching anything, and say why"
  proof every other `Violation` family already gets in this file.
- `staticDataQuality = true`, a `NotGuaranteed`/`NotStaticallyVerifiable` case (e.g. a
  UDF on the relevant column): write still executes normally (only `Violated` blocks),
  confirming the reporting-only three don't accidentally abort anything.

---

## 11. Open questions to resolve before implementation

1. ~~Confirm empirically how `IS NOT NULL`/`IS NULL`/`IN (...)` translate.~~ **Resolved**:
   `IS NOT NULL`/`IS NULL` translate via the generic `Function` fallback to
   `Function("ISNOTNULL", [...])`/`Function("ISNULL", [...])` (the latter already
   covered by an existing `SparkPlanAdapterSpec` assertion; the former confirmed via a
   throwaway probe). `IN (...)` stays as its own `Function("IN", value :: literals)`
   node in the *analyzed* plan — Spark does not desugar it to an OR-of-equalities
   until the optimizer, which this translator never sees (per ADR-002, only the
   analyzed plan is translated) — so §3.4's table now includes a direct `IN` →
   `OneOf` narrowing rule, no OR-chain special case needed.
2. **Where does `DataQualityCheckResult` surface in `demo/output/report.json`/the web
   UI**, and does the harness need a demo contract exercising this? (Likely yes, per
   this repo's own "prove it against a real Spark job, not just unit tests" discipline
   — probably a new, narrowly-scoped demo contract/fixture, not a change to the real
   `invaract_output.yaml`, to avoid entangling this capability's own demonstration
   with the existing enforcement demo's unrelated assertions.)
3. **Docs-site scope**: a new guide (mirroring `enforcing-dml-rules.mdx`/
   `enforcing-transformation-rules.mdx`'s precedent) is clearly needed once this ships
   — not attempted in this design pass, flagged so it isn't forgotten at
   implementation time per CLAUDE.md's Documentation Policy.
4. **MiMa impact**: `VerificationResult` gaining a new field with a default value is
   binary-compatible for construction call-sites using named/default args, but a
   `VerificationResult(a,b,c,d)` positional call site (if any exist in this codebase
   or, worse, in a third party's compiled code) would need checking — flagged for the
   implementation pass, not resolved here.
