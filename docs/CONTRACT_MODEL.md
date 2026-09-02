# Contract Model

This document describes the Invaract contract model delivered in Phase 1: the
object model, parser, structural validator, and version-compatibility engine
that together represent "the minimum contract required to verify a
transformation" (see [ROADMAP.md](../ROADMAP.md), Phase 1 — Contract Model).

Code lives in the `contract/` sbt module (`com.invaract.contract` package),
independent of `plugin/` and `runner/`. It has no Spark dependency — a
contract is a plain data structure that any future engine adapter (Spark,
SQL, dbt) can be verified against.

## Relationship to ODCS

Invaract does not invent a new contract syntax. The shape mirrors the [Open
Data Contract Standard (ODCS)](https://github.com/opendatadiscovery/open-data-contracts-standard):
an identity, a version, one or more input/output datasets with physical
locations, and per-dataset schemas made of typed, nullable fields.

Phase 1 implements the subset of ODCS concepts required to *verify* a
transformation (see [MISSION.md, §4](../MISSION.md#4-what-a-contract-means)),
not the full standard. Two decisions follow from that:

1. **Unrecognized top-level keys are preserved, not rejected.** A contract
   authored with additional ODCS fields Invaract doesn't yet interpret
   (owners, SLAs, quality rules, etc.) still parses successfully — those keys
   land in `Contract.extensions` verbatim. This keeps Invaract additive to
   the ecosystem rather than a competing, incompatible format.
2. **Only fields needed for verification are strongly typed.** `id`,
   `version`, dataset `location`s, and schema `field`s are structured; the
   rest is opaque metadata Invaract carries but does not act on.

## Contract Document Shape

```yaml
id: customer_orders
version: "1.0.0"
status: active

inputs:
  - name: orders
    location: raw.orders
    format: table
    schema:
      fields:
        - name: order_id
          type: string
          required: true
          nullable: false
        - name: customer_id
          type: string
          required: true
          nullable: false
        - name: amount
          type: decimal
          required: true
          nullable: false

outputs:
  - name: customer_orders
    location: gold.customer_orders
    format: table
    saveMode: overwrite
    schema:
      fields:
        - name: customer_id
          type: string
          required: true
          nullable: false
        - name: total_orders
          type: integer
          required: true
          nullable: false
        - name: total_amount
          type: decimal
          required: true
          nullable: false

rules:
  - type: compatibility
    mode: backward

extensions:
  owner: data-platform-team
```

### Field Reference

| Key | Required | Meaning |
|---|---|---|
| `id` | yes | Stable contract identifier. |
| `version` | yes | `MAJOR.MINOR[.PATCH]`, e.g. `"1.0.0"` or `"1.0"`. |
| `status` | no | Defaults to `active`. Free-form (e.g. `active`, `deprecated`, `draft`). |
| `inputs` | no | List of datasets the transformation reads. |
| `outputs` | yes | List of datasets the transformation produces. At least one required. |
| `rules` | no | List of `{type, ...properties}`. Most types are recorded, not yet interpreted (verification is future work) — three are interpreted and enforced today, see "Interpreted rules" below. |
| `extensions` | no | Free-form map, merged with any unrecognized top-level keys. |

Each dataset (`inputs[]` / `outputs[]`) has:

| Key | Required | Meaning |
|---|---|---|
| `name` | yes | Logical name, referenced by `Contract.input(name)` / `.output(name)`. |
| `location` | yes | Physical location (table name, path, topic). |
| `format` | no | Storage/serialization format. |
| `saveMode` | no | Expected write behavior toward existing data at `location` (`append`/`overwrite`/`ignore`/`error`). Meaningful for outputs only; checked against the plan's actual write mode. |
| `schema.fields` | yes | List of fields (at least one). |

Each field has:

| Key | Required | Meaning |
|---|---|---|
| `name` | yes | Field name. |
| `type` | yes | Logical type. See known types below; unknown types produce a validation warning, not a parse failure. |
| `required` | no | Defaults to `false`. Whether the field must be present. |
| `nullable` | no | Defaults to `!required`. Whether the value may be null when present. |
| `properties` | no | Nested `Field` list, for struct/record types. |

Known types (validator warns, does not error, on anything else):
`string`, `integer`, `long`, `short`, `byte`, `double`, `float`, `decimal`,
`boolean`, `date`, `timestamp`, `binary`, `struct`, `array`, `map`.

## Object Model

```
Contract
├── id: String
├── version: ContractVersion(major, minor, patch)
├── status: String
├── inputs: List[Dataset]
├── outputs: List[Dataset]
├── rules: List[ContractRule(ruleType, properties)]
└── extensions: Map[String, Any]

Dataset
├── name: String
├── location: String
├── format: Option[String]
├── schema: Schema(fields: List[Field])
└── saveMode: Option[String]

Field
├── name: String
├── fieldType: String
├── required: Boolean
├── nullable: Boolean
└── properties: List[Field]   // non-empty => struct
```

`Contract.input(name)` / `Contract.output(name)` and `Schema.field(name)`
provide lookup by name.

## Parser

`com.invaract.contract.ContractParser` turns YAML into a `Contract`:

```scala
val contract = ContractParser.parseFile("contracts/customer_orders.yaml")
// or
val contract = ContractParser.parse(yamlString)
```

The parser is **fail-fast** on structure it must understand to build the
model: a missing `id`, an unparsable `version`, a dataset without `location`,
or a schema without `fields` each raise a `ContractParseException` with a
message identifying the offending path (e.g. `outputs[0].schema`). It is
**permissive** on everything else: unrecognized top-level keys are folded
into `extensions` rather than rejected.

Backed by [SnakeYAML](https://bitbucket.org/snakeyaml/snakeyaml/) — the only
runtime dependency of the `contract` module.

## Validator

Parsing succeeding means the document was *structurally interpretable*; it
does not mean the contract is *well-formed*. `ContractValidator` runs a
second pass over an already-parsed `Contract` and collects every issue in one
call, rather than stopping at the first:

```scala
val result = ContractValidator.validate(contract)
result.isValid    // true iff no Errors (warnings are still allowed)
result.errors     // Duplicate names, empty schemas, empty outputs, ...
result.warnings   // Unrecognized types, required+nullable both true, ...
```

| Check | Severity |
|---|---|
| Empty/missing `id` | Error |
| `id` doesn't match `^[a-zA-Z][a-zA-Z0-9_.-]*$` | Warning |
| No `outputs` declared | Error |
| Empty/missing dataset `name` or `location` | Error |
| Duplicate dataset name within `inputs` or `outputs` | Error |
| Schema with zero fields | Error |
| Duplicate field name within a schema (including nested `properties`) | Error |
| Empty field `name` or `type` | Error |
| Field `type` not in the known-types set (and not a struct) | Warning |
| Field marked both `required` and `nullable` | Warning |
| Rule with empty `type` | Error |

Validation recurses into nested struct fields (`properties`), so a warning on
a deeply nested field reports its full dotted path (e.g.
`outputs[0].schema.address.zip`).

## Version Compatibility

`ContractCompatibility` compares two versions of the *same* contract and
classifies the difference, following the same MAJOR/MINOR/PATCH semantics as
[docs/VERSIONING.md](VERSIONING.md):

```scala
val report = ContractCompatibility.diff(previousContract, nextContract)
report.requiredLevel   // Patch | Minor | Breaking
report.isBreaking       // true iff any Breaking change present
report.changes          // every detected change, each tagged with a level and path
```

| Change | Level |
|---|---|
| Dataset removed | Breaking |
| Dataset added | Minor |
| Dataset `location` changed | Breaking |
| Field removed | Breaking |
| Optional field added | Minor |
| Required field added (no default) | Breaking |
| Field `type` changed | Breaking |
| Field changed optional → required | Breaking |
| Field changed nullable → non-nullable | Breaking |
| Contract `id` changed | Breaking |

`ContractCompatibility.verifyVersionBump(previous, next)` checks that the
*declared* version bump matches the *actual* scope of change, and returns
human-readable problems if not — e.g. catching a breaking schema change that
was released as a PATCH:

```scala
val problems = ContractCompatibility.verifyVersionBump(v1, v2)
// Nil if the bump is consistent with (or more conservative than) the diff;
// otherwise a message naming the required bump level.
```

This is the mechanism a future CI check (see
[MISSION.md, §9](../MISSION.md#9-contract-driven-cicd)) would call on a pull
request that modifies a contract file.

## Fixtures

`contract/src/test/resources/fixtures/` contains worked examples exercised by
the test suite:

| Fixture | Purpose |
|---|---|
| `customer_orders_v1.yaml` | Baseline valid contract. |
| `customer_orders_v1_1_compatible.yaml` | Adds an optional output field — MINOR change. |
| `customer_orders_v2_breaking.yaml` | Removes a field and changes another's type — BREAKING change. |
| `invalid_missing_id.yaml` | Missing `id` — parser raises `ContractParseException`. |
| `invalid_no_outputs.yaml` | Parses fine, but validator errors on zero outputs. |
| `warnings_field_issues.yaml` | Parses fine, but validator reports duplicate field name, required+nullable contradiction, and an unrecognized type. |

## JSON Schema

`contract/schema/invaract-contract.schema.json` (Draft 2020-12) is a
standalone, language-agnostic description of the same document shape
`ContractParser`/`ContractValidator` accept — the actual public interface
for anyone authoring or generating an Invaract contract in a language
other than Scala (or wanting IDE validation/autocomplete while writing
one by hand). This is a genuinely different concern from
`demo/output/report.json`: the contract format is something external
authors and tooling bind to; `report.json` is an internal artifact of the
demo test harness that nothing outside this repository consumes (see
CLAUDE.md's "What's the product, and what's the test harness").

The schema is deliberately not a 1:1 mirror of every rule the Scala
implementation enforces — it sits between two layers:

- **Mirrors `ContractParser`'s hard failures** (`ContractParseException`):
  `id`/`version` presence and shape, a dataset's `name`/`location`/
  `schema`, a field's `name`/`type`, a schema's `fields` key.
- **Mirrors `ContractValidator`'s Error-level checks**, not just the bare
  parser, in two places: `outputs` is required and non-empty, and a
  schema's `fields` array must be non-empty — a document that merely
  parses but is immediately rejected by validation isn't a useful
  "valid contract" for this schema's purpose either.
- **Does not attempt** duplicate-name detection (dataset names, field
  names) or cross-field business rules (e.g. `required` and `nullable`
  both `true` — a `ContractValidator` Warning, not an Error). JSON Schema
  has no clean way to express "unique by nested key," and business-rule
  checks like this are exactly what stay engine-only, not
  structural-shape concerns.
- **Does not restrict `field.type` to an enum**, even though
  `ContractValidator.KnownTypes` lists the recognized set — an
  unrecognized type is only a Warning there, so a contract using a type
  this version of Invaract doesn't yet know about still validates
  against the schema, matching what the real parser actually accepts.

`ContractSchemaSpec` (`contract/src/test/scala/com/invaract/contract/`)
validates the schema against the same fixtures above, both ways: every
valid fixture (including `warnings_field_issues.yaml`, which has real
`ContractValidator` warnings but is still schema-conformant) must
validate cleanly, and both `invalid_*.yaml` fixtures must be rejected.
This is what keeps the schema from silently drifting out of sync with
the Scala implementation it documents as that implementation evolves —
nothing in the contract module's own runtime consults the schema file,
so there's no compiler to catch drift otherwise.

`demo/contracts/*.yaml` (the real contracts the demo harness runs
against) each carry a `# yaml-language-server: $schema=...` comment
pointing at the schema, so an editor with the
[YAML Language Server](https://github.com/redhat-developer/yaml-language-server)
extension validates and autocompletes them live while editing.

## Interpreted rules

Beyond `rules`' general "recorded but not verified" role, `ContractRule`
decodes three specific `type`s into `InterpretedRule` (both in
`ContractModel.scala`) — the first slice of ROADMAP.md's "Full semantic
DML verification" item, checked by `spark-adapter`'s `RuleVerifier`
against a real Spark row-level DML operation (`ir.RowMutation`, extracted
by `RowMutationSupport` — see docs/TRANSFORMATION_IR.md and
docs/SPARK_ADAPTER.md):

```yaml
rules:
  - type: merge_condition
    columns: [customer_id]
  - type: forbid_unconditional_delete
  - type: allowed_update_columns
    columns: [status, updated_at]
```

- **`merge_condition`** (`columns: List[String]`) — a MERGE's `ON`
  condition must include a genuine equality match (`t.col = s.col`, or
  the null-safe `<=>`) on every listed column, not merely reference it.
  Deliberately `columns`, not `on`: SnakeYAML's default (YAML 1.1)
  resolver treats the bare key
  `on` as the boolean `true` (the "Norway problem" — `on`/`off`/`yes`/`no`
  all resolve to booleans), confirmed the hard way by a real failing test
  before this was caught.
- **`forbid_unconditional_delete`** (no properties) — a DELETE (or a
  DSv2 `DELETE FROM ... WHERE`) may never omit a filtering predicate.
- **`allowed_update_columns`** (`columns: List[String]`) — an UPDATE may
  only assign to the listed columns.

`ContractRule.interpret: Option[InterpretedRule]` decodes a rule's
`properties` into one of these three shapes, or `None` for a rule type
Invaract doesn't interpret *or* a known type with malformed properties
(e.g. `merge_condition` with no `columns`) — `ContractValidator` reports
the latter as an `Error` (`"Rule type '...' has malformed or missing
properties for its shape"`), so a contract reaching enforcement with an
interpretable rule type is guaranteed well-formed. Each rule only
constrains the DML *shape* it names — a `merge_condition` rule is
silently inapplicable (not violated) to an operation that isn't a MERGE,
and likewise for the other two — see `RuleVerifier`'s class doc in
`spark-adapter` for the full reasoning, including why the merge-condition
check is still a structural approximation, not full predicate-logic
verification: it recognizes only a flat top-level `AND` of equalities,
without reasoning about `NOT`, `CASE WHEN`, or De Morgan equivalences,
and doesn't distinguish target- from source-side qualifiers.

## API compatibility

`Contract`, `Dataset`, `Schema`, `Field`, `ContractVersion`, `ContractRule`,
`RuleType`, and `InterpretedRule` (all in `ContractModel.scala`), plus
`ContractParser`, `ContractValidator`, and `ContractCompatibility`'s
public methods, are this module's binary API surface — checked by
[MiMa](https://github.com/lightbend/mima) via `sbt mimaReportBinaryIssues`,
CI-enforced on every PR. See CLAUDE.md's "API Compatibility Requirement"
for the full mechanism (why there's no Maven Central release to compare
against yet, how CI substitutes a recent prior commit instead, and what
to do when it fails). The case classes here are exactly the shape most likely to
break by accident: adding a field to `Field` or `Dataset` without putting
it last, or reordering `Contract`'s constructor parameters, breaks every
already-compiled caller even though nothing in this repository's own
build would show a compile error for it. A subtler real example found
while adding `ContractRule.interpret`: giving an existing case class its
first hand-written companion object (to hold `interpret` and a helper)
silently dropped the compiler-synthesized `extends AbstractFunction2` —
and with it, `tupled`/`curried` — that a case class with no user-written
companion gets for free. `mimaReportBinaryIssues` caught it; the fix is
declaring that same `extends AbstractFunction2[...]` explicitly on the
companion, not a `ProblemFilters` exclusion.

## What Phase 1 Does *Not* Do Yet

This is the contract **model**, not the verification **engine**. Out of
scope for this deliverable, tracked in [ROADMAP.md](../ROADMAP.md):

- Analyzing a Spark logical plan and checking it against a contract
- Interpreting `rules` beyond the three DML rule types described in
  "Interpreted rules" above (compatibility mode, quality expectations,
  and everything else `rules` can carry are still recorded only)
- Column-level lineage extraction
- A contract registry or versioned storage (Phase 3)

## Testing

```bash
cd contract
sbt test
```

38 tests across `ContractParserTest`, `ContractValidatorTest`,
`ContractCompatibilityTest`, and `ContractSchemaSpec` — parsing success and
failure paths, every validator rule, every compatibility classification, and
the JSON Schema's conformance both ways, all run against real fixture files
(not just in-memory case classes) plus targeted case-class constructions for
edge cases fixtures can't easily express (e.g. contradictory flags).

---

**Last Updated:** 2026-08-22
**Status:** Phase 1 — Contract Model, initial implementation
