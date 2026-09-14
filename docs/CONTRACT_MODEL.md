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
| `catalog` | no | Expected data-catalog registration — see `CatalogRequirement` below. Unlike `saveMode`, meaningful for both inputs and outputs. Omitted entirely (the default) means no check at all. |
| `schema.fields` | yes | List of fields (at least one). |

A dataset's `catalog` block, when present:

| Key | Required | Meaning |
|---|---|---|
| `required` | yes (within a present `catalog:` block) | Whether this dataset must be registered in a catalog at all. `false` means the rest of the block is informational only — declared but not checked (`ContractValidator` warns on this combination). |
| `technology` | no | The catalog implementation, e.g. `hive`, `delta`, `iceberg`. Open-vocabulary, not a closed enum. |
| `catalogName` | no | The Spark-level catalog plugin/session-catalog name, e.g. `spark_catalog` — a local alias, not necessarily unique across an organization. |
| `location` | no | The catalog *service's* own address (e.g. a Hive metastore's `thrift://host:port` URI) — the durable, technology-specific answer to "which Hive," distinct from `catalogName`'s local alias. |
| `namespace` | no | The database/schema path within the catalog, as an ordered list (e.g. `[default]`). |
| `table` | no | The table name within `namespace`. |

Every sub-field beyond `required` is independently optional and checked
only when declared — the same "both sides known" convention `format`
already uses. A dataset can declare `format` with no `catalog` block at
all (unchanged, default behavior), `catalog: {required: false, ...}` (an
expected shape that gates nothing), or `catalog: {required: true, ...}`
(checked against the transformation's actual resolved catalog identity —
see `spark-adapter`'s `StructuralVerifier`):

```yaml
outputs:
  - name: sales
    location: /data/sales
    format: parquet
    catalog:
      required: true
      technology: hive
      catalogName: spark_catalog
      location: "thrift://metastore1.example.com:9083"
      namespace: [default]
      table: sales
    schema:
      fields: [...]
```

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
├── saveMode: Option[String]
└── catalog: Option[CatalogRequirement]

CatalogRequirement
├── required: Boolean
├── technology: Option[String]
├── catalogName: Option[String]
├── location: Option[String]
├── namespace: List[String]
└── table: Option[String]

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
| `catalog.required: false` with an identity sub-field still declared | Warning |
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
| `format` newly declared, or changed to a different value | Breaking |
| `format` declaration removed | Not flagged (loosening) |
| `saveMode` newly declared, or changed to a different value | Breaking |
| `saveMode` declaration removed | Not flagged (loosening) |
| Catalog registration newly made `required: true` | Breaking |
| An already-`required: true` catalog block's `technology`/`catalogName`/`location`/`namespace`/`table` changed | Breaking |
| Catalog block removed, or relaxed to `required: false` | Not flagged (loosening) |
| Catalog block added with `required: false` (informational only) | Not flagged |

`format`/`saveMode`/`catalog` all follow the same asymmetric philosophy the
schema checks above already use: `StructuralVerifier` only ever compares one
of these against a real write when *both* the contract and the actual write
have a value for it (see its own "unknown information" doc) — so newly
*declaring* one, or changing its value, is exactly like adding a required
field: a producer that previously passed can now fail. *Removing* a
declaration only loosens what gets checked, so — like a field changing from
required to optional — it isn't flagged. A `catalog` block's own `required:
false` case is a third state, not just "some declaration": it's accepted by
`ContractValidator` as informational only (see its own warning above) and
gates nothing during verification, so it never counts as a real
requirement change either way.

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

## Organizational Policy

Everything above is scoped to *one* contract, authored by whoever owns
that dataset's pipeline. `OrgPolicy` (`OrgPolicyModel.scala`) is a
different axis entirely: a platform-owned document, independent of any
one contract, expressing rules that apply across *every* contract in an
organization — "every output must be catalog-registered," "every dataset
must declare a `pii_reviewed` field," "every field name must be
`snake_case`." A contract's own author cannot opt out of it; only
whoever owns the policy document can (see "Exemptions" below). See
docs-site's "Enforce an Organizational Policy" guide for the user-facing
walkthrough this section's design backs.

### Model, parser, validator, evaluator

Same three-layer split as the contract model itself, all in `contract/`
(no Spark dependency — `OrgPolicyEvaluator` only ever touches the
`Contract` object model):

- **`OrgPolicy`** (`OrgPolicyModel.scala`) — `version`, `policies: List[PolicyRule]`,
  `inject: InjectedDefaults`, `exemptions: List[PolicyExemption]`. A
  `PolicyRule` carries `id` (referenced by exemptions, shown in violation
  messages), `ruleType`, open `properties: Map[String, Any]` (the same
  shape `ContractRule.properties` uses), `scope` (`Inputs`/`Outputs`/`All`),
  an optional `when: PolicyCondition` (currently just `sensitivityTag`,
  narrowing which datasets a rule applies to by `Field.sensitivityTags`),
  and `mode` (`Enforce`/`Warn`).
- **`OrgPolicyParser`** — YAML → `OrgPolicy`, the same fail-fast/permissive
  split `ContractParser` uses: strict about what it needs to interpret the
  document (`version`; a policy rule's `id`/`type`; an exemption's
  `contractId`/`policyIds`/`reason`; a well-formed `reviewBy` date),
  permissive about everything else (an unrecognized policy `type` still
  parses — see `PolicyRule.interpret` below). Deliberately self-contained
  rather than reaching into `ContractParser`'s private YAML-coercion
  helpers — two small, independently evolving parsers duplicating a few
  dozen lines of coercion logic is a smaller risk than widening visibility
  on an already-published, MiMa-covered file for it. `inject.rules`
  entries are parsed straight into ordinary `ContractRule`s (same `type` +
  properties shape a contract's own `rules:` key uses).
- **`OrgPolicyValidator`** — the org-policy counterpart to
  `ContractValidator`, reusing its exact `ValidationSeverity`/
  `ValidationIssue`/`ValidationResult` types rather than a parallel result
  shape. Errors: empty `version`; a policy rule with an empty `id`/`type`;
  a duplicate policy `id`; a known `ruleType` (`PolicyType.All`) with
  malformed properties (`interpret` returns `None` — the identical
  "known-type-but-malformed is an Error, unknown-type is silently
  recorded" split `ContractValidator` already uses for `ContractRule`);
  an exemption with an empty `contractId`/`reason`, no `policyIds`, or a
  `policyIds` entry naming a policy `id` this document doesn't declare.
  Warning: an exemption whose `reviewBy` has already passed (informational
  — the exemption simply stops applying, per "Exemptions" below).
- **`OrgPolicyEvaluator`** — the pure engine. `evaluate(contract, policy,
  now)` evaluates every policy rule (skipping one an unexpired exemption
  covers for `contract.id`) and splits the resulting `PolicyViolation`s by
  `PolicyMode` into `OrgPolicyEvaluation(enforceViolations,
  warnViolations)`. `applyInjectedRules(contract, policy)` merges
  `policy.inject.rules` into `contract.rules` (skipping a rule the
  contract already declares, by `ruleType`+`properties` equality) — see
  "Rule/option injection" below.

### Policy types (`PolicyType`/`InterpretedPolicy`)

Deliberately narrow and closed, mirroring `RuleType`/`InterpretedRule`'s
own role for contract-level rules — not a general policy-expression
language:

- **`require_catalog`** (optional `technology`) — a dataset must declare
  `catalog.required: true`, optionally pinning `technology` (e.g.
  `"hive"`) to a specific implementation.
- **`require_field`** (required `name`, optional `fieldType`) — a dataset's
  schema must declare a top-level field named `name` (`Schema.field`'s
  own lookup semantics — not recursive into nested structs), optionally
  of the declared `fieldType`, checked case-insensitively. Deliberately
  `fieldType`, not `type`: a policy rule's own kind is itself spelled
  `type:` at the same YAML mapping level, and SnakeYAML doesn't reject a
  document with two `type:` keys — it silently keeps only the last one,
  which would corrupt `ruleType` itself into whatever field type was
  named rather than raise anything. Confirmed the hard way by a real
  failing round-trip through `OrgPolicyParser.parse` before this was
  caught — every unit test covering the type pin had constructed
  `PolicyRule` directly in Scala, bypassing the parser entirely, so none
  of them could have caught it.
- **`field_naming_convention`** (required `pattern`) — every field name in
  a dataset's schema — recursing into nested struct `properties` — must
  *fully* match the regular expression `pattern` (`Matcher.matches()`,
  not `find()`: a partial match doesn't satisfy it). `interpret` returns
  `None` for a `pattern` that doesn't even compile as a regex, the same
  "malformed known type" treatment `OrgPolicyValidator` reports as an
  Error.
- **`require_extension`** (required `key`, optional `value`) — the
  contract *itself* must declare `key` in its top-level `extensions` map
  (e.g. `extensions: { owner: data-platform-team }`), optionally pinning
  it to an exact `value` (case-sensitive string equality). A key present
  but mapped to YAML `null` (`extensions: { owner: }`) is treated as
  absent — present-but-blank isn't a real declaration. Unlike the other
  types, this checks the *contract as a whole*, once, not once per
  dataset in scope — see "`DatasetPolicy` vs. `ContractPolicy`" below.
- **`require_format`** (required `formats`) — a dataset must declare
  `format` as one of `formats`, matched case-insensitively (e.g. every
  output must be `"delta"` or `"iceberg"`, never raw `"parquet"`/`"csv"`).
  `formats` accepts either a single scalar (`formats: delta`) or a YAML
  list (`formats: [delta, iceberg]`) — the one property in this file that
  needs list coercion at all, handled by `PolicyRule.parseFormats`. A
  dataset with no `format` declared at all does not satisfy this, the
  same "required but absent" treatment every other check in this section
  gives.

`PolicyRule.interpret: Option[InterpretedPolicy]` decodes `properties`
into one of these five shapes — `None` for an unrecognized `ruleType`
*or* malformed properties for a recognized one, the identical
total/safe design `ContractRule.interpret` already documents; this is
what lets `OrgPolicyEvaluator.evaluate` run safely even against a policy
`OrgPolicyValidator` hasn't checked (though a real caller should still
validate first, so a malformed policy surfaces as a clear, named error
rather than a silent no-op).

#### `DatasetPolicy` vs. `ContractPolicy`

`InterpretedPolicy` is split into two sub-traits, reflecting the two
different granularities a policy rule can check at:

- **`DatasetPolicy`** — `require_catalog`, `require_field`,
  `field_naming_convention`, and `require_format`. `OrgPolicyEvaluator`
  narrows to
  `datasetsInScope(contract, rule.scope)` (honoring `scope`/`when`) and
  checks each dataset independently, producing a `PolicyViolation` with
  `dataset = Some(name)` per offending dataset.
- **`ContractPolicy`** — `require_extension`. Checked exactly once
  against `contract` as a whole; `PolicyRule.scope`/`.when` have no
  effect on it at all (`OrgPolicyEvaluator` never even inspects them for
  this `ruleType`), and its `PolicyViolation` always carries
  `dataset = None`. `OrgPolicyValidator` warns — not errors, since the
  rule still evaluates correctly — if `scope`/`when` are set on a
  `require_extension` rule anyway, since declaring either is silently
  inert rather than a mistake the evaluator itself can catch.

A future contract-level check (e.g. a hypothetical `require_status`)
would join `ContractPolicy`, not `DatasetPolicy` — the split exists so
adding one doesn't force a dataset-shaped evaluation path onto a check
that was never about any one dataset.

### Rule/option injection

Beyond the policy types above (which check the contract *document's
declared shape*), `OrgPolicy.inject` lets a policy contribute to what's
already checked, reusing the existing engine wholesale instead of needing
a second evaluator for behavioral concerns:

- **`inject.rules: List[ContractRule]`** — merged into a governed
  contract's own `rules` before `RuleVerifier` runs (`applyInjectedRules`
  above). An org-wide `forbid_unconditional_delete` no contract author has
  to remember to declare themselves.
- **`inject.minVerificationOptions: Map[String, Boolean]`** — floors ORed
  onto a job's `VerificationOptions` by `spark-adapter`'s
  `ContractEnforcementRule.applyMinVerificationOptions` (e.g.
  `"rejectUndeclaredFields" -> true`) — a flag a job's own code left
  `false` can still be forced `true` by policy; never the reverse. A
  plain `Map[String, Boolean]`, not a typed `VerificationOptions`, since
  that type is `spark-adapter`-specific and this module has no Spark
  dependency to spend on it — which also means this module can't itself
  validate a key against the real flag set. `spark-adapter`'s
  `ContractEnforcementRule.requireKnownMinVerificationOptionKeys` does
  that check instead, right where the three real flag names
  (`rejectUndeclaredInputs`/`rejectUndeclaredFields`/`computeFingerprint`)
  are already known, throwing `OrgPolicyParseException` on an
  unrecognized key rather than silently no-op'ing a typo a platform team
  would otherwise believe was actually enforced.

### Exemptions

`PolicyExemption(contractId, policyIds, reason, reviewBy: Option[LocalDate])`
lives in the *policy* document, not the contract — deliberately: a
contract's own author cannot exempt their own contract from an org-wide
rule (that would defeat the rule's purpose), only whoever owns the policy
file can. `reviewBy` is optional expiry, not a permanent bypass: an
exemption with no `reviewBy` never expires; one whose `reviewBy` has
passed simply stops applying (`PolicyExemption.covers`'s
`reviewBy.forall(!now.isAfter(_))` — `true` for no expiry at all, or for
`now` on-or-before it) and the violation it was suppressing re-surfaces,
rather than remaining a silent, forever bypass. `OrgPolicyValidator`
warns (does not error) once a `reviewBy` has already passed, so an
expired exemption stays visible without being fatal on its own.

### Enforcement in `spark-adapter` — eager, "stop ASAP"

`ContractEnforcementRule.forContract` reads a `spark.invaract.orgPolicy`
Spark configuration key (the same attachability `LocationMapConfKey`/
`RejectUndeclaredFieldsConfKey` document — see CLAUDE.md's "External
Attachability Requirement") naming an `OrgPolicy` YAML document,
resolved once per session build, inside `forContract`'s outer
`session => {...}` closure — the same moment `resolveContractLocations`/
`resolveVerificationOptions` already run, and deliberately *before*
`forContract` returns the inner `LogicalPlan => Unit` check function.

This is a deliberate design choice, not an implementation detail: unlike
every other check in this module, an org-policy rule depends only on the
*contract's own declared shape* — never on what a specific write actually
does — so there is no reason to wait for a plan to exist before rejecting
a non-compliant contract. `enforceOrgPolicy`:

1. Merges `policy.inject.rules`/`minVerificationOptions` into the
   contract/options `verifyOrThrow` will use for every subsequent plan
   this session checks.
2. Evaluates every policy rule. `Warn`-mode violations publish an
   informational `VerificationResult` (status `"PASSED"`, built directly
   rather than via `VerificationResult.of`, which would have inferred
   `FAILED` from a non-empty violation list — nothing was actually
   blocked) to the configured `NotificationSink`, if any, and never
   block — the mechanism for rolling a new org-wide policy out safely: a
   platform introduces a rule as `warn`, watches violations accumulate,
   then flips it to `enforce`, no code change on any governed job.
3. Any `Enforce`-mode violation throws `ContractViolationException`
   **immediately** — through the same `Violation`/`explain`/
   notification-sink path a structural violation already uses (a new
   `ViolationType.OrgPolicyViolation`, `explain`'s existing four-part
   format, `com.invaract.ir.UnknownPlan` standing in for "no real plan
   exists yet" the same way `requireValidContract`'s own rejection
   already does) — before `forContract` ever returns a usable check
   function to its caller. A non-compliant contract can't even finish
   installing, let alone reach a write.

A malformed policy document — bad YAML, or an `OrgPolicyValidator` error
(duplicate policy ids, an exemption naming an unknown policy) — fails
loudly via `OrgPolicyParseException` rather than silently skipping
enforcement, the same "fail loudly, not quietly" principle
`ContractParser`/`ContractValidator` already apply to a malformed
contract. No `spark.invaract.orgPolicy` conf key at all means
`enforceOrgPolicy` returns `(contract, options)` completely unchanged —
today's behavior, byte for byte, for every job that doesn't opt in.

### Linting contracts without Spark (`OrgPolicyLintCli`)

Because `OrgPolicyEvaluator` never touches Spark, the exact same
evaluation a job pays for at session-build time can also run as a
standalone check with nothing but a JVM:

```
sbt "contract/runMain com.invaract.contract.cli.OrgPolicyLintCli org-policy.yaml contracts/"
```

Recursively lints every `*.yaml`/`*.yml` contract under a directory (or a
single file) against a policy document, printing an `[ OK ]`/`[WARN]`/
`[FAIL]` line per contract and exiting non-zero on any Enforce-mode
violation, a parse failure, or an invalid policy document (`0` otherwise).
The policy document itself is excluded from the scan even when it lives
alongside the contracts it governs (a real bug found and fixed while
writing this CLI's tests — see `OrgPolicyLintCliTest`). Meant for a
contracts repository's own CI/pre-commit gate: an author gets policy
feedback at authoring time, not only the next time a job runs.

Also reports, on every run, any exemption whose `reviewBy` falls within
the next `--warn-expiring-within-days` days (default 30, backed by
`OrgPolicyEvaluator.expiringExemptions`) — a proactive look-ahead a
platform team can act on before an exemption lapses, rather than
`OrgPolicyValidator`'s Warning, which only fires once it already has.
Never affects the exit code: the exemption named is still fully active.

### JSON Schema and fixtures

`contract/schema/invaract-org-policy.schema.json` (Draft 2020-12) is the
org-policy document's own standalone schema, the same role
`invaract-contract.schema.json` plays for contracts — validated against
real fixtures both ways (`OrgPolicySchemaSpec`) so it can't silently
drift from `OrgPolicyParser`/`OrgPolicyValidator`. Fixtures live under
`contract/src/test/resources/fixtures/org-policy/`.

### What this does *not* do (yet)

Deliberately out of scope for this iteration, named rather than silently
absent:

- **Fingerprint-based drift control** — a policy type comparing a write's
  semantic fingerprint (`fingerprint/`) against a previously-approved
  value, blocking on undisclosed logic changes even when the schema
  didn't change. `fingerprint/` currently has no consumer that compares
  against a stored prior value at all; this is real, substantial future
  work, not a narrow gap.
- **Cross-contract policies** — a rule spanning *multiple* contracts at
  once (e.g. "every input must be some other contract's declared
  output," "no two contracts may target the same physical location")
  needs a contract registry to know about every contract in an
  organization at once, which ROADMAP.md already tracks as unbuilt Phase
  3 work.
- **Policy layering/inheritance** — a stricter business-unit policy
  composing with a looser org-wide one. `OrgPolicy` is deliberately flat/
  single-document for now; revisit once a real need for layering
  appears.

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
