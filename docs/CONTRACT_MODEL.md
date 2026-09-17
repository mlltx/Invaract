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
| `description` | no | Free-form human-readable explanation of what this dataset is/contains. Purely documentary — never checked by `RuleVerifier`/`StructuralVerifier`. An organizational policy can require every dataset to declare one (`require_dataset_description` — see "Organizational Policy" below). |
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
├── catalog: Option[CatalogRequirement]
└── description: Option[String]

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
| `description` added, changed, or removed | Not flagged (never a constraint) |

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
requirement change either way. `description` is different again: it is
purely documentary (see the field table above), never compared against a
real write at all, so `ContractCompatibility.diffDatasets` doesn't diff it
in either direction — unlike `format`/`saveMode`/`catalog`, there is no
asymmetry to apply, because there is no constraint.

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

### Custom rule types (`CustomRuleVerifier`)

The three interpreted rule types above are deliberately closed, the same
way `PolicyType`'s built-in set is (see "Custom policy types" below) —
but a contract's own DML governance vocabulary isn't limited to them.
`Contract` carries an eighth field, `customRuleTypes: Map[String, String]`,
mapping a `ContractRule.ruleType` this document uses to the fully-qualified
class name of a `spark-adapter`-defined `CustomRuleVerifier`
implementation:

```yaml
customRuleTypes:
  forbid_password_update: com.example.governance.ForbidPasswordColumnUpdateVerifier
rules:
  - type: forbid_password_update
```

Unlike `customPolicyTypes` — whose `CustomPolicyEvaluator` trait,
factory, and dispatch all live in `contract` itself, since evaluating an
organizational policy only ever needs a contract's own declared shape —
`customRuleTypes` is a `contract`-model field whose *behavior* lives
entirely in `spark-adapter` (`CustomRuleVerifier`/
`CustomRuleVerifierFactory`/`RuleVerifier` — see
docs/SPARK_ADAPTER.md's "Custom rule types"): a DML rule inherently needs
to know what a real Spark write actually *did* (`ir.RowMutation`, only
extractable from a live Spark plan), which is exactly the kind of fact
`contract` — deliberately Spark-independent — has no way to produce or
even reference the type of. This module's own role is correspondingly
narrower than `OrgPolicyValidator`'s equivalent check: `ContractValidator`
validates only `customRuleTypes`' *shape* (an entry with an empty
ruleType/class name is an Error; an entry whose ruleType collides with a
built-in `RuleType` is a Warning — dead, the built-in always wins, the
same treatment `customPolicyTypes`' identical collision gets). It
deliberately does **not** warn on a `ContractRule.ruleType` matching
neither a built-in `RuleType` nor a `customRuleTypes` entry, unlike
`OrgPolicyValidator`'s equivalent check for `PolicyType`: unlike
`OrgPolicy`'s closed, org-controlled seven-type set (where an unrecognized
type is almost always a typo), a contract's own `rules:` list routinely
carries rule types no code interprets at all by design — `compatibility`
being the standing, deliberately-inert example used throughout this
repository's own contracts — so the identical check here would be a
false positive on ordinary, correct usage, not a useful signal.
Actually *resolving* a named class (as opposed to checking its name isn't
empty) can only happen in `spark-adapter`, which is where
`ContractEnforcementRule.requireValidContract` eagerly resolves every
`customRuleTypes` entry via `CustomRuleVerifierFactory.tryResolve` — the
same "fail loudly, at validation time, before any write is checked"
treatment `OrgPolicyValidator`'s equivalent eager resolution gets.

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
  `inject: InjectedDefaults`, `exemptions: List[PolicyExemption]`,
  `customPolicyTypes: Map[String, String]` (ruleType → `CustomPolicyEvaluator`
  class name — see "Custom policy types" below). A `PolicyRule` carries `id`
  (referenced by exemptions, shown in violation messages), `ruleType`, open
  `properties: Map[String, Any]` (the same shape `ContractRule.properties`
  uses), `scope` (`Inputs`/`Outputs`/`All`), an optional `when:
  PolicyCondition` (currently just `sensitivityTag`, narrowing which
  datasets a rule applies to by `Field.sensitivityTags`), and `mode`
  (`Enforce`/`Warn`).
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
  `policyIds` entry naming a policy `id` this document doesn't declare; a
  `customPolicyTypes` entry with an empty ruleType/class name, or one
  naming a class `CustomPolicyEvaluatorFactory` can't resolve. Warnings:
  an exemption whose `reviewBy` has already passed (informational — the
  exemption simply stops applying, per "Exemptions" below); a
  `PolicyRule.ruleType` matching neither a built-in type nor a
  `customPolicyTypes` entry (it will never be evaluated); a
  `customPolicyTypes` entry whose ruleType collides with a built-in
  `PolicyType` (dead — the built-in always wins). See "Custom policy
  types" below for the latter two.
- **`OrgPolicyEvaluator`** — the pure engine. `evaluate(contract, policy,
  now)` evaluates every policy rule (skipping one an unexpired exemption
  covers for `contract.id`) and splits the resulting `PolicyViolation`s by
  `PolicyMode` into `OrgPolicyEvaluation(enforceViolations,
  warnViolations)`. Every rule — built-in or custom — is evaluated through
  the identical `CustomPolicyEvaluator` interface: `resolveEvaluator`
  checks `builtinEvaluators` (the seven built-in types, each an ordinary
  `CustomPolicyEvaluator` compiled into this module) before falling back to
  `policy.customPolicyTypes` for a `ruleType` outside that set — see
  "Custom policy types" below for the full mechanism, and its own note on
  why a built-in type always wins a naming collision.
  `applyInjectedRules(contract, policy)` merges `policy.inject.rules` into
  `contract.rules` (skipping a rule the contract already declares, by
  `ruleType`+`properties` equality) — see "Rule/option injection" below.

### Policy types (`PolicyType`/`InterpretedPolicy`)

Deliberately narrow and closed, mirroring `RuleType`/`InterpretedRule`'s
own role for contract-level rules — not a general policy-expression
language. An organization needing a policy type outside this set isn't
stuck waiting on an Invaract release for it, though — see "Custom policy
types" below, the reflective escape hatch this closed set doesn't have to
grow to cover:

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
- **`require_dataset_description`** (no properties) — a dataset must
  declare a non-blank `description` (see `Dataset`'s own doc in the
  "Contract Format" section above). The one type in this object that takes
  no properties at all: there's nothing to configure beyond "this dataset
  must have one," so unlike every other type here, `interpret` can never
  fail on malformed properties for it.
- **`require_extension_if`** (required `ifKey`/`thenKey`, optional
  `ifValue`/`thenValue`) — a conditional counterpart to `require_extension`:
  only once the contract already satisfies `ifKey` (optionally pinned to
  `ifValue`) must it *also* satisfy `thenKey` (optionally pinned to
  `thenValue`) — e.g. `ifKey: status, ifValue: deprecated, thenKey:
  sunsetDate` requires a `sunsetDate` extension only on a contract whose
  `status` extension is already `deprecated`. A contract that doesn't
  satisfy the `if` condition at all produces no violation — the rule
  simply doesn't apply to it, not merely "exempted" from it. Like
  `require_extension`, this checks the contract as a whole, once — see
  "`DatasetPolicy` vs. `ContractPolicy`" below. Both `checkRequireExtension`
  and `checkRequireExtensionIf` (`OrgPolicyEvaluator`) share one private
  `extensionSatisfies(contract, key, value)` helper for this "does
  `extensions` declare `key`[`=value`]" test, since `require_extension_if`
  needs it applied twice — once for its `if`, once for its `then`.

`PolicyRule.interpret: Option[InterpretedPolicy]` decodes `properties`
into one of these seven shapes — `None` for an unrecognized `ruleType`
*or* malformed properties for a recognized one, the identical
total/safe design `ContractRule.interpret` already documents; this is
what lets `OrgPolicyEvaluator.evaluate` run safely even against a policy
`OrgPolicyValidator` hasn't checked (though a real caller should still
validate first, so a malformed policy surfaces as a clear, named error
rather than a silent no-op).

#### `DatasetPolicy` vs. `ContractPolicy`

`InterpretedPolicy` is split into two sub-traits, reflecting the two
different granularities a policy rule can check at. This is a
*classification*, not `OrgPolicyEvaluator`'s dispatch mechanism — each
built-in type is its own `CustomPolicyEvaluator` in `builtinEvaluators`
(see "Custom policy types" below), and it's that evaluator's own body,
not a shared branch keyed off this split, that decides whether to narrow
to `scopedDatasets` or check the contract as a whole. The trait split
remains useful, though: it's what a reader (or `OrgPolicyValidator`, via
`PolicyType.ContractLevelTypes`) uses to know which shape a given
built-in type is, without re-deriving it from each evaluator's own body:

- **`DatasetPolicy`** — `require_catalog`, `require_field`,
  `field_naming_convention`, `require_format`, and
  `require_dataset_description`. Each of these types' own
  `builtinEvaluators` entry narrows to `scopedDatasets(contract, rule)`
  (`datasetsInScope(contract, rule.scope)`, further filtered by
  `rule.when`) and checks each dataset independently, producing a
  `PolicyViolation` with `dataset = Some(name)` per offending dataset.
- **`ContractPolicy`** — `require_extension` and `require_extension_if`
  (`PolicyType.ContractLevelTypes`). Each of these types' own
  `builtinEvaluators` entry checks `contract` as a whole, exactly once;
  neither ever reads `rule.scope`/`.when` at all, and their
  `PolicyViolation`s always carry `dataset = None`. `OrgPolicyValidator`
  warns — not errors, since the rule still evaluates correctly — if
  `scope`/`when` are set on either rule type anyway, since declaring
  either is silently inert rather than a mistake the evaluator itself
  can catch. This warning is keyed off `PolicyType.ContractLevelTypes`,
  not a hardcoded `ruleType` check, so a future `ContractPolicy` addition
  gets it for free.

A future contract-level check (e.g. a hypothetical `require_status`)
would join `ContractPolicy`, not `DatasetPolicy` — the split exists so
adding one doesn't force a dataset-shaped evaluation path onto a check
that was never about any one dataset.

### Custom policy types (`CustomPolicyEvaluator`)

The seven built-in types above are deliberately closed — but an
organization's own policy vocabulary isn't limited to them. `OrgPolicy`
carries a fifth field, `customPolicyTypes: Map[String, String]`, mapping
a `PolicyRule.ruleType` this document uses to the fully-qualified class
name of a `CustomPolicyEvaluator` implementation:

```scala
trait CustomPolicyEvaluator {
  def evaluate(contract: Contract, rule: PolicyRule): List[PolicyViolation]
}
```

`CustomPolicyEvaluatorFactory` resolves it reflectively — `Class.forName`,
a public no-arg constructor, one instance cached and reused across every
rule/contract that names it — the identical "class name in config, loaded
once" mechanism `spark-adapter`'s `NotificationSinkFactory` already
established for a `NotificationSink`'s `sink.class`, just relocated into
`contract` itself (plain JVM reflection, not a `spark-adapter`/Spark
dependency `contract` would otherwise have to take on). This is what
turns "add an organizational policy type" from "change Invaract's own
source, cut a release, wait for it" into "write and deploy a small jar" —
see CLAUDE.md's "External Attachability Requirement": the whole thing
composes for free with the conf-driven `spark.invaract.orgPolicy=<path>`
attachment point ("Enforcement in `spark-adapter`" below) — a platform
team points `--jars` at their own evaluator jar alongside the engine's,
with zero change to any governed job's own source.

There is nothing special about a *built-in* type's evaluation mechanism —
`OrgPolicyEvaluator.resolveEvaluator` looks `ruleType` up in
`builtinEvaluators` (a `Map[String, CustomPolicyEvaluator]` covering the
seven built-in types, each an ordinary `CustomPolicyEvaluator`, built via
the private `interpreted` helper — see that method's own doc for why —
compiled into this module) before ever consulting `customPolicyTypes`.
This is why
a `customPolicyTypes` key that collides with a built-in type is inert; the
built-in lookup always wins (`OrgPolicyValidator` warns on this, not
errors, the same "still evaluates correctly, just not the way you might
expect" treatment `ContractLevelTypes`' inert `scope`/`when` already
gets). Unlike a built-in type's own `CustomPolicyEvaluator` (which starts
from `rule.interpret`'s already-parsed `InterpretedPolicy` shape and, for
a `DatasetPolicy` type, narrows to `scopedDatasets` itself), a *custom*
implementation receives only the raw `Contract` and `PolicyRule`
(`scope`/`when`/`mode`/`properties` all included, `properties` unparsed)
and decides for itself what to check and how — or whether — to honor
`rule.scope`/`rule.when`; there is no `interpret`-equivalent parsing step
or `DatasetPolicy`/`ContractPolicy` split available to it, since neither
is part of the public `CustomPolicyEvaluator` contract. `PolicyExemption`
coverage and `PolicyMode` splitting still apply automatically to both
alike, though: both happen in `OrgPolicyEvaluator.evaluate` *before*
`resolveEvaluator` is even called, so an implementation — built-in or
custom — gets both for free without reimplementing either.

Two different failure modes get two different treatments, deliberately
asymmetric:

- **A misconfigured `customPolicyTypes` entry** (a typo'd class name, a
  class with no public no-arg constructor, a class that doesn't implement
  `CustomPolicyEvaluator`) never throws out of `OrgPolicyEvaluator` —
  `evaluateRule` stays total/safe, the same "config problem, not ours to
  crash a real job over" design every other malformed-policy case in this
  file already gets. `OrgPolicyValidator` is where this becomes a visible
  Error instead, eagerly resolving every `customPolicyTypes` entry during
  `validate()` — the same moment `OrgPolicyLintCli` and
  `ContractEnforcementRule.enforceOrgPolicy` already call it, before any
  real contract is evaluated. A `PolicyRule.ruleType` matching neither a
  built-in type nor a `customPolicyTypes` entry is a Warning (not an
  Error — a genuinely-unrecognized type still just no-ops, the same as
  before this field existed), since this is exactly the shape a forgotten
  or misspelled registration takes.
- **An exception from a custom evaluator's own `evaluate()` call**
  (a bug in the implementation itself, not a configuration problem)
  propagates, on purpose — fail-closed, the same principle behind every
  other "reject rather than silently pass" decision in this codebase. A
  broken custom check must not quietly pass every contract it was
  supposed to be checking.

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

### Policy layering

A single `OrgPolicy` document is still the whole model — nothing about its
shape changes for layering. What's new is composing *several* of them: an
ordered stack of layers, loosest/most general first (typically an org-wide
baseline) to strictest/most specific last (e.g. a business unit's, or a
team's, own overlay), so a platform team's org-wide policy and a business
unit's own stricter additions both apply to the same contract, without the
business unit's document having to duplicate or replace anything the
org-wide one already declares.

- **`OrgPolicyEvaluator.evaluateLayers(contract, layers, now)`** — the
  `List[OrgPolicy]` counterpart to `evaluate`. Each layer is evaluated
  independently, through the exact same single-document `evaluate` above,
  and the resulting `enforceViolations`/`warnViolations` are unioned into
  one `OrgPolicyEvaluation`. That independence, not a merged document, is
  the whole mechanism:
  - A layer's own `exemptions` can only ever suppress a violation produced
    by evaluating *that same layer* — `evaluate(contract, layers(i), now)`
    only ever consults `layers(i).exemptions` against `layers(i).policies`.
    Combined with `OrgPolicyValidator` already rejecting (as an Error) an
    exemption naming a policy id outside its own document's `policies`,
    this makes it structurally impossible for a business-unit overlay to
    exempt — i.e. loosen — a rule the org-wide layer (or any other layer)
    declares. Only that rule's own owning document can grant an exemption
    for it, the same "only whoever owns the policy file can" principle
    "Exemptions" above already establishes for the single-document case,
    now holding *per layer* rather than per document.
  - Each layer resolves its own `customPolicyTypes` against only its own
    map, so two layers naming the same `ruleType` for different
    `CustomPolicyEvaluator` classes never collide — there's no shared
    namespace to collide in.
  - Unioning independently-evaluated violation lists is also why
    composition is always *at least as strict* as any one layer alone:
    adding another layer's rules can only ever add violations, never
    remove ones an earlier layer already produced. There is deliberately
    no cross-layer "override" or "replace" mechanism for a rule's
    mode/properties — a layer wanting a stricter version of a check an
    earlier layer already declares (e.g. tightening `warn` to `enforce`)
    simply adds its own additional rule of that type; both rules are then
    evaluated and unioned, the same as any other pair of unrelated rules.
    A layer can only ever make the *effective* policy stricter, never
    looser.
- **`OrgPolicyEvaluator.applyInjectedRulesFromLayers(contract, layers)`** —
  `layers.foldLeft(contract)(applyInjectedRules)`. A rule any layer injects
  (base or overlay) ends up in the final `contract.rules` exactly once,
  since `applyInjectedRules`' own per-call dedup already covers a rule
  repeated across layers, not just within one.
- **`OrgPolicyValidator.validateLayers(layers, now)`** — takes
  `List[(String, OrgPolicy)]`, each layer paired with a caller-supplied
  label (typically the file path it was loaded from), and is the single
  entry point both real callers (`ContractEnforcementRule.enforceOrgPolicy`,
  `OrgPolicyLintCli`) use to validate a policy-layering stack, rather than
  each hand-rolling its own per-layer loop alongside it. Validates every
  layer individually (`validate`, each layer's issues re-pathed with its
  own label so a caller can tell which layer an issue came from), plus one
  cross-layer-only check: a policy `id` repeated in more than one layer is
  a Warning (not an Error — unlike a duplicate `id` *within* one document,
  which stays an Error). It's not unsafe (each layer still evaluates
  independently — a repeated id never causes shadowing or confused
  exemption/violation attribution), but a human reading two violations
  both attributed to id `catalog-required` — one from the org-wide layer,
  one from a business unit's — can't tell them apart by id alone. Rather
  than mechanically namespacing ids, the recommended fix is a naming
  convention: a layer prefixes its own rule ids (e.g.
  `bu-finance-catalog-required`).

There is no new document shape for an overlay — it's just another ordinary
`OrgPolicy` YAML document, parsed and validated exactly like the org-wide
base, reusing `OrgPolicyParser`/`OrgPolicyValidator` wholesale. See
"Enforcement in `spark-adapter`" below for how a job attaches a stack of
layers purely via `spark-submit --conf`, and "Linting contracts without
Spark" for the standalone lint's `--overlays` flag.

### Enforcement in `spark-adapter` — eager, "stop ASAP"

`ContractEnforcementRule.forContract` reads a `spark.invaract.orgPolicy`
Spark configuration key (the same attachability `LocationMapConfKey`/
`RejectUndeclaredFieldsConfKey` document — see CLAUDE.md's "External
Attachability Requirement") naming an `OrgPolicy` YAML document — the
org-wide base layer — plus, optionally, `spark.invaract.orgPolicyOverlays`
(`ContractEnforcementRule.OrgPolicyOverlaysConfKey`): a comma-separated,
ordered list of additional `OrgPolicy` documents layered on top of the
base for policy layering/inheritance (see "Policy layering" above). Both
are resolved once per session build (`resolveOrgPolicyLayers`), inside
`forContract`'s outer `session => {...}` closure — the same moment
`resolveContractLocations`/`resolveVerificationOptions` already run, and
deliberately *before* `forContract` returns the inner
`LogicalPlan => Unit` check function. Setting `orgPolicyOverlays` without
`orgPolicy` also set is rejected (`resolveOrgPolicyLayers` throws) rather
than silently treating the first overlay as the base — layering is
additive to a named org-wide anchor, not a substitute for one, and a
misconfigured job should lose org-wide governance loudly, not silently.

```
spark-submit \
  --conf spark.invaract.orgPolicy=/policies/org-wide.yaml \
  --conf spark.invaract.orgPolicyOverlays=/policies/bu-finance.yaml,/policies/team-payments.yaml \
  ...
```

This is a deliberate design choice, not an implementation detail: unlike
every other check in this module, an org-policy rule depends only on the
*contract's own declared shape* — never on what a specific write actually
does — so there is no reason to wait for a plan to exist before rejecting
a non-compliant contract. `enforceOrgPolicy`, when at least one layer is
configured:

1. Merges every layer's `inject.rules`/`minVerificationOptions`
   (`OrgPolicyEvaluator.applyInjectedRulesFromLayers`, `applyMinVerificationOptions`
   folded across layers) into the contract/options `verifyOrThrow` will
   use for every subsequent plan this session checks — an org-wide *or*
   overlay DML rule, or a forced `VerificationOptions` flag from either,
   applies with no further change needed there.
2. Evaluates every layer independently and unions the violations
   (`OrgPolicyEvaluator.evaluateLayers`). `Warn`-mode violations, from any
   layer, publish one informational `VerificationResult` (status
   `"PASSED"`, built directly rather than via `VerificationResult.of`,
   which would have inferred `FAILED` from a non-empty violation list —
   nothing was actually blocked) to the configured `NotificationSink`, if
   any, and never block — the mechanism for rolling a new policy out
   safely: a platform (or a business unit, for its own overlay)
   introduces a rule as `warn`, watches violations accumulate, then flips
   it to `enforce`, no code change on any governed job.
3. Any `Enforce`-mode violation, from any layer, throws
   `ContractViolationException` **immediately** — through the same
   `Violation`/`explain`/notification-sink path a structural violation
   already uses (a new `ViolationType.OrgPolicyViolation`, `explain`'s
   existing four-part format, `com.invaract.ir.UnknownPlan` standing in
   for "no real plan exists yet" the same way `requireValidContract`'s own
   rejection already does) — before `forContract` ever returns a usable
   check function to its caller. A non-compliant contract can't even
   finish installing, let alone reach a write.

A malformed policy document, in any layer — bad YAML, or an
`OrgPolicyValidator` error (duplicate policy ids, an exemption naming an
unknown policy id — the exact mechanism that keeps one layer from
exempting a different layer's rule, see "Policy layering" above) — fails
loudly via `OrgPolicyParseException`, naming the specific layer's own path,
rather than silently skipping enforcement, the same "fail loudly, not
quietly" principle `ContractParser`/`ContractValidator` already apply to a
malformed contract. Neither `spark.invaract.orgPolicy` nor
`spark.invaract.orgPolicyOverlays` configured at all means
`enforceOrgPolicy` returns `(contract, options)` completely unchanged —
today's behavior, byte for byte, for every job that doesn't opt in.

### Linting contracts without Spark (`OrgPolicyLintCli`)

Because `OrgPolicyEvaluator` never touches Spark, the exact same
evaluation a job pays for at session-build time can also run as a
standalone check with nothing but a JVM:

```
sbt "contract/runMain com.invaract.contract.cli.OrgPolicyLintCli org-policy.yaml contracts/"
sbt "contract/runMain com.invaract.contract.cli.OrgPolicyLintCli --overlays bu-finance.yaml,team-payments.yaml org-policy.yaml contracts/"
```

Recursively lints every `*.yaml`/`*.yml` contract under a directory (or a
single file) against a policy document — or, with `--overlays`, the full
layered stack a governed job would actually run under, the same
base-first, comma-separated, ordered-list convention
`spark.invaract.orgPolicyOverlays` uses, evaluated via
`OrgPolicyEvaluator.evaluateLayers` — printing an `[ OK ]`/`[WARN]`/
`[FAIL]` line per contract and exiting non-zero on any Enforce-mode
violation (in any layer), a parse failure, or an invalid policy document
(the base or any overlay) (`0` otherwise). Every policy document — base and
overlays alike — is excluded from the scan even when it lives alongside
the contracts it governs (a real bug found and fixed while writing this
CLI's tests — see `OrgPolicyLintCliTest`). Meant for a contracts
repository's own CI/pre-commit gate — including a business unit's own CI
linting against org-wide + its own overlay, with the exact argument shape
`spark-submit` would take — giving an author policy feedback at authoring
time, not only the next time a job runs.

Also reports, on every run, any exemption in any layer whose `reviewBy`
falls within the next `--warn-expiring-within-days` days (default 30,
backed by `OrgPolicyEvaluator.expiringExemptions`) — a proactive look-ahead
a platform team can act on before an exemption lapses, rather than
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

## API compatibility

`Contract`, `Dataset`, `Schema`, `Field`, `ContractVersion`, `ContractRule`,
`RuleType`, and `InterpretedRule` (all in `ContractModel.scala`), plus
`ContractParser`, `ContractValidator`, and `ContractCompatibility`'s
public methods, are this module's binary API surface — as are `OrgPolicy`
and its own model/parser/validator/evaluator classes
(`OrgPolicyModel.scala`/`OrgPolicyParser.scala`/`OrgPolicyValidator.scala`/
`OrgPolicyEvaluator.scala`), `CustomPolicyEvaluator`, and
`CustomPolicyEvaluatorFactory` — checked by
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
