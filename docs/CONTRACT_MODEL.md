# Contract Model

This document describes the Invariant contract model delivered in Phase 1: the
object model, parser, structural validator, and version-compatibility engine
that together represent "the minimum contract required to verify a
transformation" (see [ROADMAP.md](../ROADMAP.md), Phase 1 — Contract Model).

Code lives in the `contract/` sbt module (`com.example.contract` package),
independent of `plugin/` and `runner/`. It has no Spark dependency — a
contract is a plain data structure that any future engine adapter (Spark,
SQL, dbt) can be verified against.

## Relationship to ODCS

Invariant does not invent a new contract syntax. The shape mirrors the [Open
Data Contract Standard (ODCS)](https://github.com/opendatadiscovery/open-data-contracts-standard):
an identity, a version, one or more input/output datasets with physical
locations, and per-dataset schemas made of typed, nullable fields.

Phase 1 implements the subset of ODCS concepts required to *verify* a
transformation (see [MISSION.md, §4](../MISSION.md#4-what-a-contract-means)),
not the full standard. Two decisions follow from that:

1. **Unrecognized top-level keys are preserved, not rejected.** A contract
   authored with additional ODCS fields Invariant doesn't yet interpret
   (owners, SLAs, quality rules, etc.) still parses successfully — those keys
   land in `Contract.extensions` verbatim. This keeps Invariant additive to
   the ecosystem rather than a competing, incompatible format.
2. **Only fields needed for verification are strongly typed.** `id`,
   `version`, dataset `location`s, and schema `field`s are structured; the
   rest is opaque metadata Invariant carries but does not act on.

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
| `rules` | no | List of `{type, ...properties}`. Recorded, not yet interpreted (verification is future work). |
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

`com.example.contract.ContractParser` turns YAML into a `Contract`:

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

`contract/schema/invariant-contract.schema.json` (Draft 2020-12) is a
standalone, language-agnostic description of the same document shape
`ContractParser`/`ContractValidator` accept — the actual public interface
for anyone authoring or generating an Invariant contract in a language
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
  this version of Invariant doesn't yet know about still validates
  against the schema, matching what the real parser actually accepts.

`ContractSchemaSpec` (`contract/src/test/scala/com/example/contract/`)
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

## What Phase 1 Does *Not* Do Yet

This is the contract **model**, not the verification **engine**. Out of
scope for this deliverable, tracked in [ROADMAP.md](../ROADMAP.md):

- Analyzing a Spark logical plan and checking it against a contract
- Interpreting `rules` (compatibility mode, quality expectations) beyond
  recording them
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
