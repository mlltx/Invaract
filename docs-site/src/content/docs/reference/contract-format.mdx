---
title: Contract Format
description: The complete Invariant contract document shape, field by field.
sidebar:
  order: 1
---

import { Aside } from '@astrojs/starlight/components';

The full shape `ContractParser`/`ContractValidator` accept. See
[Write a Contract](/guides/writing-a-contract/) for a guided introduction.

## Document shape

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

rules:
  - type: compatibility
    mode: backward

extensions:
  owner: data-platform-team
```

## Top-level fields

| Key | Required | Meaning |
|---|---|---|
| `id` | yes | Stable contract identifier. |
| `version` | yes | `MAJOR.MINOR[.PATCH]`, e.g. `"1.0.0"` or `"1.0"`. |
| `status` | no | Defaults to `active`. Free-form (e.g. `active`, `deprecated`, `draft`). |
| `inputs` | no | List of datasets the transformation reads. |
| `outputs` | yes | List of datasets the transformation produces. At least one required. |
| `rules` | no | List of `{type, ...properties}`. Most types are recorded, not yet interpreted — three are interpreted and enforced today, see [Enforce Row-Level DML Rules](/guides/enforcing-dml-rules/). |
| `extensions` | no | Free-form map, merged with any unrecognized top-level keys. |

## Dataset fields (`inputs[]` / `outputs[]`)

| Key | Required | Meaning |
|---|---|---|
| `name` | yes | Logical name, referenced in lookups and violation messages. |
| `location` | yes | Physical location (table name, path, topic) — see [location matching](#location-matching) below. |
| `format` | no | Storage/serialization format (e.g. `csv`, `parquet`, `delta`). |
| `saveMode` | no | Expected write behavior toward existing data (`append`/`overwrite`/`ignore`/`error`). Outputs only; checked against the plan's actual write mode when both are known. |
| `schema.fields` | yes | List of fields (at least one). |

## Field fields

| Key | Required | Meaning |
|---|---|---|
| `name` | yes | Field name. |
| `type` | yes | Logical type — see [known types](#known-types) below. An unrecognized type produces a validation warning, not a parse failure. |
| `required` | no | Defaults to `false`. Whether the field must be present. |
| `nullable` | no | Defaults to `!required`. Whether the value may be null when present. |
| `properties` | no | Nested field list, for `struct`/record types. |

### Known types

`string`, `integer`, `long`, `short`, `byte`, `double`, `float`, `decimal`, `boolean`,
`date`, `timestamp`, `binary`, `struct`, `array`, `map`. An unlisted type is a validator
warning, not a rejection — a contract using a type Invariant doesn't yet recognize still
validates and parses.

## Validator checks

Parsing succeeding means a document was structurally interpretable; it doesn't mean it's
well-formed. `ContractValidator` collects every issue in one pass:

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
| Interpretable rule type (`merge_condition`, `forbid_unconditional_delete`, `allowed_update_columns`) with malformed/missing properties | Error |

Validation recurses into nested struct fields, so a warning on a deeply nested field
reports its full dotted path (e.g. `outputs[0].schema.address.zip`).

## Location matching

A contract declares a portable, relative location (`demo/input/sample.csv`); Spark
reports an absolute `file:` URI at runtime
(`file:/home/user/.../demo/input/sample.csv`). Locations are matched by normalized
suffix, not exact equality: the `file:` scheme is stripped, and a declared location
matches if it equals, or is a path-boundary suffix of, the actual one.

## Interpreted rules

```yaml
rules:
  - type: merge_condition
    columns: [customer_id]
  - type: forbid_unconditional_delete
  - type: allowed_update_columns
    columns: [status, updated_at]
```

See [Enforce Row-Level DML Rules](/guides/enforcing-dml-rules/) for the full
semantics of each. Every other `rules` entry type (e.g. `compatibility`) is recorded on
the parsed contract but not yet interpreted or enforced.

## JSON Schema

`contract/schema/invariant-contract.schema.json` (Draft 2020-12) is a standalone,
language-agnostic description of this same shape — useful for IDE validation/autocomplete
while authoring a contract by hand, or for generating one from another language. Point
your editor at it:

```yaml
# yaml-language-server: $schema=../../contract/schema/invariant-contract.schema.json
id: my_contract
```

<Aside type="note">
The schema mirrors `ContractParser`'s hard failures and `ContractValidator`'s
*Error*-level checks, but not warning-level or cross-field business rules (like
duplicate-name detection, which JSON Schema has no clean way to express) — those stay
engine-only.
</Aside>

## What isn't part of the contract format

- **Compatibility mode, quality expectations**, and anything else `rules` can carry
  beyond the three interpreted types — recorded, not yet enforced.
- **Column-level lineage** and **governance rules** (masking, residency, purpose
  limitation) — no vocabulary exists for these in the contract format yet.

See [Data Contracts](/concepts/data-contracts/) for how this format relates to
ODCS, and [What is Invariant?](/introduction/what-is-this/) for what's verified
against it today.
