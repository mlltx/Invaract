---
title: Write a Contract
description: Author an Invariant data contract, field by field, from the real one this project ships.
sidebar:
  order: 1
---

import { Aside } from '@astrojs/starlight/components';

An Invariant contract is a YAML document describing a transformation's inputs and
outputs — shaped after the
[Open Data Contract Standard (ODCS)](https://github.com/opendatadiscovery/open-data-contracts-standard),
not a new format invented from scratch. This guide builds one up using the real contract
this repository ships, `demo/contracts/invariant_output.yaml`.

## Start with identity and version

```yaml
id: invariant_demo_output
version: "1.0.0"
status: active
```

- `id` — a stable identifier for this contract.
- `version` — `MAJOR.MINOR[.PATCH]`, as a string (`"1.0.0"` or `"1.0"`).
- `status` — free-form; defaults to `active` if omitted.

## Declare inputs

Each input is a dataset the transformation reads:

```yaml
inputs:
  - name: orders
    location: demo/input/sample.csv
    format: csv
    schema:
      fields:
        - name: id
          type: integer
          required: true
          nullable: true
        - name: value
          type: integer
          required: true
          nullable: true
```

- `name` — logical name, used for lookups and violation messages.
- `location` — the physical location Invariant matches against the real Spark plan (a
  table name or a file path). Spark reports absolute `file:` URIs at runtime; a relative
  location declared here still matches, by suffix — see
  [Contract Format](/reference/contract-format/#location-matching).
- `format` — optional, e.g. `csv`, `parquet`, `delta`.
- `schema.fields` — at least one field, each with a `name` and `type`.

## Declare outputs

At least one output is required — this is what a contract actually gates:

```yaml
outputs:
  - name: result
    location: demo/output/result.parquet
    format: parquet
    saveMode: overwrite
    schema:
      fields:
        - name: id
          type: integer
          required: true
          nullable: true
        - name: value
          type: integer
          required: true
          nullable: true
        - name: value_squared
          type: integer
          required: true
          nullable: true
```

`saveMode` (`append`/`overwrite`/`ignore`/`error`) is checked against the write's actual
save mode when both are known.

## Field semantics

| Key | Default | Meaning |
|---|---|---|
| `required` | `false` | The field must be present in the actual schema. |
| `nullable` | `!required` | The field's values may be null. |
| `properties` | — | Nested fields, for a `struct` type. |

<Aside type="caution">
Marking a field both `required: true` and `nullable: true` is legal — Invariant warns
about it (it's an unusual combination) but doesn't reject the contract.
</Aside>

## Add rules (optional)

`rules` is a list of `{type, ...properties}` entries. Most rule types are recorded for
future use but not yet enforced; three are interpreted and enforced today, for row-level
`MERGE`/`UPDATE`/`DELETE` statements:

```yaml
rules:
  - type: merge_condition
    columns: [customer_id, region]
  - type: forbid_unconditional_delete
  - type: allowed_update_columns
    columns: [status, updated_at]
```

See [Enforce Row-Level DML Rules](/guides/enforcing-dml-rules/) for what each
one checks.

## Add extensions (optional)

Any top-level key Invariant doesn't recognize — and the explicit `extensions` map — are
preserved verbatim, not rejected:

```yaml
extensions:
  owner: data-platform-team
```

This keeps a contract additive to the wider ODCS ecosystem: fields your organization
cares about (owners, SLAs, quality rules) round-trip through Invariant even though it
doesn't act on them yet.

## Validate as you write

Point your editor at the JSON Schema so you get inline validation and autocomplete (this
works with the [YAML Language Server](https://github.com/redhat-developer/yaml-language-server)
VS Code extension, already configured for this repo):

```yaml
# yaml-language-server: $schema=../../contract/schema/invariant-contract.schema.json
id: my_contract
# ...
```

Every contract under `demo/contracts/` carries this comment already.

## Full reference

For every field, its type, and the complete set of validator checks, see
[Reference → Contract Format](/reference/contract-format/).
