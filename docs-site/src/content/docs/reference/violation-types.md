---
title: Violation Types
description: Every violation Invariant can raise, and what each one means.
sidebar:
  order: 3
---

Every violation carries a `type`, a human-readable `message`, a `remediation` (a concrete
next step, not just a restatement of the problem), and — where relevant — the `column`,
`location`, `expected`, and `actual` values involved. These appear in
`contractVerification.violations` in a run's report (see
[View Verification Results](/guides/viewing-results/)) and in the four-part
explanation `ContractEnforcementRule` prints when it aborts a write.

## Structural violations — inputs

| Type | Meaning |
|---|---|
| `MISSING_INPUT` | A dataset the contract declares as an input was never read by the plan. |
| `UNDECLARED_INPUT` | The plan read a dataset the contract doesn't declare as an input. Only checked when `rejectUndeclaredInputs` is enabled. |
| `MISSING_INPUT_FIELD` | A required input field is absent from the actual input schema. |
| `UNDECLARED_INPUT_COLUMN` | The actual input schema has a column the contract doesn't declare. Only checked when `rejectUndeclaredFields` is enabled. |
| `INPUT_FIELD_TYPE_MISMATCH` | An input field's actual type doesn't match the contract's declared type. |
| `INPUT_FIELD_NULLABILITY_MISMATCH` | The contract requires an input field to be non-null, but the actual schema permits nulls. |

## Structural violations — outputs

| Type | Meaning |
|---|---|
| `MISSING_OUTPUT` | The contract's declared output was never written by the plan. |
| `OUTPUT_LOCATION_MISMATCH` | The write's actual location doesn't match the contract's declared location. |
| `OUTPUT_FORMAT_MISMATCH` | The write's actual format doesn't match the contract's declared format. Only checked when both are known. |
| `OUTPUT_SAVE_MODE_MISMATCH` | The write's actual save mode doesn't match the contract's declared `saveMode`. Only checked when both are known. |
| `MISSING_OUTPUT_FIELD` | A required output field is absent from the actual output schema. |
| `UNDECLARED_OUTPUT_COLUMN` | The actual output schema has a column the contract doesn't declare. Only checked when `rejectUndeclaredFields` is enabled. |
| `OUTPUT_FIELD_TYPE_MISMATCH` | An output field's actual type doesn't match the contract's declared type. |
| `OUTPUT_FIELD_NULLABILITY_MISMATCH` | The contract requires an output field to be non-null, but the actual schema permits nulls. |

## DML rule violations

Produced when a contract declares one of the [row-level DML rules](/guides/enforcing-dml-rules/)
and the actual `MERGE`/`UPDATE`/`DELETE` doesn't satisfy it:

| Type | Meaning |
|---|---|
| `RULE_MERGE_CONDITION_VIOLATION` | A `MERGE`'s `ON` condition doesn't reference every column a `merge_condition` rule declares. |
| `RULE_UNCONDITIONAL_DELETE` | A `DELETE` (or DSv2 `DeleteFromTable`) removes every row it reaches, with no filtering predicate, under a `forbid_unconditional_delete` rule. |
| `RULE_DISALLOWED_UPDATE_COLUMN` | A standalone `UPDATE` assigns a column outside an `allowed_update_columns` rule's declared list. |

## Fail-closed violations

Produced when Invariant genuinely can't verify a write or operation, rather than when a
verified write is structurally wrong. See
[Fail-Closed by Default](/concepts/fail-closed/) for the reasoning behind both.

| Type | Meaning |
|---|---|
| `UNVERIFIABLE_WRITE` | The plan is command-shaped and isn't on the known-safe list, but doesn't translate to a recognized write either — Invariant can't confirm it's safe, so it's rejected. |
| `RULE_UNVERIFIABLE_DML` | The plan is genuinely row-level DML of a kind the active contract declares a rule for, but Invariant couldn't extract the fact that rule needs (e.g. Iceberg's merge-on-read `UPDATE`). |
| `INVALID_CONTRACT` | The contract itself is structurally unsound (e.g. no declared outputs) — caught before any plan is checked against it. |

## Learn more

- [View Verification Results](/guides/viewing-results/) — where these appear in
  a run's output
- [Reference → Contract Format](/reference/contract-format/) — the validator
  checks that keep a contract from reaching `INVALID_CONTRACT` in the first place
