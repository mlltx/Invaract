---
title: Violation Types
description: Every violation Invaract can raise, and what each one means.
sidebar:
  order: 3
---

Every violation carries a `type`, a human-readable `message`, a `remediation` (a concrete
next step, not just a restatement of the problem), and — where relevant — the `column`,
`location`, `expected`, and `actual` values involved. These appear in
`contractVerification.violations` in a run's report (see
[View Verification Results](/guides/viewing-results/)) and in the four-part
explanation `ContractEnforcementRule` prints when it aborts a write.

`rejectUndeclaredInputs`/`rejectUndeclaredFields` below are `VerificationOptions` flags,
off by default — set explicitly in code or, to attach either one to a job without changing
its source, via `spark-submit --conf spark.invaract.rejectUndeclaredInputs=true`/
`spark.invaract.rejectUndeclaredFields=true`. See
[Fingerprint a Transformation's Business Logic](/guides/fingerprinting-transformations/#enable-it)
for both mechanisms worked through in full (it documents `computeFingerprint`, the third
flag in the same case class, the identical way).

## What each violation carries

`column`, `location`, `expected`, `actual` and `rule` mean the same thing on every violation
that sets them, so a consumer can group or route findings without special-casing each type:

- `location` — the dataset the finding is about. For a missing input or output it is the
  location the contract *declares*; for everything the plan really did (an output's
  location, format, save mode, catalog or schema, and every rule, data-quality and
  unverifiable-operation finding) it is the write's *actual* location, so a contract with
  several outputs can tell which write a finding belongs to. Input-schema and input-catalog
  findings carry the input's declared location; an organizational-policy finding carries the
  declared location of the dataset it faulted, when it names one.
- `column` — the field path, dotted for a nested field (`address.zip`).
- `expected` / `actual` — each is set when the finding has that value to report. A
  comparison sets both (declared type vs. actual type, declared format vs. actual format).
  A missing field reports only the `expected` type; an undeclared column only its `actual`
  type; a data-quality violation the declared constraint as `expected`; an unverifiable
  operation the kind of operation or command as `actual`.
- `rule` — the contract rule type (`required_group_by`, `forbid_cross_join`, ...) or the
  organizational policy id that raised the finding.

| Kind | `location` | `column` | `expected` / `actual` | `rule` |
|---|---|---|---|---|
| `MISSING_INPUT`, `MISSING_OUTPUT` | declared | — | — | — |
| `UNDECLARED_INPUT` | the read | — | — | — |
| `OUTPUT_LOCATION_MISMATCH` / `_FORMAT_` / `_SAVE_MODE_` | write | — | declared / actual | — |
| `*_CATALOG_REGISTRATION`, `*_CATALOG_MISMATCH` | dataset | — | catalog (mismatch only) | — |
| `MISSING_*_FIELD` | dataset | path | declared type / — | — |
| `UNDECLARED_*_COLUMN` | dataset | path | — / actual type | — |
| `*_FIELD_TYPE_MISMATCH`, `*_FIELD_NULLABILITY_MISMATCH` | dataset | path | declared / actual | — |
| `ROLE_CONSISTENCY_VIOLATION` | the input | — | — | — |
| `DATA_QUALITY_VIOLATION` | write | field | declared constraint / — | — |
| `RULE_*` | write | — | the rule's own columns / what the plan has | rule type |
| `RULE_UNVERIFIABLE_DML` | write | — | — / operation kind | — |
| `ORG_POLICY_VIOLATION` | dataset, when named | — | — | policy id |
| `UNVERIFIABLE_WRITE` | — | — | — / command class | — |
| `UNSUPPORTED_CONTRACT_FEATURE` | — | — | capability / `unsupported by <adapter>` | — |
| `INVALID_CONTRACT` | — | — | — | — |

## Structural violations — inputs

| Type | Meaning |
|---|---|
| `MISSING_INPUT` | A dataset the contract declares as an input was never read by the plan. A `.checkpoint()` between the read and the write does not cause this: Invaract sees through it, so an input read before a checkpoint counts as read (see [Notification sinks](/guides/notification-sinks/#what-an-event-looks-like)). It is also not reported when the plan contains a lineage boundary Invaract could not see through (for example a checkpoint created before the enforcement rule was installed) — the input is then reported as unverifiable instead, since its absence isn't proven. An unrelated checkpoint elsewhere in the job never excuses a genuinely missing input. See `unverifiableInputs` in the [notification sinks guide](/guides/notification-sinks/#what-an-event-looks-like) for that report-only alternative. When the output the write lands on declares [`derivedFrom`](/guides/mapping-outputs-to-inputs/), only *those* inputs are expected of that write. |
| `UNDECLARED_INPUT` | The plan read a dataset the contract doesn't declare as an input. Only checked when `rejectUndeclaredInputs` is enabled. The table a row-level `DELETE`/`UPDATE`/`MERGE` changes is not an input, even though the command reads it. Also raised, under the same option, for a read of an input the contract does declare but not as a source of the output being written (via that output's [`derivedFrom`](/guides/mapping-outputs-to-inputs/)). |
| `MISSING_INPUT_FIELD` | A required input field is absent from the actual input schema. |
| `UNDECLARED_INPUT_COLUMN` | The actual input schema has a column the contract doesn't declare. Only checked when `rejectUndeclaredFields` is enabled. |
| `INPUT_FIELD_TYPE_MISMATCH` | An input field's actual type doesn't match the contract's declared type — including the contents of an `array<...>`/`map<...>`/`struct<...>` type or a struct's `properties` (the violation's `column` is then a dotted path such as `address.zip`). |
| `INPUT_FIELD_NULLABILITY_MISMATCH` | The contract requires an input field to be non-null, but the actual schema permits nulls. |
| `MISSING_INPUT_CATALOG_REGISTRATION` | The contract's input declares `catalog: { required: true }`, but the actual read has no catalog registration at all. |
| `INPUT_CATALOG_MISMATCH` | The input is catalog-registered, but a declared `catalog` sub-field (`technology`/`catalogName`/`location`/`namespace`/`table`) disagrees with the actual registration. |

## Structural violations — outputs

| Type | Meaning |
|---|---|
| `MISSING_OUTPUT` | A declared output was never written by the plan. One violation per declared output the plan didn't produce a matching write for. |
| `OUTPUT_LOCATION_MISMATCH` | The write's actual location doesn't match any of the contract's declared output locations. |
| `OUTPUT_FORMAT_MISMATCH` | The write's actual format doesn't match the contract's declared format. Only checked when both are known. |
| `OUTPUT_SAVE_MODE_MISMATCH` | The write's actual save mode doesn't match the contract's declared `saveMode`. Only checked when both are known. |
| `MISSING_OUTPUT_FIELD` | A required output field is absent from the actual output schema. |
| `UNDECLARED_OUTPUT_COLUMN` | The actual output schema has a column the contract doesn't declare. Only checked when `rejectUndeclaredFields` is enabled. |
| `OUTPUT_FIELD_TYPE_MISMATCH` | An output field's actual type doesn't match the contract's declared type — including the contents of an `array<...>`/`map<...>`/`struct<...>` type or a struct's `properties` (the violation's `column` is then a dotted path such as `address.zip`). |
| `OUTPUT_FIELD_NULLABILITY_MISMATCH` | The contract requires an output field to be non-null, but the actual schema permits nulls. |
| `MISSING_OUTPUT_CATALOG_REGISTRATION` | The contract's output declares `catalog: { required: true }`, but the actual write has no catalog registration at all. |
| `OUTPUT_CATALOG_MISMATCH` | The output is catalog-registered, but a declared `catalog` sub-field (`technology`/`catalogName`/`location`/`namespace`/`table`) disagrees with the actual registration — e.g. the write went through a different Hive metastore than the contract declares. |

## DML rule violations

Produced when a contract declares one of the [row-level DML rules](/guides/enforcing-dml-rules/)
and the actual `MERGE`/`UPDATE`/`DELETE` doesn't satisfy it:

| Type | Meaning |
|---|---|
| `RULE_MERGE_CONDITION_VIOLATION` | A `MERGE`'s `ON` condition doesn't include a genuine equality match on every column a `merge_condition` rule declares. |
| `RULE_UNCONDITIONAL_DELETE` | A `DELETE` (or DSv2 `DeleteFromTable`) removes every row it reaches, with no filtering predicate, under a `forbid_unconditional_delete` rule. |
| `RULE_DISALLOWED_UPDATE_COLUMN` | A standalone `UPDATE` assigns a column outside an `allowed_update_columns` rule's declared list. |

## Transformation shape rule violations

Produced when a contract declares one of the [transformation shape
rules](/guides/enforcing-transformation-rules/) and the plan's structure doesn't satisfy
it — a separate rule family from DML rules above, checked against the plan's whole shape
rather than one extracted DML operation:

| Type | Meaning |
|---|---|
| `RULE_REQUIRED_GROUP_BY_VIOLATION` | No aggregation anywhere in the plan groups by every column a `required_group_by` rule declares. |
| `RULE_CROSS_JOIN_VIOLATION` | The plan contains a cartesian-product join (a `CROSS JOIN`, or any join with no condition at all) under a `forbid_cross_join` rule. |
| `RULE_REQUIRED_JOIN_COLUMNS_VIOLATION` | No join's condition anywhere in the plan establishes an equality match on every column a `required_join_columns` rule declares. |
| `RULE_REQUIRED_FILTER_COLUMNS_VIOLATION` | No filter anywhere in the plan references a column a `required_filter_columns` rule declares. |

## Static data-quality violations

Produced when a contract declares an output field's `nullable: false` or a
[static data-quality constraint](/guides/verifying-static-data-quality/) and the
transformation's own semantics *prove* it cannot hold — never merely that it
might fail, which is reported (not enforced) instead. Only checked when
`staticDataQuality` is enabled (off by default; see the guide linked above for
both ways to enable it, the identical mechanism `computeFingerprint` uses).

| Type | Meaning |
|---|---|
| `DATA_QUALITY_VIOLATION` | The transformation's own logic provably guarantees an output field can produce a value that violates its declared `nullable: false`/`equals`/`oneOf`/`range` constraint — e.g. a filter's negation, or an arithmetic operation, that demonstrably crosses a declared bound. |

## Role-consistency violations

Produced when a contract declares an input's [`type`](/guides/declaring-input-and-output-types/)
and the transformation's own translated plan *contradicts* it — currently, the one
high-confidence shape checked: a `CONTROL`-declared input whose data reaches a produced
output column. Only checked when `roleConsistency` is enabled (off by default; see the
guide linked above for both ways to enable it, the identical mechanism `staticDataQuality`
uses).

| Type | Meaning |
|---|---|
| `ROLE_CONSISTENCY_VIOLATION` | A `CONTROL`-declared input's data reaches a produced output column — pipeline-only/control data must not become part of the resulting business data asset it's declared to only operate/control. |

## Organizational policy violations

Produced when a [`spark.invaract.orgPolicy`-configured organizational
policy](/guides/enforcing-organizational-policy/) rejects the contract itself, independent
of any transformation — checked once, eagerly, before any plan is even analyzed, since a
policy rule depends only on the contract's own declared shape:

| Type | Meaning |
|---|---|
| `ORG_POLICY_VIOLATION` | The contract doesn't satisfy an `enforce`-mode organizational policy rule (e.g. every output must be catalog-registered). `message`/`remediation` name the specific policy rule (by `id`) and dataset involved — there's no further sub-vocabulary the way structural violations have one per check, since the policy document's own `id`/`description` already carry that specificity. |

## Fail-closed violations

Produced when Invaract genuinely can't verify a write or operation, rather than when a
verified write is structurally wrong. See
[Fail-Closed by Default](/concepts/fail-closed/) for the reasoning behind both.

| Type | Meaning |
|---|---|
| `UNVERIFIABLE_WRITE` | The plan is command-shaped and isn't on the known-safe list, but doesn't translate to a recognized write either — Invaract can't confirm it's safe, so it's rejected. |
| `RULE_UNVERIFIABLE_DML` | The plan is genuinely row-level DML of a kind the active contract declares a rule for, but Invaract couldn't extract the fact that rule needs (e.g. Iceberg's merge-on-read `UPDATE`). |
| `INVALID_CONTRACT` | The contract itself is structurally unsound (e.g. no declared outputs) — caught before any plan is checked against it. |
| `UNSUPPORTED_CONTRACT_FEATURE` | The contract relies on something the engine adapter in use declares it cannot verify (a catalog requirement, a rule, a nested type, a format, ...), so passing the write would represent an unchecked requirement as checked. `expected` is the capability id, e.g. `check.catalogRegistration`; the message gives the adapter's own reason. Remove what needs it from the contract, or use an adapter that supports it — see [Engine Capabilities](/reference/engine-capabilities/). The Spark adapter declares none of the checks a contract can ask for as unsupported, so this does not occur with it today. |

## Checkpoints

A job that calls `.checkpoint()` or `.localCheckpoint()` is verified **as if the checkpoint
weren't there**. Invaract remembers the plan each checkpoint was made from and puts it back
before any check runs, so every check that looks at what a job reads or how it transforms
data applies to the work done *upstream* of a checkpoint too:

- the input checks — `MISSING_INPUT`, `UNDECLARED_INPUT`, the input schema checks
  (`MISSING_INPUT_FIELD`, `UNDECLARED_INPUT_COLUMN`, `INPUT_FIELD_TYPE_MISMATCH`,
  `INPUT_FIELD_NULLABILITY_MISMATCH`) and the input catalog checks;
- the transformation-shape rules (`RULE_CROSS_JOIN_VIOLATION`,
  `RULE_REQUIRED_GROUP_BY_VIOLATION`, `RULE_REQUIRED_JOIN_COLUMNS_VIOLATION`,
  `RULE_REQUIRED_FILTER_COLUMNS_VIOLATION`), including rules an
  [organizational policy](/guides/enforcing-organizational-policy/) injects;
- `ROLE_CONSISTENCY_VIOLATION` and `DATA_QUALITY_VIOLATION`, and the sensitivity tags and
  [fingerprint](/guides/fingerprinting-transformations/) computed from the same lineage.

The verdict is the same one the same job gets with the checkpoint removed.

**This can change a job's result.** A checkpoint used to hide everything before it, so a job
could pass by accident — an undeclared read, a cross join, a `CONTROL` input flowing into the
output, an input with the wrong schema, all upstream of the checkpoint, were never checked. A
job could also be rejected by accident: a `required_filter_columns` or `required_group_by`
rule satisfied *before* the checkpoint used to be reported as violated. If a checkpointed
job that passed before is now rejected, the violation is real; it was there all along.

### What still can't be seen through

Where Invaract can't determine what a checkpoint was made from, it leaves the checkpoint
opaque rather than guess. Nothing is blocked because of that — a declared input that can't
be found is reported as *unverifiable* (report-only, see `unverifiableInputs` in the
[notification sinks guide](/guides/notification-sinks/#what-an-event-looks-like)) instead of
`MISSING_INPUT` — but the checks above can't run on what is upstream of it:

| Case | Why | What to do |
|---|---|---|
| A checkpoint created before the enforcement rule was installed, in another session, or from a plan Invaract no longer remembers | Invaract can only look up plans it saw being created (it keeps the most recent 256, and never remembers a plan over 5,000 nodes — a checkpoint exists to cut a lineage that long) | Install the rule before the job creates its Datasets |
| Several plans that share their output columns but read *different* datasets — typically a join with a lookup followed by a `select` of one side's columns, unchanged | The plan a checkpoint was made from can't be told apart from a plain read of that side | Give the selected columns a new name or an alias (`col("id").as("id")`) — that makes the columns distinct and the checkpoint resolvable |
| A Delta `MERGE` whose *source* is the checkpointed Dataset | The command keeps its source where Invaract doesn't look | — |
| A cached relation (`.cache()`/`.persist()`) | Not a checkpoint; a plain cache never reaches the check at all | — |

## Learn more

- [View Verification Results](/guides/viewing-results/) — where these appear in
  a run's output
- [Reference → Contract Format](/reference/contract-format/) — the validator
  checks that keep a contract from reaching `INVALID_CONTRACT` in the first place
- [Require Catalog Registration](/guides/requiring-catalog-registration/) — the full
  guide behind the four `*_CATALOG_*` types above
- [Enforce an Organizational Policy](/guides/enforcing-organizational-policy/) — the full
  guide behind `ORG_POLICY_VIOLATION`
- [Declare Input and Output Types](/guides/declaring-input-and-output-types/) — the full
  guide behind `ROLE_CONSISTENCY_VIOLATION`
