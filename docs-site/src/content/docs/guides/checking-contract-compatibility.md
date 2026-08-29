---
title: Check Contract Compatibility
description: Classify the difference between two versions of a contract, and catch a mis-declared version bump.
sidebar:
  order: 6
---

As a contract evolves, its `version` should reflect the actual scope of change — a
breaking removal shouldn't ship as a patch. `ContractCompatibility` compares two parsed
versions of the *same* contract (matched by `id`) and classifies the difference.

## Diff two versions

```scala
import com.example.contract.ContractCompatibility

val report = ContractCompatibility.diff(previousContract, nextContract)

report.requiredLevel // Patch | Minor | Breaking
report.isBreaking     // true iff any Breaking change is present
report.changes        // every detected change, each tagged with a level and a path
```

## What's classified, and how

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

## Verify a version bump matches its scope

```scala
val problems = ContractCompatibility.verifyVersionBump(previous, next)
// Nil if the bump is consistent with (or more conservative than) the diff;
// otherwise, human-readable messages naming the required bump level.
```

This is the check a CI pipeline would run on a pull request that modifies a contract
file — catching, for example, a breaking schema change released as a `PATCH` version
bump, before it ships.

## Where this fits

This is a `contract`-module concern, independent of Spark — it works on two parsed
`Contract` values, with no transformation or plan involved. It's the mechanism a
contract-driven CI check would call against a diff of a contract file across two commits.
