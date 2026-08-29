---
title: Data Contracts
description: What an Invariant contract is, and its relationship to ODCS.
sidebar:
  order: 1
---

A data contract, in Invariant, is a plain YAML document describing what a transformation
is supposed to produce: an identity, a version, one or more input/output datasets with
physical locations, and per-dataset schemas made of typed, nullable fields.

## Relationship to ODCS

Invariant doesn't invent a new contract syntax. The shape mirrors the
[Open Data Contract Standard (ODCS)](https://github.com/opendatadiscovery/open-data-contracts-standard).
Two decisions follow from building on an existing standard rather than a bespoke one:

1. **Unrecognized top-level keys are preserved, not rejected.** A contract authored with
   additional ODCS fields Invariant doesn't yet interpret (owners, SLAs, quality rules,
   and so on) still parses successfully — those keys land in the contract's `extensions`
   verbatim. Invariant is additive to the ecosystem, not a competing, incompatible
   format.
2. **Only the fields needed for verification are strongly typed.** `id`, `version`,
   dataset locations, and schema fields are structured; the rest is opaque metadata
   Invariant carries but does not act on.

## Parsing is not validation

A contract can be *structurally interpretable* (it parses) without being *well-formed*.
These are two separate passes:

- **Parsing** (`ContractParser`) is fail-fast on structure it must understand to build
  the model at all — a missing `id`, an unparsable `version`, a dataset without a
  `location`. It's permissive on everything else.
- **Validation** (`ContractValidator`) runs a second pass over an already-parsed
  contract and collects every issue in one call, distinguishing **errors** (duplicate
  names, an empty schema, zero declared outputs) from **warnings** (an unrecognized field
  type, a field marked both required and nullable).

A contract that fails validation never reaches enforcement — see
[Fail-Closed by Default](/Invariant/concepts/fail-closed/).

## A contract is not the verification engine

The contract is a plain data structure — parsing and validating one has no dependency on
Spark at all. **Verification** — checking a contract against a real transformation's
actual plan — is a separate concern, handled by translating that plan into the
[transformation IR](/Invariant/concepts/transformation-ir/) and comparing the two. See
[What is Invariant?](/Invariant/introduction/what-is-this/) for how the pieces fit
together end to end.

## Learn more

- [Write a Contract](/Invariant/guides/writing-a-contract/) — a hands-on walkthrough of
  the format
- [Reference → Contract Format](/Invariant/reference/contract-format/) — every field,
  fully specified
- [Check Contract Compatibility](/Invariant/guides/checking-contract-compatibility/) —
  classifying changes between two versions of the same contract
