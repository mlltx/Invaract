---
title: FAQ
description: Frequently asked questions about Invariant.
sidebar:
  order: 2
---

### Is there a published library I can add as a dependency?

Not yet. Invariant is early-stage, and its modules (`contract`, `ir`, `spark-adapter`)
have no Maven Central release. See [Installation](/Invariant/getting-started/installation/)
for how to build and use it from source today.

### Does Invariant work with engines other than Spark?

The transformation IR itself (`ir/`) is engine-independent by design, but the only front
end that exists today translates Spark's Catalyst logical plans
(`spark-adapter/`). See [The Transformation IR](/Invariant/concepts/transformation-ir/).

### Does Invariant verify that my transformation's logic is *correct*, not just its shape?

No — today's checks are structural (does the output exist, at the right location, with
the right schema, format, and save mode; do declared DML rules hold), not semantic. See
[What is Invariant?](/Invariant/introduction/what-is-this/#what-it-verifies-today).

### What happens if my job writes to a connector or in a shape Invariant doesn't recognize?

The write is rejected, not silently allowed through — see
[Fail-Closed by Default](/Invariant/concepts/fail-closed/). Check
[Connector Support](/Invariant/reference/connector-support/) for what's recognized today.

### Can I use my existing ODCS contracts as-is?

Invariant's format mirrors ODCS and preserves any fields it doesn't recognize rather than
rejecting them (see [Data Contracts](/Invariant/concepts/data-contracts/)), but it only
*interprets* a subset of ODCS today — schema/location/format/saveMode, and three DML rule
types. Fields describing anything else (SLAs, quality rules, governance policies) will
parse and round-trip, but won't be enforced yet.

### Does a passing contract mean my job's output is definitely correct?

It means the job's actual write matches what the contract structurally declares —
location, schema, format, save mode, and any declared DML rules. It doesn't verify
business logic (that an aggregation computed the right value, for instance). Treat it as
"the shape is what was promised," not "the values are right."

### Where do I report a bug or request a connector?

Open an issue on [GitHub](https://github.com/mlltx/Invariant/issues).
