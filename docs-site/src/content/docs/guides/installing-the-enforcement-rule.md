---
title: Install the Enforcement Rule
description: Wire Invariant's contract enforcement into your own Spark job.
sidebar:
  order: 2
---

import { Aside, Steps } from '@astrojs/starlight/components';

This guide wires Invariant into a Spark job of your own, the same way the example job in
this repository (`runner/DemoJobHarness`) does it. `ContractEnforcementRule` is a
`SparkSessionExtensions` check rule: it runs on the analyzed logical plan of every query
your session executes, and throws before Spark runs a query that violates the active
contract.

<Aside type="caution" title="Install it at session construction">
`SparkSessionExtensions` are configured when a `SparkSession` is built and can't be
changed afterward. Load your contract *before* building the session.
</Aside>

<Steps>

1. ### Parse your contract

   ```scala
   import com.example.contract.ContractParser

   val contract = ContractParser.parseFile("contracts/my_contract.yaml")
   ```

2. ### Install the check rule when you build the session

   ```scala
   import com.example.sparkadapter.ContractEnforcementRule
   import org.apache.spark.sql.SparkSession

   val spark = SparkSession
     .builder()
     .appName("MyJob")
     .master("local[*]")
     .withExtensions(_.injectCheckRule(ContractEnforcementRule.forContract(contract)))
     .getOrCreate()
   ```

   From this point on, every write your job attempts is checked against `contract` before
   it executes. A violation throws `ContractViolationException` — before any output is
   created — with a human-readable explanation of what the contract expected, what the
   plan contained, and how to fix it (see
   [Your First Contract](/Invariant/getting-started/first-contract/) for a real example of
   that message).

3. ### Run your job as usual

   Nothing else about how you write your transformation changes. If your job's actual
   write matches the contract, it proceeds exactly as it would without Invariant
   installed.

</Steps>

## Handling a rejection

`ContractEnforcementRule.forContract` throws `ContractViolationException` from inside
Spark's query analysis, so it surfaces as an ordinary Scala exception around whatever
triggered the write (`.write.save(...)`, `.write.parquet(...)`, and so on):

```scala
import com.example.sparkadapter.ContractViolationException
import scala.util.{Failure, Success, Try}

Try {
  outputDf.write.mode("overwrite").parquet(outputPath)
} match {
  case Success(_) => // write succeeded and was verified
  case Failure(e: ContractViolationException) =>
    // e.getMessage is the full four-part explanation: what the contract
    // expects, what the plan contains, why it violates the contract, and
    // how to correct it.
    logger.error(e.getMessage)
    sys.exit(1)
  case Failure(other) => throw other
}
```

## Reporting on a write, not just gating it

`ContractEnforcementRule` only decides pass/fail — it doesn't hand you a structured
summary of the write it just verified. If you also want the translated plan and its
column-level lineage (for a report, a dashboard, or logging), register
`SparkAdapterListener` alongside it — it observes the same writes *after* they succeed:

```scala
import com.example.sparkadapter.SparkAdapterListener

val irListener = new SparkAdapterListener
spark.listenerManager.register(irListener)

// ... your write happens here, gated by ContractEnforcementRule ...

// QueryExecutionListener callbacks run asynchronously on Spark's own
// listener thread, so poll briefly if you need the result immediately
// after write() returns.
val result = irListener.lastWrite // Option[TranslationResult]
```

See [Verification vs. Enforcement](/Invariant/concepts/verification-vs-enforcement/) for
why these are two separate mechanisms rather than one.

## What gets checked

Only a plan that translates to a recognized write is verified — everything else (reads,
`.count()`, intermediate transformations) is a silent no-op. See
[Connector Support](/Invariant/reference/connector-support/) for which write shapes are
recognized today, and
[Fail-Closed by Default](/Invariant/concepts/fail-closed/) for what happens when your job
writes via a shape Invariant doesn't recognize at all.
