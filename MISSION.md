# Invaract — Product Vision

## 1. Vision

Invaract is an open framework for verifying that data transformations conform to machine-readable data contracts.

The long-term goal is to make data transformations statically verifiable in the same way that software compilers and type systems verify programs against defined interfaces.

A data contract should not merely describe a dataset after it exists. It should define the properties that a transformation is required to satisfy before that transformation executes.

Invaract will turn this:

```
Data contract + transformation
            ↓
       "we hope it matches"
```

into this:

```
Data contract + transformation
            ↓
          Invaract
            ↓
    VERIFIED / REJECTED
```

A verified transformation can then produce trusted, version-controlled metadata and lineage that can be used by data platforms, governance systems, CI/CD pipelines and AI agents.

---

## 2. The Problem

Modern data platforms commonly separate several concerns:

* Data contracts describe what a dataset is supposed to look like.
* Lineage systems describe what happened during execution.
* Data quality systems check data after or during execution.
* Catalogues describe datasets and their relationships.
* CI/CD systems validate source code and infrastructure.
* Governance systems apply policies to data.
* AI systems try to infer relationships between all of the above.

The missing primitive is verification between the intended contract and the actual transformation.

A pipeline can claim:

> This model produces customer_orders.

A lineage system can later observe:

> This job read A and B and wrote C.

But neither necessarily establishes:

> The implementation of this transformation has been proven to conform to the contract for customer_orders.

Invaract exists to provide that missing layer.

---

## 3. Core Concept

Invaract treats a data transformation as an implementation of a contract.

The conceptual model is:

```
                 DATA CONTRACT
                       │
              "What must be true?"
                       │
                       ▼
                TRANSFORMATION
                       │
             "What will happen?"
                       │
                       ▼
                  INVARACT
                  VERIFIER
                       │
              ┌────────┴────────┐
              ▼                 ▼
           VERIFIED          REJECTED
              │
              ▼
       VERIFIED METADATA
              │
       ┌──────┼─────────┐
       ▼      ▼         ▼
    Lineage   CI       AI
```

The contract defines the invariants.

The execution technology provides the transformation plan.

Invaract determines whether the plan satisfies those invariants.

---

## 4. What a Contract Means

Invaract will build on established open standards rather than creating an isolated contract ecosystem.

Open Data Contract Standard (ODCS) should be the primary foundation for the contract representation wherever its concepts are applicable.

Invaract will add only the concepts required to express executable transformation guarantees that are not adequately represented by the underlying standard.

A contract may describe:

* Inputs
* Outputs
* Physical locations
* Schemas
* Data types
* Required and optional fields
* Compatibility requirements
* Transformation expectations
* Allowed dependencies
* Column-level constraints
* Governance policies
* Ownership and metadata
* Version
* Compatibility semantics

The important distinction is:

> A contract is not simply a schema.
>
> A schema describes structure.
>
> A contract describes what an implementation is allowed and required to do.

---

## 5. Verified Lineage

Invaract will make a distinction between different kinds of lineage.

**Observed Lineage**

Produced from an executed workload.

> "This job read A and wrote B."

**Declared Lineage**

Described by metadata or a contract.

> "This product is expected to be derived from A."

**Verified Lineage**

Derived from the transformation plan and proven to conform to the contract.

> "This implementation has been analysed and proven to produce B from A according to the contract."

Verified lineage is one of the project's most important outputs.

Where possible, Invaract should interoperate with OpenLineage rather than replacing it.

OpenLineage remains the ecosystem interoperability layer for lineage events.

Invaract provides the verification layer that can establish confidence in those relationships.

---

## 6. The Transformation Abstraction

Invaract should not ultimately be coupled to Spark.

The core abstraction is:

```
Source language / execution engine
              │
              ▼
       Transformation IR
              │
              ▼
       Invaract verifier
              │
              ▼
       Verification result
```

The first-class implementation will be Spark because Spark's logical plan provides a rich representation of the intended transformation.

Over time the same model should support:

* Spark
* Spark SQL
* Generic SQL
* dbt
* Trino
* BigQuery
* DuckDB
* Other query engines
* Potentially workflow or transformation systems that can expose equivalent plans

The engine should therefore be execution-technology agnostic even though individual adapters are technology-specific.

---

## 7. Invaract as a Data Type System

One of the long-term conceptual goals is to make data contracts behave like interfaces and data transformations behave like implementations.

The analogy is:

| Software | Data |
|----------|------|
| Type | Schema |
| Interface | Data contract |
| Program | Transformation |
| Compiler | Invaract verifier |
| Type checking | Contract verification |
| Dependency graph | Lineage |
| Build artifact | Verified data product |

This gives data engineering a familiar model:

> A pipeline should not merely run successfully. It should compile against its data contract.

---

## 8. Contract Verification

Invaract should eventually verify multiple classes of properties.

**Structural**

* Output exists.
* Output location is correct.
* Output schema matches.
* Required fields exist.
* Types are compatible.
* Unexpected fields are rejected where the contract requires it.

**Dependency**

* Required inputs are present.
* Forbidden inputs are rejected.
* Undeclared dependencies are identified.
* Dependencies are version compatible.

**Transformation**

* Column lineage can be established.
* Aggregations are identified.
* Joins are identified.
* Filters are identified.
* Expressions are represented.
* Derived fields can be traced to their sources.

**Governance**

* Restricted fields do not propagate into prohibited outputs.
* Data residency rules are respected.
* Approved sources are enforced.
* Purpose or usage constraints can be represented and checked.

**Compatibility**

* Contract changes can be classified as compatible or breaking.
* Downstream consumers can be identified.
* Proposed changes can be evaluated before merge or deployment.

---

## 9. Contract-Driven CI/CD

Invaract should make contracts part of the software development lifecycle.

A future workflow:

```
Developer changes transformation
              │
              ▼
             Git
              │
              ▼
       Compile / analyse
              │
              ▼
          Invaract
              │
        ┌─────┴─────┐
        ▼           ▼
      PASS         FAIL
        │           │
        ▼           ▼
      Merge      Explain violation
```

A pull request should eventually be able to answer:

* Which contracts does this change implement?
* Does the transformation still satisfy them?
* Did the output schema change?
* Did dependencies change?
* Did lineage change?
* Is the change breaking?
* Which downstream products may be affected?

This makes the contract a first-class software interface.

---

## 10. AI-Ready Data

Verified metadata can provide a substantially stronger foundation for AI systems than inferred metadata alone.

An AI agent should be able to ask:

> Can this dataset satisfy the input requirements of my task?

Invaract could eventually expose machine-readable answers such as:

```
compatible(dataset, contract)
compatible(transformation, contract)
impact(contract_change)
explain(violation)
find_implementations(contract)
find_contracts(dataset)
```

This allows AI systems to reason over data products using verified constraints rather than semantic guesses alone.

Potential applications include:

* AI data discovery
* Agentic pipeline generation
* Automatic transformation selection
* Contract-aware code generation
* Safe dataset recommendation
* Impact analysis
* Automated remediation
* Data product composition

---

## 11. A Contract Registry Ecosystem

Invaract should eventually support a world where contracts are versioned artefacts.

```
Git / Registry
      │
      ├── customer_orders@1
      ├── customer_orders@2
      ├── customer_orders@3
      └── payments@7
             │
             ▼
        Implementations
             │
       ┌─────┼─────┐
       ▼     ▼     ▼
     Spark   dbt  SQL
       │     │     │
       └─────┼─────┘
             ▼
          Verified
```

The contract becomes the stable interface while implementations can evolve independently.

---

## 12. Open Ecosystem Principles

Invaract should be designed as an open ecosystem rather than a closed platform.

Principles:

1. Open specifications over proprietary metadata.
2. Standards first.
3. Adapters over forks.
4. Human-readable contracts.
5. Git-friendly artefacts.
6. Deterministic verification wherever possible.
7. Explainable failures.
8. No requirement for a central SaaS control plane.
9. Interoperability with existing lineage and catalogue ecosystems.
10. Execution engine independence in the core model.

---

## 13. Long-Term Destination

The long-term ambition is not to become another data catalogue, lineage UI or data quality platform.

Invaract should become a verification primitive underneath those systems.

The ecosystem could eventually look like:

```
                    DATA CONTRACTS
                          │
                          ▼
                   ┌─────────────┐
                   │  INVARACT  │
                   │ verification│
                   └──────┬──────┘
                          │
        ┌─────────────────┼─────────────────┐
        ▼                 ▼                 ▼
     Lineage             CI/CD              AI
        │                 │                 │
        ▼                 ▼                 ▼
   Catalogues         Deployment        Data agents
   Governance         Platforms         Discovery
```

The ultimate goal is simple:

> Make data transformations provable, not merely observable.

---

## 14. What Success Looks Like

Invaract succeeds when it becomes normal for a data product to have:

1. A versioned machine-readable contract.
2. One or more implementations.
3. Automated verification of those implementations.
4. Verified lineage derived from those implementations.
5. Contract-aware CI/CD.
6. Machine-readable compatibility and impact information.
7. Interoperability across transformation engines.

The measure of success is not the number of Spark plugins installed.

It is whether the data engineering ecosystem begins to treat:

> "Does this transformation conform to its contract?"

as a normal question that can be answered automatically.
