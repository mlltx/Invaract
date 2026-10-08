# Adding an Engine Adapter

This is the process for giving Invaract a new engine: Apache Beam, BigQuery, or anything else whose jobs write
data. It is for a *new engine*. A new data source on an engine that already has an adapter is
`docs/ADDING_A_SPARK_CONNECTOR.md`.

It exists so the second and third adapter do not repeat the discoveries of the first. Everything an adapter
claims is held to a check that can fail, and everything it does not do is said, with a reason, in a file the
build reads. The process below is that discipline made into steps, each with a way to know it is finished.

Three artifacts carry it; read their docs before starting:

- `docs/MULTI_ENGINE_ADAPTERS.md`: what an adapter is (the SPI), the conventions every adapter follows
  (locations, formats, save modes, types), and the conformance kit.
- `docs/ENGINE_RESEARCH_BIGQUERY_BEAM.md`: a worked investigation of two engines, including what could not be
  checked. Your investigation should read like it.
- `modules.json` and `dev/new-adapter`: the module registry and the generator below.

## The rule that decides everything

**An adapter may claim a capability only as far as something checks it.** A capability a job can show is claimed
by passing the kit's scenario for it, run on the real engine through the real enforcement path. A capability no
job can show (how the adapter attaches to a job, what it applies before one exists) names the test that
demonstrates it (`AttestedClaimsSpec`). Everything else is `unsupported` or `not-applicable`, with a note, and a
contract that relies on it is then rejected as `UNSUPPORTED_CONTRACT_FEATURE` rather than passed unchecked.

Two consequences. An adapter that does little is a correct adapter, as long as it says so. And "it compiles and the
happy path works" is never the finish line; the kit's report is.

## Before you start

- A go-ahead from the maintainer to start the adapter. No engine adapter starts on its own initiative.
- `./dev/build` has run (it publishes `contract`, `ir`, `fingerprint`, `verification-core` and `adapter-testkit` to
  the local Ivy repository, which the new module resolves).
- A friction log: a file you append to as you go (`docs/<ENGINE>_ADAPTER.md`, a section "Friction log"). Record
  every undocumented decision, every surprise, every place you guessed. It is the input to improving this guide
  and the add-an-adapter skill, and it is how the next adapter gets cheaper.

## Step 1: Investigate the engine

Answer these in writing before any code, with evidence for each: a link to the engine's own source or a command
you ran, never memory. Mark anything you could not check **unverified** and list it at the end; the first thing
the adapter's tests should do is settle those.

| # | Question | Why it matters | Feeds |
|---|---|---|---|
| 1 | **Where can this engine be gated?** Inside the engine, blocking (a hook that can abort a write: Spark's check rule); before submission (a graph or statement is available and a wrapper can refuse to submit it); or only observed afterwards? | It is the adapter's `enforcement.point`, and it bounds every claim below | capability declaration |
| 2 | **What is knowable at that point?** The reads and writes; the output schema and nullability; the plan (filters, joins, groupings, function calls); parameters (disposition, format) | Anything not knowable cannot be checked: declare it unsupported, do not approximate | which capabilities can ever be `supported` |
| 3 | **How could a platform attach this to a job it does not own?** The engine's own configuration surface (flags, options, environment, runner selection) | The External Attachability Requirement in `CLAUDE.md`; `config.zeroCodeInstall` is `unsupported` if there is no answer | `ConfigSource` spelling |
| 4 | **How are datasets named?** Paths, qualified names (`project.dataset.table`), URIs | Locations are canonical `/`-separated strings; an engine whose names are not paths converts with `LocationMatching.fromParts` | location checks |
| 5 | **What are its formats and catalog technologies?** | Use the canonical names (`Formats`, `CatalogTechnologies`) where they exist; never respell one | format and catalog checks |
| 6 | **What are its write dispositions, and their defaults?** Defaults that differ by operation are the usual trap | Map onto `append`/`overwrite`/`ignore`/`error` or `None`; an approximate mapping is stated in the capability note | save-mode check |
| 7 | **How do its types map to `LogicalType`?** Every type, including the ones with no neutral equivalent (`OtherType`) and the precision rules | A wrong mapping is a silent false pass or a false rejection | schema checks |
| 8 | **What does nullability mean?** Per field, per format, per table kind | The kit's nullability scenarios assume a column the job can leave null is nullable | schema checks |
| 9 | **What are the names of its functions?** Especially the non-deterministic ones (`uuid`, `rand`, the clock) | Aliases onto the canonical function catalog, so non-determinism is classified identically on every engine | fingerprint |
| 10 | **What changes data without being a plain write?** Truncate, row-level DML, snapshot rollback, scripts | The fail-closed representative (`failClosed.unverifiableWrites`) and the DML job shape | fail-closed, DML |
| 11 | **Does it stream? Does it have a checkpoint or cache?** | `read.streaming`, `write.streaming`, `lineage.boundaryResolution` | those capabilities |
| 12 | **What runs in CI with no secrets?** A real in-process runner, an emulator, recorded fixtures, or only a credentialed job | The conformance kit judges the real engine; if CI cannot run it, say which of fixtures or a periodic credentialed job backs each "verified" | CI evidence |

The output is a short design document (`docs/<ENGINE>_ADAPTER.md`), the engine's counterpart of
`docs/SPARK_ADAPTER.md`, registered in `contributor-docs/scripts/sync-docs.mjs`. It ends with a draft capability
table: for each of the 30 capabilities, a proposed status and the evidence.

## Step 2: Generate the module and register it

```bash
dev/new-adapter <engine> [--display "BigQuery"]    # creates <engine>-adapter/ and adds it to modules.json
python3 .github/scripts/check_modules.py            # names every other file that must list the new module
```

The skeleton compiles and passes as generated: every capability is `unsupported`, so every conformance scenario is
canceled, and `ConformanceAdapter.run` is a stub that throws until you write it. CI builds a fresh skeleton on every
run (`adapter-scaffold-check`), so it cannot rot.

`check_modules.py` is the to-do list. It fails until each of these agrees with `modules.json`; it prints the file
and line, and what is missing:

- `.github/workflows/test.yml`: the `for module in` loops (API compatibility, coverage), the engine-change filter,
  a `mutation-testing-<engine>-adapter` job and its place in `summary`'s `needs`;
- `.github/workflows/release.yml` (a `Publish` step, in dependency order) and `release-dry-run.yml`;
- `dev/build` (build order), `.claude/hooks/session-start.sh` (warm-up), `dependency-graph.yml`;
- the adapter's own parts: its capability declaration, a test that extends `AdapterConformanceSpec`, one that
  extends `AttestedClaimsSpec`, a test dependency on the kit's current version.

Also, because the check cannot see them: the mutation-shard list if the adapter is large enough to shard
(`mutation-shard-drift-check` will say), `docs/RELEASING.md`, and the engine-neutral docs that count adapters.
Run the check again until it is clean.

## Step 3: Write the declaration honestly

Start from the generated file (everything `unsupported`). Move a capability only when its row in step 1 has
evidence and the scenario for it passes (step 5). Rules of thumb:

- `partial` needs a note that says *which shapes are not covered*, not a general disclaimer.
- `not-applicable` is for a concept the engine does not have (a batch-only engine has no streaming), not for
  something you have not got to.
- `enforcement.point` is the answer to question 1, with the bypass stated if there is one.
- Do not claim `config.zeroCodeInstall` unless question 3 has an answer a platform team could use.

## Step 4: Implement the adapter

An adapter owns exactly this, and nothing the pipeline already does:

1. **Recognise** an operation on the engine's own hook (question 1).
2. **Translate** it to `ir.Plan` (`Read`, `Write`, `Project`, `Filter`, `Join`, ...), with `UnknownPlan` for anything it
   cannot translate, so nothing is silently dropped.
3. **Map schemas** to `LogicalSchema` (question 7), and input schemas for each read.
4. **Hand over** a `CheckedWrite` to `VerificationPipeline.verifyWrite` (or `verifyStateChange`,
   `rejectUnverifiableWrite`). Do not re-implement validation, verifier order, fingerprinting, events or the
   rejection text.
5. **Read configuration** through `ConfigSource` and `VerificationSetup`, spelling the neutral keys the engine's way.
6. **Declare** what it does (step 3) and **fail closed** on what it cannot translate.

`adapter-testkit`'s `ReferenceAdapter` is the shortest complete example (about forty lines, no engine).
`SparkConformanceAdapter` is the real one.

## Step 5: Run the kit, one capability at a time

`<Engine>ConformanceAdapter.run` builds the neutral `ScenarioJob` as a real job on the engine and runs it through
the adapter's real enforcement; it finishes with `sink.passed` or `sink.rejected(types)`. Then, in this order,
declare a capability `supported`, run `<Engine>ConformanceSpec`, and fix what diverges:

1. Operations and structure: `read.batch`, `write.batch`, `check.location`, `check.schema`, `check.inputExistence`.
2. Formats, save modes, catalog, nested types, nullability.
3. Fail-closed (`failClosed.unverifiableWrites`) and the DML job shape.
4. Rules and the analyses (`translation` exposes the plan for lineage and sensitivity).
5. Streaming and boundaries, if the engine has them.

The report has three verdicts: *conforms*, *skipped* (a scenario the declaration says does not apply, with the
adapter's own reason), and *diverges* (the adapter did something its declaration does not allow). A divergence is a
bug in the adapter or an overclaim in the declaration; never a reason to edit a scenario. If a scenario is wrong,
that is a kit change with its own PR and a `BrokenAdapterSpec` variant.

## Step 6: Attest what no job can show

For each of the seven attested capabilities the adapter claims, name the test that demonstrates it in
`<Engine>AttestedClaimsSpec`. `AttestedClaims.requirements` says what each must show. Write the test; the kit only
checks that it exists.

## Step 7: The two ledgers

The investigation ends with two tables, kept in `docs/<ENGINE>_ADAPTER.md`, as for a Spark connector:

- **Operation surface**: every shape of read, write and state change the engine has, each ✅ covered (a permanent
  test), 🚫 fails closed (a test that proves the rejection), or ❓ not investigated.
- **Capability surface**: each of the 30 capabilities, its declared status, and what backs it (a scenario id, an
  attestation, or the note that says why not).

A ❓ is an open item, not a finished one. The adapter is not "complete" while one remains, and the docs must not call
it complete.

## Step 8: The repository's gates

Everything `CLAUDE.md` asks of an engine module applies to the new one:

- tests, with real-engine tests preferred over mocks (ADR-005);
- coverage: measure, then pin `coverageMinimumStmtTotal`/`Branch` a few points under;
- scoped mutation testing: at least 70% on the files you wrote, written to be mutation-resistant the first time;
- API compatibility: after the first release, `mimaPreviousArtifacts` names it; before, it is empty;
- `./dev/capabilities` to regenerate the engine-capabilities page, and the user documentation (`docs-site`) for
  anything a user of the engine would need: how to install it, what it verifies, what it does not;
- the External Attachability Requirement: every capability a job can turn on is reachable through the engine's own
  configuration surface, or the declaration says it is not;
- `./dev/test` and `./dev/regression` where the change touches enforcement of the Spark harness. (A new adapter does
  not run the Spark demo; its own conformance run is its real-engine proof.)

## Definition of done

- [ ] The investigation (step 1) is written, with evidence, and its unverified items are settled or listed.
- [ ] `check_modules.py` is clean.
- [ ] `<Engine>ConformanceSpec` has no divergence; every scenario is conforms or skipped with the declaration's reason.
- [ ] `<Engine>AttestedClaimsSpec` is green; every claimed attested capability names a test.
- [ ] The declaration is honest: nothing `supported` without a passing scenario or an attestation.
- [ ] Both ledgers exist, with no ❓ presented as done.
- [ ] Coverage is pinned from a measurement; mutation is at least 70% on the new files; `./dev/capabilities` is regenerated.
- [ ] The user documentation covers installing and using the adapter and what it does not verify.
- [ ] The friction log is complete, and any step of this guide that was wrong or missing is fixed in this PR.

## What this guide does not cover yet

It has been written from one real adapter (Spark) and the reference adapter, and checked against research on two
more. The first adapter built from it will find what it gets wrong; the friction log is how that comes back here.
The add-an-adapter skill should be written from that log, not before it.
