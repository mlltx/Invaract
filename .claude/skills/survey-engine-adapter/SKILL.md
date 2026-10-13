---
name: survey-engine-adapter
description: Produces a plain-language feasibility survey for a new Invaract engine adapter (Apache Beam, BigQuery, Flink, dbt, Trino, or any engine other than Spark) BEFORE any adapter code is written, so the person deciding can understand the key design decisions and see, scenario by scenario, how much of the conformance suite the adapter would pass. Use this whenever someone asks to survey, scope, size up, assess feasibility of, or "see how it would work" for a new engine adapter, or before starting any adapter ("should we add X", "what would an X adapter look like", "how much could X conform", "how would X attach to a job"). The output is one document written for a smart reader who is NOT an expert in the engine or in Invaract's internals - it explains every technical term (a plan, a graph, an AST, a runner, a hook) where it first appears, shows how the adapter would get hold of the engine's description of a job and what that contains, forecasts every conformance scenario with a tick/cross/partial and a reason, describes the external-attach story and what it looks like to a user, and logs the evidence for each claim. It never starts the adapter.
---

# Surveying a new engine adapter

This is the step **before** `docs/ADDING_AN_ENGINE_ADAPTER.md`. It answers one question for the person who has
to decide whether and how to build an adapter: *what would we be getting into, and how much of Invaract would
work on this engine?* It ends with a document and a list of decisions for them. **It does not write adapter code,
run `dev/new-adapter`, or start the module** - starting an adapter needs the person's explicit go-ahead after
they have read the survey.

It is not the engineer's design record (`docs/<ENGINE>_ADAPTER.md`, which `dev/new-adapter` generates and the
adapter's author fills in). The survey comes first, is written for a reader, and feeds that record afterwards.
It is also not the add-an-adapter skill; that one is deliberately not written until a first non-Spark adapter has
produced a friction log to write it from.

## The reader, and what that demands of the writing

**Assume the reader is intelligent, busy and new to this engine's internals.** They are deciding things, not
implementing them. They may not know what a "plan", a "graph", an "AST", a "DAG", a "runner", a "transform", a
"sink", a "hook" or "lineage" is, and the survey's job is to make them able to judge, not to impress them.

The rules below are not style preferences. A survey that skips them has failed even if every fact in it is right.

1. **Define every technical term where it first appears, in a clause, in plain words.** "The pipeline graph (the
   engine's own list of the steps the job will run, and how data flows between them)". Never "the graph" bare.
   The first use of each of these needs a definition: plan, graph, AST, DAG, runner, transform, sink, source,
   schema, lineage, IR, hook, enforcement point, fail closed, attach, wrapper, adapter, scenario, capability,
   declaration, conformance. If you find yourself writing one without a definition, stop and add it.
2. **Whenever the adapter will rely on something the engine produces (a graph, a plan, a syntax tree, a job
   configuration, a response from a service), say five things about it** - this is the single most important
   section of the survey, and each one gets its own labelled line:
   - **What it is**, in one plain sentence, with an everyday comparison if it helps.
   - **How we get hold of it**: the exact call, option or event, *when* it is available (before the job runs,
     while it runs, or only afterwards - this decides whether we can block anything), and from what code.
   - **What it contains**: the specific facts we need (which datasets are read, where the write lands, the column
     names and types, the filters and joins), shown on a real example.
   - **What it leaves out**: the facts we would need and cannot get (values inside custom code, row-level
     detail), and what that costs us.
   - **A real example**, captured from the engine in Phase 2, shown verbatim, with a line-by-line reading.
3. **Show, then tell.** Every abstract claim about how the engine represents a job is followed by the real thing
   you captured. A reader should be able to look at it and see the dataset names and column types for
   themselves.
4. **Every tick and cross has a reason a non-expert can follow** - "the engine knows the output's column types
   before it runs (see section 3), so Invaract can compare them with the contract", not "supports schema
   checks".
5. **Say what you do not know.** Every factual claim about the engine either has evidence (a link, a command
   you ran and its output) or is marked **UNVERIFIED** in place, and the unverified ones are collected at the
   end with the cheapest check that would settle each. "From memory" is not evidence. Honest uncertainty is the
   most useful thing the reader gets.
6. **Recommend; do not survey the universe.** For each design decision give the options briefly, then one
   recommendation with the reason and the cost. The reader can overrule it; they should not have to derive it.
7. **A forecast is a forecast.** Never write "will pass" about something you have not run. Use "expected to", and
   attach the confidence (High = shown by a probe you ran; Medium = stated by the engine's own source or
   documentation; Low = inferred).
8. **Plain words beat precise-sounding words.** Short sentences. Active voice. Concrete nouns. Tables for
   comparisons, prose for reasoning.

## Phase 0 - Scope

Settle, from the request and the repository, and write down at the top of your notes:

- The engine, and *which part of it* (Beam has several language SDKs and many runners; BigQuery has queries,
  loads and streaming inserts; Flink has a SQL layer and a DataStream API). Pick the surface the adapter would
  most plausibly sit on, say why, and list the others under "what would change".
- The version(s) you are looking at.
- Whether a prior research document exists (`docs/ENGINE_RESEARCH_*.md`). Build on it; do not redo it, and re-check
  anything in it marked unverified that this survey depends on.

Ask the person a question only when you are genuinely blocked on a choice that is theirs to make and cannot be
sensibly defaulted. Otherwise choose, and list the choice under "Decisions for you".

## Phase 1 - Learn what the adapter is held to

Read, in this order, enough to explain it back in plain words:

1. `docs/ADDING_AN_ENGINE_ADAPTER.md` (the process and its twelve investigation questions) and
   `docs/MULTI_ENGINE_ADAPTERS.md` (the design: what is engine-neutral and what an adapter owns).
2. `verification-core/src/main/scala/com/invaract/verification/Capability.scala`: the capabilities (things an
   adapter declares it can or cannot do), their categories, and the four statuses (`supported`, `partial`,
   `unsupported`, `not-applicable`) and three enforcement points.
3. `adapter-testkit/src/main/scala/com/invaract/testkit/Scenarios.scala`: the scenarios (small, engine-neutral
   jobs every adapter must handle), and `AttestedClaims.scala` for the capabilities no job can check.
4. `docs-site/src/content/docs/reference/violation-types.md`: plain meanings for the violations the scenarios
   expect. You will copy the ones you use into the survey's glossary.
5. How Spark does each thing, in `docs/SPARK_ADAPTER.md` - it is the one worked example, and the survey
   compares against it ("Spark does X by Y; here the equivalent is Z").

## Phase 2 - Investigate the engine, with evidence

Answer the twelve questions in `docs/ADDING_AN_ENGINE_ADAPTER.md` Step 1, plus these that the survey needs:

- **What is the engine's own description of a job, and when can we see it?** (the thing rule 2 above is about)
- **Which language and packaging would the adapter use?** The kit and the engine code are JVM/Scala; an engine
  whose jobs are written in another language needs a stated bridge, or is a different, larger piece of work.
- **How would a platform team turn this on for a job whose source they do not own?** (External Attachability,
  in `CLAUDE.md`.) Name the exact mechanism - a command-line flag, an environment variable, a configuration
  file, a plugin registry - and its limit (what a job can do to bypass it).
- **Where do errors surface?** If Invaract blocks a job, what does the user see, and where?

**Evidence rules.** Prefer, in this order: a probe you ran; the engine's own source code; its official
documentation. Record every source (URL, file and line, or the exact command). A page you could not reach is
"not checked", not "assumed". If the environment blocks the documentation host, say so and fall back to source.

**Capture a real example.** Build the smallest throwaway job on the real engine in the scratchpad directory
(never in the repository), run it far enough to capture the engine's description of itself (print the graph, dump
the plan, log the configuration), and keep the output verbatim for the survey. If the engine cannot be run here,
say that plainly, capture it from the engine's own tests or documentation, and mark the example as not
reproduced. Do not commit the probe. Record in the evidence log the command and the relevant output, because the
probe is deleted but the claim stays.

## Phase 3 - Draft the capability declaration

Copy `adapter-testkit/src/main/resources/reference/invaract-capabilities-reference.yaml` to the scratchpad and set
**every** capability (an undeclared one is an error) to the status the engine can honestly support, with a note
saying why. Rules that keep it honest:

- `supported` means a scenario will demonstrate it. If you could not, it is `partial` (say which part) or
  `unsupported`.
- A capability marked *blocks if unsupported* (`enforcesContract`) that you cannot do is declared `unsupported`,
  which makes the kit expect a **safe refusal** of any contract that relies on it. That is a correct, valuable
  outcome, not a failure; the survey should say so in plain words.
- `not-applicable` is for things that do not exist in the engine (a batch-only engine has no streaming), not for
  things that are merely hard.
- The enforcement point (`in-engine-blocking`, `pre-submit-gate`, `observe-only`) is a decision with
  consequences for every row; it gets its own explanation.

## Phase 4 - Forecast every scenario

Generate the table from the real catalogue and your draft declaration:

```bash
./dev/adapter-survey <scratchpad>/scenario-table.md <scratchpad>/draft-capabilities.yaml
```

It has a row per scenario with what it checks, the capabilities it is evidence for, the neutral result and - from
your declaration - **what the kit will expect of this adapter** (does the checks and allows; does the checks and
blocks with a given violation; must refuse safely; or is skipped, with the reason). That column is computed with
the kit's own rule, so it is not yours to change: change the declaration instead if it looks wrong.

Then fill in the last two columns for every row:

| Forecast | Meaning |
|---|---|
| ✅ | The adapter is expected to produce exactly what the kit expects, by doing the check. |
| ⚠️ | Expected to conform only in part, or with a workaround, or it depends on something unverified. Say what. |
| ❌ | The adapter cannot do the check. The kit then expects a safe refusal or a skip (the previous column says which), so this is **not** a failing scenario - say that, and say what the person would lose. |
| ➖ | The scenario does not apply to this engine. |

Every row gets a one-sentence **Why** that a non-expert can follow, and a confidence (High / Medium / Low). Group
the rows under the capability families so the reader sees the shape ("structural checks: strong; plan-shape
rules: partial; lineage: not available").

Add the **attested** capabilities as their own short table (`AttestedClaims.requirements` says what each must
demonstrate): for each, how the adapter's own tests would show it, or why it is unsupported.

Finish with totals that match the table exactly: *N expected to conform by doing the check, N by refusing safely,
N partial, N skipped, of the suite's M scenarios.*

## Phase 5 - Design decisions, attach story, and the user's view

Write the decisions the person has to make or confirm. For each: **the question, the options (briefly), the
recommendation, why, what it costs, and what would change your mind.** Cover at least:

1. Where the adapter hooks in (the enforcement point) and what that means for what can be blocked.
2. How it gets hold of the engine's description of a job (rule 2's five lines), and how that is turned into
   Invaract's engine-neutral model of a transformation (the "IR": a small, engine-independent description of
   reads, filters, joins, groupings and writes that all the verification code works on - explain it in two
   sentences with a drawing).
3. How the engine's dataset names become the locations a contract uses.
4. How the engine's types map to the contract's types, and what has no equivalent.
5. How writes are described (formats, dispositions such as append/overwrite) and what the defaults are.
6. What cannot be seen (custom code, external calls) and the policy: such a job must be refused or reported as
   unverifiable, never silently passed (this is "failing closed").
7. How results are reported (the validation events and which engine and job they name).
8. Language and packaging.
9. How CI proves it without secrets (a real in-process engine, an emulator, recorded fixtures, or a credentialed
   job on a schedule) and which "verified" each one backs.

**External attachability** gets its own section: the exact mechanism, every neutral setting (`contract`,
`dryRun`, `notifyConfig`, `registryUrl`, `locationMap`, `orgPolicy`, `rejectUndeclaredInputs`,
`rejectUndeclaredFields`, `computeFingerprint`, `staticDataQuality`, `roleConsistency`) and how it would be
spelled on this engine, what a job can do to bypass the attachment, and what is simply not possible. The test of
this section: could a platform team that does not own the job's source turn it on? Say yes, partly, or no, and
show the command.

**The user's view** is a worked example the reader can picture: a tiny contract, a tiny job, the command or
configuration to turn Invaract on, what a passing run looks like, what a blocked write looks like (the predicted
message, labelled as predicted), what dry-run mode would show, and where the validation events go. Label every
predicted output as predicted; use captured output wherever you have it.

## Phase 6 - Write the document

Copy `.claude/skills/survey-engine-adapter/survey-template.md` to `docs/<ENGINE>_ADAPTER_SURVEY.md` (uppercase
engine, as `dev/new-adapter` names its design doc), fill every section, and register it in
`contributor-docs/scripts/sync-docs.mjs` next to the other design docs so it renders on the contributor site.

Before you call it finished, check each of these against the document, and fix what fails:

- [ ] Every term in rule 1's list is defined at first use. Read the document top to bottom as the reader would.
- [ ] Every place the adapter relies on an engine artefact has the five lines of rule 2, with a real example.
- [ ] Every scenario has a forecast, a confidence and a reason; the totals equal the table.
- [ ] Every factual claim about the engine has evidence or is marked UNVERIFIED; the unverified list at the end
      is complete and each item has the cheapest check.
- [ ] Every ❌ and ⚠️ says what the person loses or what would close it.
- [ ] The attach section says yes, partly or no, with a command.
- [ ] The user's-view example labels predicted output as predicted.
- [ ] "Decisions for you" is a short numbered list the reader can answer in a line each.
- [ ] No adapter code, module or `dev/new-adapter` output was created.

## Phase 7 - Report, and stop

Reply in chat with: the verdict in a few lines (what it would be, how much of the suite it is expected to
conform to, the two biggest risks), **the scenario table**, the numbered decisions you need, and where the document
is. The person often cannot open files, so the chat reply must stand on its own:

- Print the scenario forecast as a compact table in the reply itself, one row per scenario: `#`, scenario id, the
  mark (✅ ⚠️ ❌ ➖), and a reason of at most a dozen plain words. Group the rows under the family headings the
  document uses, and end with the totals line. Do not link to the document instead of printing it.
- Also print the capability declaration as a compact table: capability, status it would declare, one-line reason.
- Do not paste the whole document, and keep every other table out of the reply; point to the section instead.

Offer to open the survey as a pull request; follow the session's rules on merging. **Do not start the adapter.** The
next step is the person reading the survey and saying go.

## Phase 8 - After the adapter exists (the survey earns its keep here)

When the adapter has been built and its conformance suite runs, return to the survey and add a column **Actual**
to the scenario table plus a short **Retrospective**: which forecasts were wrong, and why. A survey whose
forecasts are never checked teaches nothing; this is how the next survey gets better.
