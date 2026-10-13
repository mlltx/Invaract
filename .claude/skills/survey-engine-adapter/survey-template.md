<!--
  TEMPLATE for the engine adapter survey (.claude/skills/survey-engine-adapter/SKILL.md).
  Copy to docs/<ENGINE>_ADAPTER_SURVEY.md, fill every section, and DELETE every HTML comment (these) before finishing:
  `grep -n '<!--' docs/<ENGINE>_ADAPTER_SURVEY.md` must print nothing.
  The reader is intelligent and new to this engine's internals. Define each term where it first appears.
-->

# <Engine> adapter survey

> **Status: survey. No adapter code exists.** This document helps decide whether and how to build one.
> Engine surface surveyed: <e.g. the Java SDK, version x.y, on the in-process test runner>.
> Written: <date>. Evidence checked: <date>. Source of each claim: section <N>, "Evidence log".

## The short version

<!--
  Five to ten lines a busy person can act on. Plain words. No unexplained terms.
-->

**What it would be.** <One paragraph: what the adapter would do, in the reader's terms: "Before a <engine> job
starts, Invaract would read the job's description, compare it with the data contract, and refuse to start it if
it breaks the contract.">

**How much of the suite it is expected to pass.** <N> of <M> scenarios by doing the check; <N> by refusing
safely (explained below); <N> partly; <N> skipped as not applicable. Details in section 7.

**Can it be switched on without changing the job's code?** <Yes / Partly / No>, by <one line>. Section 5.

**The biggest risks.**
1. <risk, one line, why it matters>
2. <...>

**Decisions I need from you.** (Each is explained in section 4; a one-line answer is enough.)
1. <decision and my recommendation>
2. <...>

## How to read this document

- **What "conforming" means here.** Invaract has a *conformance suite*: <M> small, engine-neutral jobs ("scenarios")
  every adapter is run against. An adapter declares what it can and cannot do (its *declaration*), and each scenario
  must come out the way that declaration promises. **Refusing safely is conforming:** if an adapter says "I cannot
  check this", the suite expects it to refuse any contract that depends on it, rather than let the job through
  unchecked. So a ❌ below is a limit to understand, not a failing grade.
- **Forecast marks.** ✅ expected to do the check and produce what the suite expects. ⚠️ only partly, or with a
  workaround, or depends on something not yet verified. ❌ cannot do the check, so the suite expects a safe refusal or
  a skip. ➖ does not apply to this engine.
- **Confidence.** **High**: shown by a probe I ran. **Medium**: stated by the engine's own source or documentation.
  **Low**: inferred. A forecast is a forecast: nothing here has been run as an adapter.
- **Words.** Each technical term is explained where it first appears and again in the glossary at the end.

## 1. What Invaract asks of an engine

<!--
  One page. A reader who has never seen Invaract should finish this able to follow the rest.
  Cover: a data contract (what a dataset must look like and where a job may read and write), the check that
  runs before a write, and what happens on failure; the engine-neutral model (the IR) with a small drawing;
  capabilities and declarations; scenarios. Use Spark as the worked comparison.
-->

### The contract, and the check

<plain explanation with a tiny example contract>

### The engine-neutral model (the "IR")

<Explain: every engine describes a job in its own way; Invaract translates that description into one small
engine-independent description of reads, filters, joins, groupings and writes, and all the checking code works
on that. Draw a three-step job as boxes. Say how Spark does the translation, in two sentences.>

```
read orders  ->  filter amount > 0  ->  write report
```

## 2. The engine in plain words

<!--
  What it is, how a job is written, how it runs, and where it can be stopped. A small drawing of a tiny job as
  the engine sees it. No assumed knowledge of the engine.
-->

## 3. How the adapter would get hold of a job

<!--
  THE most important section. For each engine artefact the adapter will rely on (a pipeline graph, a query plan, a
  syntax tree, a job configuration, a service response), these five labelled lines, with a real captured example.
  Repeat the block for each artefact.
-->

### <Artefact name, e.g. "The pipeline graph">

- **What it is.** <one plain sentence, an everyday comparison if it helps>
- **How we get hold of it.** <the exact call or option; *when* it is available - before the job runs (so we can
  block), while it runs, or only afterwards (so we can only report); and from what code>
- **What it contains.** <the facts we need from it, named>
- **What it leaves out.** <what we would need and cannot get, and what that costs us>
- **A real example.** Captured by <command / probe>, <date>:

```
<verbatim output>
```

<Line-by-line reading: "this line is the dataset the job reads; this one is the write and where it lands; ...">

## 4. Key design decisions

<!--
  One block per decision, in this fixed shape, in plain words. At least: where it hooks in; how it gets hold of
  the job (refer to section 3); how names become locations; how types map; how writes are described; what cannot
  be seen and the policy (fail closed); how results are reported; language and packaging; how CI proves it.
-->

### D1. <Decision as a question>

- **Options.** <A / B / C, one line each>
- **Recommendation.** <one option>
- **Why.** <reasoning a non-expert can follow>
- **What it costs.** <what we give up, or the work>
- **What would change my mind.** <the observation that would flip it>

## 5. Switching it on without touching the job: external attachability

<!--
  Invaract's rule: a platform team that does not own a job's source must be able to turn the engine's checking on
  through the engine's own configuration. Answer plainly: yes, partly, or no. Show the command.
-->

**Verdict: <Yes / Partly / No>.**

**The mechanism.** <exactly how, e.g. a command-line flag that names a wrapper, a plugin registered with the
engine, an environment variable>. Example:

```
<the exact command or configuration, copy-pasteable>
```

**The settings.** Every neutral Invaract setting and how it would be spelled on this engine:

| Setting | What it does | How it is given on this engine | Possible? |
|---|---|---|---|
| `contract` | which contract to enforce (a file, or `registry://id@version`) | | |
| `dryRun` | observe and infer a contract instead of enforcing | | |
| `notifyConfig` | where validation events are sent | | |
| `registryUrl` | where `registry://` contracts are fetched | | |
| `locationMap` | maps `ref://` names in a contract to real locations | | |
| `orgPolicy` | organisation-wide rules layered on the contract | | |
| `rejectUndeclaredInputs` / `rejectUndeclaredFields` | stricter checking | | |
| `computeFingerprint` / `staticDataQuality` / `roleConsistency` | optional analyses | | |

**How a job can get around it, and what is not possible.** <honest list>

## 6. What it would look like to a user

<!--
  A worked example the reader can picture. Mark predicted output as PREDICTED; use captured output where you have it.
-->

**The contract** (what the data must look like):

```yaml
<tiny contract>
```

**The job** (what the team wrote; unchanged):

```
<tiny job in the engine's language>
```

**Turning it on** (what the platform team does):

```
<command or configuration>
```

**A passing run.** <what the user sees>

**A blocked run (PREDICTED).** <the message, in the four-part format Invaract prints: what was checked, what
failed, why it matters, what to do>

```
<predicted message>
```

**Dry-run mode (PREDICTED).** <what it would print and where the inferred contract goes>

**Where the events go.** <the sink, and the fields that name the engine and the job>

## 7. How much of the suite it is expected to pass

### 7.1 The capability declaration, forecast

<!--
  One row per capability (all of them). Status the adapter would declare and the reason in plain words.
  Group by category. "Blocks if unsupported" capabilities that are unsupported make the suite expect a safe refusal.
-->

| Capability | What it means | Would declare | Why | Confidence |
|---|---|---|---|---|
| | | | | |

**Enforcement point:** `<in-engine-blocking | pre-submit-gate | observe-only>` - <what that means for what can be blocked>.

### 7.2 Scenario by scenario

<!--
  Paste the table generated by ./dev/adapter-survey (with the draft declaration; it already carries each scenario's
  plain-words "What it means for a user"), then fill Forecast, Why and
  add a Confidence column. Keep the user-story column: the reader should not have to know the kit to follow a row. Group the rows under headings by capability family, each with a one-line summary of the
  shape ("structural checks: strong; plan-shape rules: partial; ..."). Use the legend marks.
-->

### 7.3 Claims no scenario can check

<!--
  The attested capabilities (AttestedClaims.requirements): for each, how the adapter's own tests would show it, or why it is unsupported.
-->

| Capability | What a test must show | How this adapter's test would show it | Forecast |
|---|---|---|---|
| | | | |

### 7.4 Totals

<N> of <M> by doing the check · <N> by refusing safely · <N> partly · <N> skipped. <Two sentences on what that
means in practice for a user.>

## 8. What it would take

<!--
  Ordered milestones, each with the size (small / medium / large, no dates), what it unlocks, and how it is
  proven in CI. Start with settling the UNVERIFIED items. Say what a first useful version is.
-->

## 9. Risks and open questions

| Risk or question | Why it matters | How to settle it |
|---|---|---|
| | | |

## 10. Evidence log

### Probes I ran

<!--
  Each: what, the command, when, and the relevant output. The probe code itself is not committed.
-->

### Sources

<!--
  Link or file-and-line for every claim that rests on a source.
-->

### Unverified

<!--
  Every claim marked UNVERIFIED above, with the cheapest check that would settle it.
-->

## Glossary

<!--
  Every technical term used, in order of first appearance, one plain sentence each. Include the violation types
  that appear in the table (copy the meanings from docs-site/src/content/docs/reference/violation-types.md).
-->

## Retrospective

<!--
  Filled in after the adapter exists: add an Actual column to 7.2 and say which forecasts were wrong and why.
-->

_Not yet: no adapter exists._
