# Documentation Playbook

This file is the operating manual for `docs-site/` — the Starlight user documentation
site for Invaract. It's written for whoever (human or Claude Code) next adds or edits a
page here. If you're about to touch `docs-site/`, read this first.

For the policy on *when* documentation must be updated as part of other work, see
[CLAUDE.md](../CLAUDE.md)'s "Documentation Policy" section. This file is about *how* to
write it well once you've decided to.

## Audience

**Users of Invaract** — people writing Spark jobs who want to install and use the
verification engine, and people authoring data contracts against it. Not contributors to
Invaract's own codebase.

The test for every page: could someone who has never opened this repository's source
follow this page and succeed, using only Invaract's public, user-facing surface (the
contract format, the enforcement rule, the CLI-equivalent `dev/` scripts) — without
reading Scala internals?

If a page can only be understood by someone who's read `SparkPlanAdapter.scala`, it's in
the wrong place, or it's disclosing an implementation detail with no user-facing payoff.
An occasional class or method name is fine when it's literally the API a user calls
(`ContractEnforcementRule.forContract`, `ContractParser.parseFile`) — that's user-facing
surface, not internals.

## Information architecture

```
Introduction     — what this is, why it exists, who it's for
Getting Started  — install → first successful use, in order
Guides           — task-oriented: "how do I accomplish X"
Concepts         — the mental model needed to use the product well
Reference        — exhaustive, stable facts (format, types, commands)
Troubleshooting  — known problems and their fixes
```

A new page's placement should be obvious from this list. If it isn't, that's a sign the
page is trying to do two things — split it, don't force a bad fit.

**Guides vs. Concepts**: a guide answers "how do I do X" and ends with the reader having
done something. A concept page answers "why does X work this way" or "what do I need to
understand before I do X" and ends with the reader knowing something. Don't turn a guide
into a concept essay, and don't turn a concept page into a numbered task list.

**Guides are organized around user goals, not source modules.** "Enforce Row-Level DML
Rules," not "The RuleVerifier Class." If you're naming a guide after a Scala class or
file, stop and rename it after the thing a user is trying to accomplish.

## Do not create empty or speculative pages

Every page in this site was written because the underlying feature exists and was
verified against the actual codebase (source, tests, docs/, or a real command run) at the
time of writing. Don't add a placeholder page "for completeness," and don't document a
planned or roadmap feature as if it exists today — check `ROADMAP.md` if you're unsure
whether something has shipped. A stub page that says "coming soon" is worse than no page:
it wastes a reader's click and erodes trust in every other page on the site.

## Writing style

- **Clear, concise, task-oriented.** "Install the enforcement rule at session
  construction," not "In order to enable enforcement functionality, it is necessary
  to...".
- **Short paragraphs, real commands, expected output.** If a command produces useful
  output, show it — verbatim, from a real run or a real source file, not invented.
- **Second person, active voice.** "You declare an output" not "outputs are declared."
- **No marketing language.** No "powerful," "seamless," "blazing fast," "enterprise-grade."
  Say what it does; let that speak for itself.
- **No internal jargon without introduction.** "Catalyst logical plan," "IR," "check
  rule" are all fine *after* they've been introduced in context — don't assume a reader
  arriving on a Reference page has read every Concept page first, so a term used across
  pages should be linked to where it's explained, not just used cold.
- **Don't restate source code.** A doc page should explain what a user does and what
  happens as a result — not narrate `if`/`else` branches. If you're tempted to describe
  control flow, you're writing a code comment, not a doc page.

## Markdown vs. MDX

- Default to plain Markdown (`.md`). Most pages need nothing else.
- Use MDX (`.mdx`) only when the page needs a Starlight component (`Tabs`, `Steps`,
  `Card`/`CardGrid`, `Aside`) or actual interactivity. `index.mdx` is MDX because it uses
  `CardGrid`; most guide/reference/concept pages don't need to be.
- Don't convert a page to MDX just because it *could* use a component — only when a
  component genuinely improves the reader's experience over a heading and a paragraph.

**A `.md` file with an `import {...}` line and `<Steps>`/`<Aside>`/`<Tabs>` tags is a real,
silent bug, not a style choice** — this shipped once and reached production before being
caught. Astro's MDX compiler only processes `.mdx` files; in a `.md` file, `import ...` is
just inert text and gets rendered on the page verbatim, and the component tags render as
nothing (unrecognized custom elements). No build error, no warning — it looks correct in
the source and is visibly broken only once rendered. If you add or copy a Starlight
component into a page, **the file extension must be `.mdx`**, full stop; there is no
"just wrap it and keep `.md`" option. After renaming, verify with a real build, not just a
visual glance at the source: `npm run build` then confirm the output is clean —
`grep -rl "import {.*} from" dist --include="*.html"` must return nothing, and the
component's real CSS class should be present (e.g. `grep -c 'sl-steps' dist/<page>/index.html`
for `Steps`, `starlight-aside` for `Aside`, `tablist-wrapper` for `Tabs`). Also clear
`node_modules/.astro/` (a content-layer cache separate from `dist/` and the project-root
`.astro/`, and *not* removed by a normal rebuild) before that verification build if you've
just renamed a file — a stale entry there can keep serving the old, broken parse of a file
that's already been fixed on disk, making the bug look unfixed when it's actually cache.

## When to use which Starlight component

- **`Steps`** — a sequential procedure where order matters and the reader is doing
  something at each step (install → configure → run). Don't use it for an unordered list
  of options.
- **`Tabs`** — genuinely equivalent alternative paths to the same result (Codespaces vs.
  local install; Docker vs. local regression pack). Don't use tabs to hide content that
  every reader actually needs to see.
- **`Card`/`CardGrid`** — landing-page navigation and "pick one of these" choices. Don't
  use them mid-page as a substitute for a bulleted list.
- **`Aside`** (`note`/`tip`/`caution`/`danger`) — a genuine warning, prerequisite, or
  behavior a reader would otherwise misunderstand. Don't use an aside for information
  that belongs in the main flow — if every paragraph is an aside, none of them are.

Keep the page's plain prose doing most of the work. A page that's all components and no
sentences is harder to scan, not easier.

## Naming conventions

- File names: lowercase, hyphen-separated, matching the page's URL slug
  (`writing-a-contract.md`, not `WritingAContract.md` or `writing_a_contract.md`).
- Frontmatter `title`: a short noun phrase or imperative ("Write a Contract"), matching
  how it should read in the sidebar and in search results — not a restatement of the
  slug with different capitalization.
- Frontmatter `description`: one sentence, written for a search-engine result snippet and
  the sidebar tooltip — what the page covers, not marketing copy.
- Every content page sets `sidebar.order` within its section so navigation order is
  explicit and doesn't depend on alphabetical file naming surviving future additions.

## How to structure a guide

1. One sentence: what the reader will accomplish.
2. Prerequisites, if any (link to Getting Started rather than repeating install steps).
3. The steps themselves — `Steps` component if genuinely sequential, headings otherwise.
4. What to check afterward (expected output, how to confirm it worked).
5. A short "what's next" pointing to the next logical page (another guide, a concept, or
   reference detail) — every guide should leave the reader knowing where to go, not
   dead-ending.

## How to write an example

**Every command, config snippet, and piece of output in this site must be real** —
derived from an actual test, an actual source file, or an actual run of the actual
`dev/` scripts. Do not invent a command, a config key, or sample output. If you can't
verify something against the repository, don't include it — flag it as a gap instead of
guessing (see "Documentation gaps" note below).

When adding a new example:

1. Find the real thing it's based on — a test file, a fixture, an actual run's console
   output, a real source file's public API.
2. Quote it faithfully. Trim for length if needed, but don't alter behavior or invent
   fields that aren't really there.
3. If you can run it (e.g. via `./dev/test`, `./dev/regression`, or a `spark-submit`
   invocation), do so and paste the actual output rather than reconstructing it from
   memory.

## What belongs in user documentation vs. elsewhere

**Belongs here (`docs-site/`):**

- How to install, configure, and use Invaract's public surface: the contract format, the
  enforcement rule, the `dev/` scripts, violation types, connector support.
- Concepts a user needs to use the product correctly (verification vs. enforcement,
  fail-closed policy) — even though these concepts are implemented in Scala, the
  *behavior* they produce is user-facing.

**Belongs in the repository's own `docs/` (module design docs), not here:**

- How `SparkPlanAdapter` is implemented internally, Catalyst plan shapes, empirical
  findings from probing Spark's internals, mutation testing methodology, MiMa binary
  compatibility mechanics, per-connector operation/feature coverage ledgers (the
  `add-spark-connector` skill's output).
- Anything a contributor needs to modify Invaract's own code, but a user of Invaract
  never needs to know.

**Belongs in `ARCHITECTURE.md`/`ROADMAP.md`, not here:**

- System design rationale, ADRs, phase-by-phase project status, what's planned but not
  yet built.

If you're unsure which side of this line something falls on, ask: "would a user who never
opens this repository's source need to know this to use Invaract correctly?" Yes → here.
No → the developer docs.

## Quality bar before publishing a change

- [ ] Every command/output/config example is real, not invented.
- [ ] No feature is described that doesn't exist yet (check `ROADMAP.md` if unsure).
- [ ] The page fits the information architecture above without straining.
- [ ] Internal links use the site's base path (`/Invaract/...`) and point to a real page.
- [ ] `npm run build` succeeds from `docs-site/`.
- [ ] New/changed content reads correctly for someone with zero prior context on the
      repository — not just someone who already knows what the feature does.

## Screenshots and other assets

`public/images/` is the home for screenshots, diagrams, and other visual assets — currently
empty, since no useful screenshots exist yet for this stage of the project (see
`ROADMAP.md`; the results web UI is the most likely future source of one). Don't fabricate
a screenshot — if a page would benefit from one, note the gap rather than faking it.
