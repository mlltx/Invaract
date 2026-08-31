# CVE / Dependency Vulnerability Remediation

This document is the developer-facing process for triaging and fixing the
security alerts GitHub reports against this repository's dependencies (as of
this writing, 238 open Dependabot alerts, accumulated with **no automated
remediation path**, since no `.github/dependabot.yml` existed until this
change). It complements — and is subordinate to — the correctness gates
already required by [CLAUDE.md](../CLAUDE.md): a dependency bump is a code
change like any other, and every gate that applies to a feature PR touching
`contract`/`ir`/`spark-adapter` applies to a CVE-fix PR touching the same
code.

## 1. How alerts get captured

GitHub's Dependabot alerts come from two independent pipelines in this repo,
and they support very different remediation paths:

| Ecosystem | How GitHub sees it | Automated fix PRs? |
|---|---|---|
| Scala/Java deps (`contract`, `ir`, `spark-adapter`, `plugin`, `runner`) | Submitted as `maven`-ecosystem entries by [`dependency-graph.yml`](../.github/workflows/dependency-graph.yml) (`scalacenter/sbt-dependency-submission`), on every push to `main`. Without this workflow the graph would be empty and Dependabot would see zero Scala dependencies. | **No.** Dependabot has no `build.sbt` updater — it can *see* vulnerable coordinates via the submitted graph, but cannot open a version-bump PR for them. These alerts are manual-only. |
| `web/` npm deps | `web/package-lock.json`, parsed natively. | Yes, once `dependabot.yml` is configured (see §4). |
| `docs-site/` npm deps | `docs-site/package-lock.json`, parsed natively. | Yes, same. |
| GitHub Actions pins (`.github/workflows/*.yml`) | Parsed natively. | Yes, same. |

This asymmetry is almost certainly why the count is 238: Spark's own
dependency tree (Hadoop client, Jackson, Netty, Guava, Avro, Arrow, Jetty,
commons-*, etc.) is large, `spark-adapter` alone pulls in Delta, Iceberg,
Hive, and ClickHouse connector runtimes for testing, and none of it had any
outlet before now. Treat the raw 238 as a starting inventory, not a
prioritized list — most of it needs bucketing before it needs fixing (§2).

## 2. Triage: bucket by blast radius before by severity

CVSS severity alone is misleading here because most of this dependency
surface is deliberately **not shipped**. Reuse the product-vs-harness
distinction CLAUDE.md already draws, plus Scala's `provided`/`test` scope,
to decide how urgently each alert actually matters:

1. **Runtime/compile-scope, in the shipped engine** — highest priority.
   Concretely: `contract`'s `snakeyaml` and `runner`'s compile-scope
   `spark-core`/`spark-sql` (runner is the one module that pulls Spark in
   unscoped, since it's the thing that actually runs `spark-submit`).
   A real Invaract user's classpath includes these.
2. **`provided`-scope in the engine** (`spark-core`/`spark-sql` in
   `spark-adapter` and `plugin`) — real, but the vulnerable jar is supplied
   by whatever cluster the user deploys to, not by this repo's artifact.
   Still worth tracking (it constrains which Spark versions we can honestly
   claim to support), but it's not something *this repo's* release ships.
3. **`test`-scope in `contract`/`ir`/`spark-adapter`** (`scalatest`,
   `json-schema-validator`, Delta/Iceberg/Hive/Avro/ClickHouse connector
   runtimes, H2, the Arrow `dependencyOverrides`) — CI/dev-machine attack
   surface only. Real, but lowest urgency of the Scala tier: fix on a normal
   cadence, don't treat as an incident.
4. **`web/` and `docs-site/` npm deps** — these are the demo results viewer
   and the docs site, not the verification engine (see CLAUDE.md's "What's
   the product" section). Prioritize normally, but a CVE here never blocks
   an engine release.
5. **GitHub Actions pins** — CI supply-chain risk (a compromised action
   version could exfiltrate secrets or tamper with a build). Treat like
   category 1 despite being "just tooling" — this is the one category where
   underestimating harness/CI status is a real mistake.

Record which bucket an alert falls into when triaging it; it's most of the
prioritization decision.

## 3. Reporting

- **System of record**: the repo's Security tab (Dependabot alerts) stays
  authoritative — don't fork the list into a spreadsheet that will drift.
  Use labels/milestones on the PRs that close each alert (Dependabot does
  this automatically for npm/Actions PRs it opens).
- **Cadence**: triage new alerts weekly (Dependabot's own PRs surface on
  their configured schedule — see §4). For the manual Scala bucket, do a
  standing review pass at least monthly, since nothing else will prompt it.
- **Accepted-risk exceptions**: some alerts will have no available fix (a
  connector runtime pinned to the only version compatible with this Spark
  build, an unfixed transitive CVE with no patched release yet). Don't let
  these sit as silent unaddressed alerts — document them explicitly, the
  same way `mimaBinaryIssueFilters` requires a documented reason for every
  accepted MiMa break (see `spark-adapter/build.sbt`'s comment style). A
  short comment at the point of the pin (why, what CVE, what's blocking the
  fix, when to re-check) is enough; it's what already happens for every
  other "pinned for a reason" dependency in this repo's `build.sbt` files.
- **Escalation**: a critical/high alert in bucket 1 or 5 (§2) that has an
  available fix should not wait for the weekly/monthly cadence — fix it as
  soon as it's triaged.

## 4. Remediation workflow, by ecosystem

### npm (`web/`, `docs-site/`) and GitHub Actions — now automated

`.github/dependabot.yml` (added alongside this document) configures weekly
version-update PRs for these three ecosystems, grouped by minor/patch so
routine bumps land as one PR instead of dozens, while majors (a Next.js or
Astro major, especially) still arrive as their own PR for manual review —
those can carry real breaking changes and shouldn't be batch-merged blind.
Security-relevant updates Dependabot opens outside that schedule (its normal
behavior for alerts with a known severity) are not blocked by the grouping.

These PRs go through the exact same `pull_request`-triggered CI as any other
PR (`test.yml`'s full matrix, plus `docker-regression`) — nothing extra to
configure, since `web`/`docs-site` changes don't touch the mutation-testing
or MiMa jobs (those only run for `ir`/`spark-adapter`). `docs-site` changes
specifically must still pass `cd docs-site && npm run build`
(`deploy-docs.yml`) per CLAUDE.md's Documentation Policy — a Dependabot bump
that breaks the Starlight build is exactly what that gate exists to catch.

### Scala/Maven (`contract`, `ir`, `spark-adapter`, `plugin`, `runner`) — manual

No tool opens these PRs for you; work the alert list in the Security tab by
hand, one coordinate at a time:

1. **Identify whether the vulnerable jar is a direct or transitive
   dependency.** `sbt evicted` / `sbt dependencyTree` from inside the
   module shows the path.
2. **Direct dependency** (e.g. `contract`'s `snakeyaml`,
   `json-schema-validator`) → bump the version in that module's
   `libraryDependencies` directly.
3. **Transitive-only dependency** (most of `spark-adapter`'s alerts will be
   this — Jackson/Netty/Guava/etc. pulled in by Spark or a connector
   runtime, not declared by this repo) → prefer a `dependencyOverrides`
   entry over bumping the thing that pulls it in. `spark-adapter/build.sbt`
   already does exactly this for Arrow (bumped to 14.0.1 to fix a real JDK
   21 incompatibility in Spark's bundled 12.0.1) — same pattern, same file,
   for a CVE-motivated override. Document the CVE and the constraint the
   same way that comment documents the JDK issue.
4. **Do not bump Spark itself (3.5.1) to chase a transitive CVE.** It's
   pinned deliberately (see CLAUDE.md's Versions table and
   `docs/SPARK_ADAPTER.md`) and a Spark version bump is a project-level
   decision with its own compatibility survey, not a dependency patch — see
   `docs/ADDING_A_SPARK_CONNECTOR.md`'s Phase 0 process for the kind of
   investigation a version change of this size actually requires. Use
   `dependencyOverrides` for the transitive jar instead, as in step 3.
5. **Test-scope connector runtimes** (Delta/Iceberg/Hive/Avro/ClickHouse/H2)
   → same direct-vs-transitive logic, but confirm a newer version is still
   compatible with Spark 3.5.1/Scala 2.12 before bumping — every one of
   these versions in `spark-adapter/build.sbt` was pinned after checking the
   connector's own issue tracker for exactly this combination (see that
   file's extensive comments). A CVE fix that reintroduces one of those
   already-solved compatibility bugs is not a fix.

## 5. Guardrails: remediating without introducing new issues

This is the part of the ask that matters most, and the repo already has the
machinery for it — the same machinery CLAUDE.md requires for any change to
`contract`/`ir`/`spark-adapter`:

- **Batch by module and ecosystem, not by "the CVE backlog."** One
  coordinate bump (or one `dependencyOverrides` addition) per commit/PR.
  A single PR that bumps ten unrelated jars across three modules makes it
  impossible to tell which bump caused a downstream test failure or a MiMa
  break — and with 238 alerts, some bumps *will* cause exactly that.
- **`ir`/`spark-adapter` bumps go through the same bar as any code change.**
  Even though a dependency-version edit isn't hand-written logic, it can
  change runtime behavior your existing tests exercise. Run `sbt test` in
  the module; if the bump is anything beyond a trivial patch version, treat
  it as touching that module for the purposes of CLAUDE.md's Mutation
  Testing Requirement and run a scoped Stryker pass, since a transitive
  library behavior change can flip what your existing tests actually
  exercise even though you didn't touch a `.scala` file yourself.
- **Run `sbt mimaReportBinaryIssues` before pushing any `contract`/`ir`/
  `spark-adapter` bump.** A transitive version bump is exactly the kind of
  change CLAUDE.md's API Compatibility Requirement exists to catch when it
  isn't obviously safe — e.g. a `snakeyaml` bump that changes a type
  `ContractParser`'s public signatures expose, or a Spark minor-version jar
  swap that changes an inferred return type at a public boundary.
- **`./dev/test` is mandatory for anything touching `contract`/`ir`/
  `spark-adapter`/`plugin`/`runner`**, per CLAUDE.md's Critical Requirement
  — unit tests passing is not evidence the engine still verifies anything
  inside a real Spark job. Run it after every such bump, not just at the
  end of a batch.
- **`./dev/regression` for anything that could plausibly affect
  enforcement** — a Jackson/Netty/etc. override that changes serialization
  or reflection behavior is exactly the kind of "looks unrelated, isn't"
  change that could silently defang `ContractEnforcementRule` without
  breaking a single unit test.
- **CI's `summary` gate already covers all of this for every PR**,
  Dependabot's included, since Dependabot PRs are ordinary
  `pull_request`-triggered PRs — `test.yml`'s matrix, `docker-regression`,
  `mutation-testing`, and `api-compatibility` all run. Don't bypass or
  auto-merge Dependabot PRs against `contract`/`ir`/`spark-adapter`-adjacent
  ecosystems (there are none configured yet — see §4 — but if `maven`/sbt
  support is ever added here, this matters); auto-merge is reasonable for
  the npm/Actions minor/patch groups once `summary` is green, manual review
  otherwise.

## 6. What Dependabot alerts won't catch

Dependency version alerts only find *known-vulnerable versions* — they
don't find *unsafe usage* of an otherwise-fine version. One concrete example
already present in this codebase, found while writing this doc, not from a
Dependabot alert: `ContractParser.scala` parses contract YAML via
`new Yaml().load(...)` — SnakeYAML's default `Constructor`, not
`SafeConstructor`/`SafeLoader`. That constructor honors YAML type tags and
can be used to instantiate arbitrary Java classes from crafted input,
independent of which SnakeYAML version is installed; bumping `snakeyaml`
for an unrelated CVE will not change this. Legitimate contract documents
never need custom-tag deserialization (they're maps/lists/scalars — see
`docs/CONTRACT_MODEL.md`'s "Contract Document Shape"), so switching to a
safe loader should be behavior-preserving for every real contract, but it
is a separate, deliberate fix — not a version bump — and should go through
`ContractParser`'s own test suite plus the MiMa/`./dev/test` gates in §5
like any other `contract` change. Flagging it here rather than folding it
into this remediation pass, since it's a code fix, not a dependency fix.

## 7. Next steps checklist

- [x] Add `.github/dependabot.yml` for `web`, `docs-site`, `github-actions`
      (this change).
- [ ] Triage the 238 open alerts into the buckets in §2; file the bucket-1
      (runtime/compile-scope engine) and bucket-5 (Actions) alerts as
      near-term work first.
- [ ] Walk the Scala/Maven bucket per §4's manual workflow, batched per §5.
- [ ] Fix or explicitly document-and-accept every alert with no available
      patched version, per §3.
- [ ] Evaluate the `ContractParser` SafeConstructor fix (§6) as its own PR.
- [ ] Once the backlog is current, treat "zero unaddressed alerts older than
      the SLA in §3" as the steady-state target, not "zero alerts" — new
      ones will always arrive with new versions of Spark's own dependency
      tree.
