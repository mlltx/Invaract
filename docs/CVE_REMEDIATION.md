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
don't find *unsafe usage* of an otherwise-fine version, so it's worth
checking dependency call sites for this pattern independent of the alert
backlog. One example investigated while writing this doc, not from a
Dependabot alert: `ContractParser.scala` parsed contract YAML via
`new Yaml().load(...)` — SnakeYAML's default `Constructor` rather than
`SafeConstructor`.

**Important correction, found by actually testing it rather than assuming
from the class name**: this is *not* a live exploitable gap in the version
pinned here. SnakeYAML 2.2's `Yaml.load` rejects `!!`-tag-directed class
resolution during composing by default (a `TagInspector` check added
library-wide as the real fix for CVE-2022-1471) — confirmed empirically:
both the default `Constructor` and `SafeConstructor` throw the identical
`ComposerException: Global tag is not allowed: ...` for a
`!!java.net.URL`-tagged document, because the check happens in the shared
Composer stage, before either constructor's own class-resolution logic
runs at all. No input distinguishes the two loaders for this vector on
2.2. `ContractParser` was switched to `SafeConstructor` anyway (see its
`newSafeYaml` comment) as defense-in-depth — a positive allowlist that
doesn't depend on that `TagInspector` default staying in force — not
because the prior code was actively exploitable. Verified via the
`contract` module's full test suite (48/48, unchanged) and
`sbt mimaReportBinaryIssues` (clean; the change is a private method) run
directly in-session; `./dev/test`'s real-Spark run was not — this
environment has no Spark distribution installed, and pulling one down
solely to validate a private-method change scoped entirely to `contract`
was judged disproportionate. CI's own `test.yml` matrix runs it on push.

## 7. Worked example: the 8 critical alerts

GitHub's 8 critical Dependabot alerts, worked end-to-end per §4/§5's
process, produced two real lessons worth keeping alongside the process
itself — both found by actually running the tests, not by reading the
advisory and assuming a version bump is safe:

| Artifact | Module(s) | Before | After | CVE |
|---|---|---|---|---|
| `org.apache.avro:avro` | `plugin`, `runner`, `spark-adapter` | 1.11.2 | 1.11.4 | CVE-2024-47561 |
| `org.apache.zookeeper:zookeeper` | `plugin`, `runner`, `spark-adapter` | 3.6.3 | 3.9.2 | CVE-2023-44981 |
| `org.codehaus.jackson:jackson-mapper-asl` | `spark-adapter` | 1.9.13 | excluded (no fix exists) | CVE-2019-10202 |
| `org.apache.derby:derby` | `spark-adapter` | 10.14.2.0 | **not changed** — accepted risk | CVE-2022-46337 |

**A "safe" transitive bump broke something two hops away.** The
ZooKeeper 3.6.3 → 3.9.2 bump pulls a newer Netty (4.1.105.Final for 9
`io.netty` artifacts) that Ivy doesn't cleanly evict against the rest of
the tree's 4.1.96.Final — both versions end up on the classpath. Every
`ClickHouseConnectorSpec` write test started failing with
`NoSuchFieldError: Class io.netty.buffer.PoolArena does not have member
field 'int chunkSize'`, because `arrow-memory-netty` (already pinned
in `spark-adapter/build.sbt` for the JDK 21 fix) reflects into
Netty-internal fields that only exist in the specific version it was
validated against. Fixed by explicitly pinning those 9 `io.netty`
coordinates back to 4.1.96.Final alongside the ZooKeeper bump — running
only the two modules that actually declare Delta/Iceberg/Hive/ClickHouse
as test deps (`plugin`/`runner` don't touch Arrow at all, and their own
test suites passed with the ZooKeeper bump alone) would have missed this
entirely. This is exactly why §5 says to run the real test suite after
every bump, not just trust that a "transitive-only" dependency can't
affect anything else on the classpath.

**Sometimes there really is no fix, even when a newer artifact exists.**
Derby 10.17.1.0 is the only version on Maven Central that actually fixes
CVE-2022-46337 (the advisory's other named fixed releases —
10.14.3/10.15.2.1/10.16.1.2 — were apparently never published; see
[DERBY-7178](https://issues.apache.org/jira/browse/DERBY-7178)). It was
tried anyway, JDK 21+-only (Derby's own release notes say 10.17 doesn't
support Java below 21) — and it broke `HiveConnectorSpec` outright:
`unzip -l` on the actual jar shows Derby restructured its packaging
between these releases, and 10.17.1.0 no longer contains
`org/apache/derby/jdbc/EmbeddedDriver.class` at all, while Hive 2.3.9's
own metastore code hardcodes that exact class name. There is no version
of Derby that is simultaneously CVE-2022-46337-fixed and compatible with
this Hive version's metastore client. This is left as a **documented
accepted risk** (see `spark-adapter/build.sbt`'s comment at the
`dependencyOverrides` block) rather than forced through: the mitigating
factor is that `HiveConnectorSpec`'s embedded-metastore JDBC URL
configures no LDAP authenticator at all, so the specific vulnerable code
path is never reachable through this module's own tests regardless of
version — but the alert itself stays open until Hive's own metastore
client moves off this Derby generation, which isn't something a
dependency override can fix.

Verification for all four: `spark-adapter`'s full suite (286/286,
twice — once to catch the Netty regression, once clean after fixing it),
`plugin`'s suite (4/4), `sbt mimaReportBinaryIssues` skipped for
`spark-adapter` specifically since the diff is 100% `build.sbt` (no
`.scala` touched, so binary API cannot have changed) but run for
`contract` per §6, and the actual shipped jars inspected directly
(`unzip -l`/`dependencyTree`) rather than trusting the build log — the
`invaract-spark-runner.jar` DemoJobHarness assembles resolves
`avro:1.11.4`/`zookeeper:3.9.2` for real, and `jackson-mapper-asl` is
confirmed absent (`0` matches) from the assembled `spark-adapter` jar.

## 8. Next steps checklist

- [x] Add `.github/dependabot.yml` for `web`, `docs-site`, `github-actions`
      (this change).
- [x] Switch `ContractParser` to `SafeConstructor` as defense-in-depth
      (this change) — see §6's correction for what this does and doesn't
      fix.
- [x] Fix the 8 critical alerts (this change) — see §7 for the worked
      example, including the one left as an accepted risk.
- [ ] Triage the remaining alerts (97 high / 121 moderate / 12 low) into
      the buckets in §2; bucket-1 (runtime/compile-scope engine) and
      bucket-5 (Actions) first.
- [ ] Walk the rest of the Scala/Maven bucket per §4's manual workflow,
      batched per §5 — one coordinate (or tightly-related group, per §7's
      Netty lesson) at a time, real test suite run after each.
- [ ] Fix or explicitly document-and-accept every alert with no available
      patched version, per §3 — Derby (§7) is the template for how to
      document one.
- [ ] Once the backlog is current, treat "zero unaddressed alerts older than
      the SLA in §3" as the steady-state target, not "zero alerts" — new
      ones will always arrive with new versions of Spark's own dependency
      tree.
