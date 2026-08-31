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

## 2. Triage: would a downstream user actually inherit this?

The single most important question for any alert against `contract`, `ir`,
or `spark-adapter` — the three modules actually published as Maven
artifacts (`com.example %% invaract-contract/invaract-ir/invaract-spark-adapter`,
per each module's `mimaPreviousArtifacts`; see CLAUDE.md's "What's the
product" section) — is **not** severity, and not even "is it shipped
somewhere." It's: *if a real user adds one of these three coordinates as a
dependency in their own build, does Maven/Ivy's own resolution actually
pull the vulnerable jar onto their classpath?* That's determined entirely
by which Ivy/Maven **scope** the dependency was declared with, and only
one scope answers yes:

- **`compile` (the default — unscoped in `libraryDependencies`)**
  propagates transitively to every downstream consumer. This is the
  *only* bucket where "our users would inherit the CVE" is literally
  true, and it should outrank every other prioritization signal,
  including CVSS severity: a Moderate alert here matters more than a
  Critical one in the buckets below.
- **`% "provided"`** appears in the published POM (confirmed empirically —
  `contract`'s generated POM correctly emits `<scope>test</scope>` for
  its test deps, so the sbt→POM scope mapping is real and trustworthy)
  but Maven's own resolution rules make `provided` **non-transitive by
  definition** — a consumer does not inherit it through us. In practice
  it's supplied by whatever Spark cluster the user already deploys to,
  which is a real vulnerability surface, but it's *theirs*, sourced from
  their own Spark install, not delivered by our artifact. Track it (it
  constrains which Spark versions we can honestly claim to support), but
  don't rank it as "our users inherit our CVE."
- **`% "test"`** is excluded from the published POM entirely — not
  merely non-transitive, genuinely absent from the dependency list a
  consumer ever sees. Zero inheritance risk, by construction. This is
  pure CI/dev-machine attack surface (see bucket 3 below).

**How to check, concretely**: look at the scope in that module's
`build.sbt` (`libraryDependencies`, unscoped = compile), or run
`sbt publishLocal` and read the generated `.pom`'s `<scope>` tags
directly — don't infer from the advisory or from "is it declared in this
file," since `provided`/`test` both *are* declared and both still fail
to reach a consumer.

**What this means concretely for this repo today**: walking
`contract`/`ir`/`spark-adapter`'s own `build.sbt` files, the *entire*
externally-facing (compile-scope) dependency surface across all three
published artifacts is a single coordinate — `org.yaml:snakeyaml:2.2` in
`contract`. `ir` declares no external compile dependency at all.
`spark-adapter` declares zero compile-scope dependencies of its own —
`spark-core`/`spark-sql` are `% "provided"` there, and everything else
(Delta/Iceberg/Hive/Avro/ClickHouse/H2) is `% "test"`. So the practical
triage rule for this repo: **check `snakeyaml` first, every time**, before
touching anything else in the Scala/Maven bucket — it's the one place a
CVE would genuinely travel into a real user's build.

One more distinction worth being explicit about, since it's easy to
conflate: `plugin` and `runner` are never consumed as a Maven dependency
by anyone (see CLAUDE.md's "What's the product" section — they're the
demo transformation and the demo job harness, not libraries). It doesn't
matter what scope *they* declare something at — nobody's `build.sbt`
ever writes `"com.example" %% "invaract-spark-runner" % "..."`. A CVE in
`runner`'s compile-scope Spark tree (it's the one module that pulls Spark
in unscoped, since it actually runs `spark-submit`) matters only for
whoever runs *that specific assembled jar* — this repo's own `./dev/test`
demo, or a real regression per `./dev/regression` — not for "a real
Invaract user" in the library-consumer sense. Keep those two audiences
separate; §7 shows a worked case where this distinction changed the
actual priority.

### Everything else: bucket by blast radius on our own CI/dev machine

Once the externally-facing check above clears an alert (or confirms it's
`compile`-scope and genuinely urgent), the rest is about limiting how much
time gets spent on lower-stakes alerts — CVSS severity still isn't the
whole story, since most of this remaining surface is deliberately not
shipped to anyone:

1. **`spark-adapter`'s `snakeyaml`-equivalent**: n/a today (see above —
   this bucket is empty for `spark-adapter`/`ir` right now, and 1
   coordinate for `contract`). Kept as its own numbered bucket so a
   future *new* compile-scope dependency lands here automatically.
2. **`provided`-scope in `contract`/`ir`/`spark-adapter`/`plugin`**
   (`spark-core`/`spark-sql`) — real, but non-transitive per the
   mechanism above; track for "which Spark versions can we honestly
   support," not as user-facing urgency.
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
   bucket 1 despite being "just tooling" — this is the one category where
   underestimating harness/CI status is a real mistake.

Record both which bucket an alert falls into *and* whether it clears the
"would a downstream user inherit this" check above — together they're
most of the prioritization decision.

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
process, produced three real lessons worth keeping alongside the process
itself — found by actually running the tests and actually checking Maven
scope, not by reading the advisory and assuming a version bump is safe
or that "critical" means "our users are exposed":

| Artifact | Module(s) | Scope | Downstream-inherited? (§2) | Before | After | CVE |
|---|---|---|---|---|---|---|
| `org.apache.avro:avro` | `plugin`, `runner`, `spark-adapter` | `provided` (`plugin`/`spark-adapter`), compile (`runner`) | **No** via `plugin`/`spark-adapter` (non-transitive); n/a via `runner` (never a dependency) | 1.11.2 | 1.11.4 | CVE-2024-47561 |
| `org.apache.zookeeper:zookeeper` | `plugin`, `runner`, `spark-adapter` | same as above | same as above | 3.6.3 | 3.9.2 | CVE-2023-44981 |
| `org.codehaus.jackson:jackson-mapper-asl` | `spark-adapter` | `test` (via `spark-hive`) | **No** — excluded from the published POM entirely | 1.9.13 | excluded (no fix exists) | CVE-2019-10202 |
| `org.apache.derby:derby` | `spark-adapter` | `test` (via `spark-hive`) | **No** — same as above | 10.14.2.0 | **not changed** — accepted risk | CVE-2022-46337 |

**None of the 8 critical alerts were in the externally-facing (compile-scope,
`contract`/`ir`/`spark-adapter`) bucket §2 says to check first** — every one
was `provided` or `test`, confirmed empirically, not assumed: `unzip -l` on
`plugin`'s and `spark-adapter`'s own assembled jars shows zero `avro`/
`zookeeper`/`spark` classes bundled (`provided` genuinely isn't in the
jar, let alone propagated to a consumer), while `runner`'s assembled jar
has 13,230 such classes — but `runner` is never a Maven dependency of
anyone's. So a real user who depends on `com.example %% invaract-spark-adapter`
today would not have inherited a single one of these 8 criticals through
us, regardless of severity. That doesn't make the work worthless — Delta/
Iceberg/Hive/ClickHouse-dependent tests, `./dev/test`'s demo run, and this
repo's own CI all exercise these versions for real — but it does mean the
*next* alert to prioritize is whatever's flagged against `snakeyaml` (§2's
one true externally-facing coordinate today), not whatever GitHub happens
to label Critical elsewhere in the tree.

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

## 7a. Worked example: a high-severity batch, and a three-deep regression chain

The next batch — a mix of high-severity alerts across `plugin`/`runner`/
`spark-adapter` — is the clearest illustration yet of why §5 insists on
running the real suite after every bump, not trusting that a fix is safe
because the reasoning sounds right. One coordinate bump here triggered a
chain of three distinct regressions, each only visible by actually
running `spark-adapter`'s full suite and reading the real exception —
never by inspecting the diff or trusting the previous fix's success.

| Artifact | Module(s) | Before | After | CVE(s) |
|---|---|---|---|---|
| `org.apache.avro:avro` (already fixed, unaffected by this batch) | — | — | — | — |
| `org.codehaus.jackson:jackson-mapper-asl` (XXE, #34) | `spark-adapter` | already excluded | no change needed | CVE unspecified — same artifact already excluded entirely in the critical-alert pass |
| `com.google.protobuf:protobuf-java` (#205/#131/#52) | all three | already 3.19.6 (fixed) | no change needed | CVE-2024-7254 — vulnerable `2.5.0` node present but evicted, confirmed via `dependencyTree` |
| `commons-io:commons-io` (#206/#132/#53) | all three | already 2.16.1 (fixed) | no change needed | CVE-2024-47554 (fixed 2.14.0) — same eviction story |
| `org.apache.ivy:ivy` (#191/#117/#38) | all three | 2.5.1 | 2.5.2 | CVE-2022-46751 (XXE) |
| `io.netty:*` (16 artifacts, #223/#149/#73, #208/#134/#57, + more) | all three | 4.1.96.Final | 4.1.132.Final | CVE-2025-24970 (SslHandler), CVE-2026-33871 (HTTP/2 CONTINUATION flood) |
| `org.apache.arrow:arrow-{vector,memory-core,memory-netty}` (not itself alerted — broke as a side effect) | `spark-adapter` | 14.0.1 | 17.0.0 | n/a — required by the Netty bump above, not a CVE fix in its own right |
| `com.fasterxml.jackson.core:{jackson-core,jackson-databind,jackson-annotations}` (not itself alerted — broke as a side effect) | `spark-adapter` | Spark's own 2.15.2 | pinned back to 2.15.2 | n/a — Arrow 17.0.0 tried to pull 2.17.1; pinned back down |
| `org.apache.thrift:libthrift` (#35/#36) | `spark-adapter` | 0.12.0 | 0.13.0 | CVE-2019-0205 fixed; CVE-2020-13949 left as accepted risk (0.14.0's fix is incompatible with Hive 2.3.9) |

**Three alerts needed zero code change.** `jackson-mapper-asl`'s XXE
alert (#34) is against the exact artifact already excluded entirely for
its earlier CVE-2019-10202 alert — one exclusion, multiple alerts closed.
`protobuf-java` and `commons-io` both already resolve, tree-wide, to
versions past their fix floor (`3.19.6` and `2.16.1` respectively); the
vulnerable nodes GitHub's graph still shows are evicted losers, never on
the real classpath. Worth checking before assuming every alert needs a
`build.sbt` edit.

**The regression chain, in the order it was actually found:**

1. **Netty 4.1.96.Final → 4.1.132.Final alone**: broke `ClickHouseConnectorSpec`
   with the identical `NoSuchFieldError: ... PoolArena ... 'int chunkSize'`
   from §7's Arrow lesson — except this time pinning Netty back to
   4.1.96.Final wasn't an option, since 4.1.96.Final is exactly what's
   vulnerable. Root-caused properly this time instead of just reverting:
   Netty's own PR #13613 restructured `PoolArena` (moved `chunkSize` out
   of it, into a `SizeClasses` field) somewhere around Netty 4.1.7x —
   meaning `arrow-memory-netty:14.0.1` was never going to survive *any*
   Netty version modern enough to carry these CVE fixes, independent of
   which one got picked. Confirmed against Arrow's own issue tracker
   (apache/arrow#36713, apache/arrow#39265), which points at Arrow 17.0.0
   as the version that fixed this on Arrow's side.
2. **Arrow 14.0.1 → 17.0.0 fixed that — and broke almost everything else.**
   245→286 tests now failing in 15 seconds (a real run takes minutes),
   the signature of a startup-level break, not scattered test failures:
   `JsonMappingException: Scala module 2.15.2 requires Jackson Databind
   version >= 2.15.0 and < 2.16.0 - Found jackson-databind version
   2.17.1`. Arrow 17.0.0's own dependency management pulls a newer
   Jackson (2.17.1) that wins eviction over Spark 3.5.1's own 2.15.2 —
   and Spark's `jackson-module-scala_2.12:2.15.2` (untouched, still on
   the classpath) enforces that version range in a static initializer
   that Spark's own error-formatting path (`ErrorClassesJsonReader`)
   depends on, so the break surfaced everywhere an exception got
   formatted, not just in Arrow-adjacent tests.
3. **Pinned `jackson-core`/`jackson-databind`/`jackson-annotations` back
   to 2.15.2** — what `jackson-module-scala` actually needs and what
   Spark 3.5.1 already ships — overriding Arrow's newer preference.
   `spark-adapter`'s full suite finally passed clean, 286/286, on the
   third attempt.

Each of the three attempts was verified by actually running the suite,
not by inspecting the diff or by extrapolating from the previous fix's
success — the second regression in particular (Jackson) was not
predictable from the first one's lesson (Netty/Arrow) at all; it's a
different dependency, a different mechanism, only visible by running the
real tests again after the "fix" for the first problem.

`libthrift` got the same Derby-style check as before: 0.14.2 (the
version needed for CVE-2020-13949) repackages `TFramedTransport` into a
`layered` subpackage — confirmed via `unzip -l` across every 0.1x
release, which pinpointed 0.14.0 as exactly where the move happens —
while Hive 2.3.9's compiled code references the pre-0.14.0 package by
name. 0.13.0 fixes CVE-2019-0205 alone while keeping the old package, so
that's what shipped; CVE-2020-13949 is accepted risk, same reasoning
pattern as Derby (not reachable — `HiveConnectorSpec` runs Hive's
*embedded* metastore, never a real Thrift RPC server that could receive
the malicious-client payload this CVE describes).

## 8. Next steps checklist

- [x] Add `.github/dependabot.yml` for `web`, `docs-site`, `github-actions`
      (this change).
- [x] Switch `ContractParser` to `SafeConstructor` as defense-in-depth
      (this change) — see §6's correction for what this does and doesn't
      fix.
- [x] Fix the 8 critical alerts (this change) — see §7 for the worked
      example, including the one left as an accepted risk, and the
      finding that none of the 8 were externally-facing per §2.
- [x] **First**, every triage pass: check whether any open alert names
      `snakeyaml` (`contract`) — today's *entire* externally-facing,
      downstream-inherited surface across `contract`/`ir`/`spark-adapter`
      per §2, and the one check that outranks severity. Checked now:
      `snakeyaml 2.2` has no known CVE (the two historical SnakeYAML CVEs,
      2022-1471 and 2022-25857, were both fixed before 2.2). **As of this
      writing, zero of the 238 open alerts fall in the bucket that would
      reach a real downstream user** — re-run this check every pass, since
      that can change the moment `snakeyaml` gets bumped or a new
      compile-scope dependency is added to `contract`/`ir`/`spark-adapter`.
- [x] Fix the first high-severity batch (Ivy, Netty, libthrift; this
      change) — see §7a for the worked example, including the three-deep
      Netty→Arrow→Jackson regression chain it triggered, and
      `jackson-mapper-asl`/`protobuf-java`/`commons-io` alerts in this
      batch that were already resolved without a code change.
- [ ] Triage the remaining alerts (down from 97 high / 121 moderate / 12
      low) into the buckets in §2's "blast radius" list; bucket 1 (any
      *new* compile-scope dependency in `contract`/`ir`/`spark-adapter`)
      and bucket 5 (Actions) next after the snakeyaml check above.
- [ ] Fix the Spark History Server RCE (CVE-2025-54920, Direct dependency,
      High) — Spark 3.5.1 → 3.5.7. Deliberately held out of every batch so
      far: unlike a transitive-jar override, a Spark version bump can
      shift *many* other pinned transitive versions at once (as §7a's
      chain shows even a single-jar bump can cascade), so it needs its own
      isolated pass with its own full-suite verification before merging
      with anything else.
- [ ] Walk the rest of the Scala/Maven bucket per §4's manual workflow,
      batched per §5 — one coordinate (or tightly-related group, per §7a's
      Netty→Arrow→Jackson chain) at a time, real test suite run after
      each, no matter how confident the reasoning sounds. Record each
      one's scope/inheritance status per §2's table format (see §7/§7a).
- [ ] Fix or explicitly document-and-accept every alert with no available
      patched version, per §3 — Derby (§7) is the template for how to
      document one.
- [ ] Once the backlog is current, treat "zero unaddressed alerts older than
      the SLA in §3" as the steady-state target, not "zero alerts" — new
      ones will always arrive with new versions of Spark's own dependency
      tree.
