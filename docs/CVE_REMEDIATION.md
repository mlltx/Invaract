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
artifacts (`com.invaract %% invaract-contract/invaract-ir/invaract-spark-adapter`,
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
ever writes `"com.invaract" %% "invaract-spark-runner" % "..."`. A CVE in
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
anyone's. So a real user who depends on `com.invaract %% invaract-spark-adapter`
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

## 7b. Worked example: a second high-severity batch, a clean run, and two new accepted risks

The next batch — a mix of alerts GitHub's own scan turned up against
artifacts already touched by §7a, plus several new ones — is a useful
contrast to §7a: same modules, same kind of coordinate bumps, but no
cascading regression this time, because the bumps were smaller and closer
to what had already been proven compatible.

| Artifact | Module(s) | Before | After | CVE(s) |
|---|---|---|---|---|
| `org.apache.zookeeper:zookeeper` | all three | 3.9.2 | 3.9.5 | CVE-2026-24308 (config-value log exposure), CVE-2024-51504 (Admin Server IP-auth bypass) |
| `io.netty:*` (same 16 artifacts as §7a) | all three | 4.1.132.Final | 4.1.136.Final | CVE-2025-55163 (MadeYouReset HTTP/2 DDoS), CVE-2026-44249 (IPv6 subnet filter bypass), plus ByteBuf-leak/infinite-loop DoS bugs in `SpdyHttpDecoder`/`Bzip2Decoder` |
| `io.airlift:aircompressor` | all three | 0.25 | 0.27 | CVE-2024-36114 (unchecked `sun.misc.Unsafe` access — JVM crash / memory leak) |
| `org.apache.commons:commons-lang3` | all three | 3.12.0–3.14.0 | 3.18.0 | CVE-2025-48924 (`ClassUtils.getClass` uncontrolled recursion) |
| `org.xerial.snappy:snappy-java` | all three | 1.1.10.3 | 1.1.10.4 | CVE-2023-43642 (`SnappyInputStream` unbounded chunk-length allocation) |
| `org.lz4:lz4-java` → `at.yawk.lz4:lz4-java` | all three | `org.lz4:lz4-java:1.8.0` | `at.yawk.lz4:lz4-java:1.11.1` | CVE-2025-12183, CVE-2025-66566 |
| `org.codehaus.jackson:jackson-mapper-asl` (XXE variant) | `spark-adapter` | already excluded | no change needed | — |
| `com.google.protobuf:protobuf-java` | all three | already 3.19.6 | no change needed | — |
| `commons-lang:commons-lang` (2.x — not itself alerted, found while researching the commons-lang3 CVE) | `spark-adapter` | 2.6 | **not changed** — accepted risk | CVE-2025-48924 also affects this pre-rename 2.x line |
| `com.google.guava:guava` | all three | 16.0.1 | **not changed** — accepted risk | CVE-2018-10237 |

**Two alerts, again, needed zero code change**: the `jackson-mapper-asl`
alert this batch cites is a different CVE flavor (XXE) against the exact
artifact already excluded in §7 — one exclusion still covers it.
`protobuf-java` remains resolved at the already-fixed `3.19.6`, unchanged
since §7a.

**`lz4-java` needed an actual coordinate switch, not a version bump.**
The upstream `org.lz4:lz4-java` project is archived; no fix for either
CVE was ever published under that groupId. Confirmed directly against
Maven Central: `org.lz4:lz4-java:1.8.1`'s own POM is a real Sonatype
relocation pointing at `at.yawk.lz4:lz4-java`, the community fork that
continues shipping fixes (up through `1.11.1`, which covers both CVEs).
Rather than lean on Ivy to follow that relocation — sbt/Ivy's handling of
Maven relocation POMs has a history of being inconsistent — the old
coordinate was excluded outright and the fork added directly. Verified
concretely, not just assumed compatible: the fork kept the same
`net.jpountz.lz4` Java package namespace as the original (confirmed via
`unzip -l` on the rebuilt `invaract-spark-runner.jar` — 48 matching class
files), so Spark's own shuffle-compression code, a real code path
exercised by any shuffle stage even under `local[*]`, needed no changes
to keep working.

**Two new accepted risks, each for a different reason than Derby/libthrift's
"the fix breaks Hive" pattern:**

- **`commons-lang` (2.x)**: this is not the same artifact as
  `commons-lang3` above — the pre-rename `org.apache.commons.lang`
  package and `commons-lang3`'s `org.apache.commons.lang3` package
  coexist on the classpath and are not interchangeable, so old
  Hadoop-ecosystem code that imports the former can't simply be pointed
  at the latter. CVE-2025-48924 affects the 2.x line too (2.0–2.6, the
  same `ClassUtils.getClass` recursion bug, in the codebase
  `commons-lang3` was forked from) but no 2.x fix was ever released — the
  line is EOL, confirmed via multiple downstream trackers listing "no fix
  planned." Excluding it outright (the `jackson-mapper-asl` playbook) was
  considered and rejected here specifically because, unlike
  `jackson-mapper-asl`, this module's own source doesn't establish that
  nothing transitively needs `org.apache.commons.lang.*` — removing it
  risks a `NoClassDefFoundError` with no way to verify safety short of
  exercising every transitive code path that might reach it.
- **`guava`**: traced to its actual source rather than assumed —
  `sbt Test/dependencyTree` shows it arrives via
  `org.apache.curator:curator-client:2.13.0`, which backs Spark's
  ZooKeeper-based standalone-cluster recovery mode
  (`spark.deploy.recoveryMode=ZOOKEEPER`). CLAUDE.md's Execution Model
  has every test and the demo harness running against a `local[*]`
  master, which never touches this code at all. That's a materially
  different situation from every other override in this doc: a full
  suite pass couldn't actually *prove* a Guava bump safe here, because if
  Curator's own code (compiled against Guava 16.0.1's decade-old API
  surface) never loads under `local[*]`, an incompatibility simply
  wouldn't surface as a test failure regardless of whether it's real — a
  green run would be confirming nothing. Combined with the CVE's
  Moderate severity (§2's lowest-urgency tier), left as an accepted risk
  rather than pushed through on a test result that couldn't actually back
  it up.

**No regression this time** — `spark-adapter`'s full suite passed
286/286 on the first attempt for the ZooKeeper/Netty/aircompressor/
commons-lang3/snappy-java bumps, and again on the first attempt once the
`lz4-java` fork switch was added. The Netty delta here (4.1.132.Final →
4.1.136.Final, four patch versions) was far smaller than §7a's 96 → 132
jump that broke Arrow, which is the likely reason nothing broke — but
this was *confirmed* by running the real suite both times, not inferred
from the delta being small.

**Deliberately not touched in this batch**, each held as its own future,
isolated pass given how much a single Netty/Arrow bump already cascaded
in §7a:

- Two `jackson-databind` CVEs (CVE-2026-54512, CVE-2026-54513 —
  `PolymorphicTypeValidator` bypasses) and one `jackson-core` CVE
  (GHSA-r7wm-3cxj-wff9 — async-parser number-length bypass), all fixed at
  `2.18.x`. This module's `jackson-core`/`jackson-databind`/
  `jackson-annotations` are currently pinned to `2.15.2` *specifically*
  because `jackson-module-scala_2.12:2.15.2` (Spark's own, unchanged)
  enforces that exact range — see §7a. `jackson-module-scala:2.18.8`
  does exist on Maven Central, so the fix is plausible, but it needs its
  own dedicated verification pass given this exact dependency corner's
  track record in §7a.
- The Spark History Server RCE (CVE-2025-54920, Direct dependency, High)
  — still held for its own isolated Spark 3.5.1 → 3.5.7 pass, per §7a.

## 7c. Worked example: a version-numbering jump, and a second CVE on an already-fixed artifact

A small batch, closing out most of what remained after §7a/§7b:

| Artifact | Module(s) | Before | After | CVE |
|---|---|---|---|---|
| `io.airlift:aircompressor` | all three | 0.27 (already fixed for CVE-2024-36114) | 2.0.3 | CVE-2025-67721 |
| `org.apache.thrift:libthrift` | `spark-adapter` | 0.13.0 | **not changed** — accepted risk | CVE-2026-43869 |
| `org.apache.thrift:libthrift` (revisited) | `spark-adapter` | 0.13.0 | unchanged, already accepted per §7 | CVE-2020-13949 (reconfirmed) |
| `com.google.protobuf:protobuf-java` | all three | already 3.19.6 | no change needed | CVE unspecified in this batch — already fixed |

**A second, distinct CVE landed on an artifact already bumped once.**
`aircompressor` was already moved to 0.27 in §7b for CVE-2024-36114; this
batch found CVE-2025-67721 still present *at* 0.27 — a different bug (a
crafted zero-offset input makes the Snappy/LZ4 decompressors copy from
not-yet-written positions in a *reused* output buffer, leaking prior
contents), fixed only in 2.0.3. Worth noting because Aircompressor's own
versioning jumped straight from the `0.x` line to `2.0.x` with nothing
published in between — exactly the shape of jump that broke Derby's and
libthrift's packaging in §7/§7a. Checked for the same failure mode before
trusting it, not assumed safe because the previous 0.27 bump had been:
`unzip -l` on both jars shows an identical class list end to end,
including the `io.airlift.compress.hadoop` adapter package Spark's own
codec integration actually calls into. Confirmed via the real suite
regardless (286/286) rather than resting on the jar comparison alone.

**A third Thrift CVE, and the clearest illustration yet that "the fix
breaks Hive" doesn't need re-testing every time.** CVE-2026-43869 (TLS
hostname-verification bypass in `TSSLTransportFactory.java`) is fixed in
`0.23.0` — a version *nine* minors past `0.14.0`, the exact point §7a
already proved breaks Hive 2.3.9's `TFramedTransport` package
expectations by testing it directly. There was no reason to re-run that
experiment at a larger delta to learn the same lesson again; accepted as
risk on that basis, plus an even more direct reachability argument than
CVE-2020-13949's: this CVE is specifically about certificate validation
on a *TLS* Thrift connection, and `HiveConnectorSpec`'s embedded
metastore never opens a real socket at all, TLS or otherwise — there's
no certificate to mis-validate, full stop.

**`protobuf-java` reconfirmed already-fixed** for whatever CVE this
batch's alert cited — still resolving to `3.19.6` tree-wide, unchanged
since §7a first found this.

## 7d. Worked example: the Jackson stack bump — held back three times, passed clean the fourth

The Jackson stack bump (`jackson-core`/`jackson-databind`/
`jackson-annotations`/`jackson-module-scala`, all moved together to
`2.18.8`) was deliberately kept out of every batch since §7a first
flagged it, specifically because that section's Netty→Arrow→Jackson
chain happened in this exact corner of the dependency graph. It got its
own fully isolated pass, run and verified before anything else touched
it.

| Artifact | Module(s) | Before | After | CVE(s) |
|---|---|---|---|---|
| `com.fasterxml.jackson.core:jackson-core` | all three | 2.15.2 | 2.18.8 | GHSA-r7wm-3cxj-wff9 |
| `com.fasterxml.jackson.core:jackson-databind` | all three | 2.15.2 | 2.18.8 | CVE-2026-54512, CVE-2026-54513 |
| `com.fasterxml.jackson.core:jackson-annotations` | all three | 2.15.2 | 2.18.8 | (moved in lockstep, not itself alerted) |
| `com.fasterxml.jackson.module:jackson-module-scala` | all three | 2.15.2 (unpinned, Spark's own default) | 2.18.8 | (moved in lockstep — this is what actually resolves the version-check conflict) |

**It passed clean, first attempt, both directions.** §7a's regression
happened because Arrow's *own* dependency management pulled a newer
Jackson than `jackson-module-scala` would tolerate — an unplanned side
effect of a different bump. This time the Jackson bump was the deliberate,
direct target, with all four related artifacts moved to a single
version that's actually a real `jackson-module-scala` release (`2.18.8`
exists as a genuine, matched set — confirmed on Maven Central before
touching anything), rather than one artifact racing ahead of what
another still expected. `spark-adapter`'s full suite passed 286/286 on
the first run; `plugin`'s passed 4/4. The lesson isn't "Jackson bumps are
actually safe" — it's that *this specific* bump was safe because the
four pieces that need to agree on a version were bumped as the matched
set they actually are, not because bumping Jackson is inherently
lower-risk than bumping Netty.

**`plugin` and `runner` needed the override added, not adjusted** — unlike
`spark-adapter`, neither had ever needed to pin Jackson before (no Arrow,
no prior regression to fix), so they were still resolving Spark's own
unpinned `2.15.2` default. Confirmed via `dependencyTree` that both
modules genuinely resolved the vulnerable version, even though this
alert batch (like the critical-alert Avro/ZooKeeper case in §7) only
named `spark-adapter/build.sbt`.

**Verified past the build log, one more time**: `runner`'s assembled
`invaract-spark-runner.jar` was rebuilt (`sbt clean assembly`, not just
relying on `assembly`'s own staleness check, which reported "up to date"
against a jar from *before* the Jackson bump on the first attempt — a
real trap this session hit directly) and the embedded
`com/fasterxml/jackson/databind/cfg/PackageVersion.class` was extracted
and inspected directly, confirming `2.18.8` is what a real
`spark-submit` of that jar would run. `spark-adapter`'s and `plugin`'s
own assembled jars, by contrast, are byte-identical before and after
this bump (confirmed via jar hash) — expected and correct, since their
Jackson override is `test`/`provided`-scope only, exactly like every
other CVE fix in those two modules; only `runner`'s compile-scope
dependencies actually ship.

## 7e. Worked example: the Spark version bump, and a fourth regression it uncovered

The Spark History Server RCE (CVE-2025-54920, GHSA-jwp6-cvj8-fw65) is a
Direct dependency, not a transitive jar — Spark's own event-log
deserialization bug, fixed in 3.5.7. There's no `dependencyOverrides`
workaround for a bug in Spark's own code, so unlike every other fix in
this document, this one is a real `sparkVersion` bump. It was held back
from every prior batch specifically because a Spark bump can shift many
other pinned transitive versions at once — confirmed necessary caution,
not just caution for its own sake, since it did exactly that.

| Artifact | Module(s) | Before | After | CVE |
|---|---|---|---|---|
| `org.apache.spark:spark-core`/`spark-sql` | `spark-adapter` (`provided`), `plugin` (`provided`), `runner` (compile-scope) | 3.5.1 | 3.5.7 | CVE-2025-54920 |
| `io.delta:delta-spark` | `spark-adapter` (`test`-only) | 3.2.0 | 3.3.3 | not itself CVE-driven — see below |

**Checked before touching it, not assumed:** fetched `spark-core_2.12:
3.5.7`'s own published POM and confirmed it still declares
`fasterxml.jackson.version=2.15.2` and `jackson-module-scala_2.12:
2.15.2`, identical to 3.5.1 — since `dependencyOverrides` always wins
regardless of what any POM in the tree declares, this ruled out the
bump reopening §7a/§7d's Netty→Arrow→Jackson conflict class before a
single test ran.

**It still found a real regression — just not the one already
guarded against.** `spark-adapter`'s full suite failed 1/286 on the
first attempt: `ContractEnforcementRuleSpec`'s
`.format("delta").saveAsTable()` case on a *brand-new* table started
failing Spark's own analysis with `Table ... does not support truncate
in batch mode.`, before `ContractEnforcementRule` ever got a chance to
run. Root-caused rather than reverted, the same discipline as §7a's
chain:

1. Confirmed it wasn't a stale-warehouse artifact or test-order
   collision — isolating just `ContractEnforcementRuleSpec` with a wiped
   `spark-warehouse/` reproduced it standalone, so it was real.
2. Diffed `TableCapabilityCheck.scala` and `DataFrameWriter`'s
   `saveAsTable` logic between Spark 3.5.1 and 3.5.7 directly (bytecode
   diff for the former, source fetch for the latter) — both were
   unchanged in the ways that would matter here; `SaveMode.Overwrite`
   still always builds `ReplaceTableAsSelect(orCreate = true)`
   regardless of whether the table exists, exactly as this module's own
   pre-existing test comment already documented.
3. The actual failing plan (`OverwriteByExpression` against a
   placeholder `DataSourceV2Relation` reporting neither `TRUNCATE` nor
   `OVERWRITE_BY_FILTER`) pointed at the target catalog, not Spark's
   analyzer: `delta-spark:3.2.0`'s `DeltaCatalog` predates whatever
   Spark 3.5.x point release changed about the DSv2 write path it
   exercises here. Checked delta-io/delta's own release metadata (each
   git tag's `build.sbt` sets `LATEST_RELEASED_SPARK_VERSION` — `3.2.0`
   was built/tested against Spark `3.5.0`; `3.3.3`, the newest published
   `delta-spark_2.12` release at the time, against `3.5.6`) rather than
   guessing a version to try.
4. `3.3.3` is also safely past the reason `3.2.0` was pinned in the
   first place: [delta-io/delta#3737](https://github.com/delta-io/delta/issues/3737),
   a `NoSuchMethodError` the issue's own thread isolates to exactly
   Scala 2.12 + Spark 3.5.1 + Delta 3.2.1, naming "upgrade past Spark
   3.5.3" as a workaround — moot at Spark 3.5.7.

Bumped `deltaVersion` to `3.3.3` alongside the Spark bump (both land in
the same commit, since the Delta move exists *because of* the Spark
move, not independently of it). `ContractEnforcementRuleSpec` alone then
passed 49/49; the full `spark-adapter` suite passed 286/286;
`plugin` (no Delta dependency, never hit this) passed 4/4 unchanged.

**Verified past the unit suites, per this repo's Critical Requirement**:
this environment didn't have a `spark-submit` binary on `PATH` at all —
`./dev/test`'s local-execution fallback path isn't a substitute for it
(it hit its own unrelated log4j2 classloading error, a pre-existing gap
in that fallback, not a regression from this change), so a matching
Spark 3.5.7 binary distribution was installed before treating anything
as verified. With real `spark-submit` in place: `./dev/test` passed,
`report.json` shows `"sparkVersion": "3.5.7"` and
`"contractVerification": {"status": "PASSED", "violations": []}`; and
`./dev/regression` passed both cases (2/2) — a satisfying transformation
still writes normally, and a violating one is still aborted with zero
bytes written, proving `ContractEnforcementRule` itself is unaffected by
either version bump, not just that a harness run completed.

## 7f. Worked example: a 14-alert moderate batch, a real Hive-version spike, and two duplicate alerts

The first batch drawn from the moderate tier rather than critical/high —
a useful test of whether this document's triage framework (§2) still
holds up once severity alone no longer forces the pace. All 14 alerts
named `spark-adapter/build.sbt` specifically.

| Artifact | Module(s) | Before | After | CVE(s) |
|---|---|---|---|---|
| `com.fasterxml.jackson.core:jackson-databind` (+core/annotations/module-scala) | all three | 2.18.8 | 2.18.9 | CVE-2026-59889, CVE-2026-54515 |
| `io.netty:netty-codec-http` (+15 other `io.netty` artifacts) | all three | 4.1.136.Final | 4.1.137.Final | CVE-2026-59903 |
| `org.apache.logging.log4j:log4j-core`/`log4j-api`/`log4j-1.2-api`/`log4j-slf4j2-impl` | `spark-adapter`, `plugin`, `runner` (newly added — see below) | 2.20.0 | 2.25.5 | CVE-2025-68161, CVE-2026-34477, CVE-2026-34480, CVE-2026-34479, CVE-2026-49844 |
| `com.google.guava:guava` | `spark-adapter`, `plugin`, `runner` (newly documented — see below) | 16.0.1 | unchanged, accepted risk | CVE-2020-8908 (new); CVE-2018-10237 (duplicate of an existing accepted risk) |
| `commons-lang:commons-lang` | `spark-adapter` | 2.6 | unchanged, already accepted | CVE-2025-48924 (duplicate of an existing accepted risk) |
| `org.apache.hive:hive-llap-common` | `spark-adapter` | 2.3.9 | unchanged, accepted risk | CVE-2024-23953 |
| `org.apache.hive:hive-exec` | `spark-adapter` | 2.3.9 | unchanged, accepted risk | CVE-2024-29869 |
| `com.fasterxml.jackson.core:jackson-databind` (EXTERNAL_PROPERTY creator bypass) | `spark-adapter`, `plugin`, `runner` | 2.18.9 | unchanged, accepted risk | GHSA-mhm7-754m-9p8w |

**Two of the fourteen were already-decided duplicates, not new work.**
The Guava DoS alert and the `commons-lang` recursion alert are the exact
same CVEs (CVE-2018-10237, CVE-2025-48924) §7b already investigated and
accepted as risk, with the reasoning already sitting in
`spark-adapter/build.sbt`'s own comments — Dependabot re-surfacing an
alert doesn't mean re-deriving the decision, just confirming the
existing one still applies (it does; nothing about the dependency tree
or this module's test reachability changed).

**A third Jackson-databind CVE in this batch has no available fix in the
2.x line at all**, a case this document hadn't hit before for Jackson
specifically: GHSA-mhm7-754m-9p8w (a `@JsonView` bypass for a creator
property that also carries `@JsonTypeInfo(include = As.EXTERNAL_PROPERTY)`)
was fixed only on Jackson's 3.x line (a different Maven groupId,
`tools.jackson.core` — a full migration, not a version bump) and was
never backported to 2.18/2.21. Accepted risk, same "no available patched
version" bucket as the Derby/`jackson-mapper-asl` precedents in §7,
just the first time this document has hit it for an *actively
maintained* library rather than an abandoned one.

**The real work of this batch was the Hive pair, and it's the first time
this document tried a fix and disproved it via a real test run rather
than reasoning about it up front.** Both Hive CVEs (CVE-2024-23953 in
`hive-llap-common`, CVE-2024-29869 in `hive-exec`) require Hive `4.0.x`
— a major-version jump from the `2.3.9` Spark 3.5.7's own `spark-hive`
bundles. Rather than assuming that's too large a jump (the instinct
`§7`'s Derby/libthrift entries warn against), it was tried: overriding
just `hive-exec`/`hive-llap-common`/`hive-llap-client` to `4.0.1`.
`Test/compile` passed — meaningless on its own, since this module never
imports Hive classes directly, only via reflection/class-name matching
(`WriteCommandSupport`'s own convention) — so the real check was running
`HiveConnectorSpec` for real. It failed all 14 of its own tests, 1 suite
aborted: `ClassNotFoundException:
org.apache.hadoop.hive.ql.metadata.HiveException` while Spark's
`HiveExternalCatalog` reflectively constructs itself. Root cause: only
three of the many `org.apache.hive` artifacts were overridden, leaving
the rest (`hive-common`, `hive-metastore`, etc., still pulled at `2.3.9`
by `spark-hive` itself) on the old version — a split-version classpath,
the same class of failure the Netty/PoolArena lesson in `spark-adapter/
build.sbt`'s own comments already warns about, just for a different
library. Overriding *every* `org.apache.hive` artifact to `4.0.1` to
close that gap would mean replacing `spark-hive`'s entire bundled Hive
client — no longer a transitive-CVE override at that point, but
reimplementing Spark's own Hive integration against a metastore-client
generation it was never built for. Accepted risk; reverted the override
back to `2.3.9` (implicit, via `spark-hive`'s own resolution) and
documented the finding directly in `spark-adapter/build.sbt`'s comment,
the same place Derby's and libthrift's own disproved attempts live.

**`plugin`/`runner` got the Jackson/Netty/log4j fixes and the Guava
accepted-risk documentation too, matching §7d's precedent** — this
alert batch named only `spark-adapter/build.sbt`, but `dependencyTree`
confirmed both modules resolve the identical vulnerable Jackson/Netty/
log4j/Guava versions via the same Spark dependency footprint. Hive and
`commons-lang` 2.x, by contrast, don't appear in `plugin`/`runner`'s
trees at all (neither module depends on `spark-hive`), so those two
stayed `spark-adapter`-only — confirmed via `dependencyTree`, not
assumed from the alert batch's own file list either way.

`spark-adapter`'s full suite passed 286/286 with the confirmed fixes in
place (Jackson `2.18.9`, Netty `4.1.137.Final`, log4j `2.25.5`, Hive
reverted to `2.3.9`); `plugin` passed 4/4. `runner`'s `sparkVersion`/
`deltaVersion` aren't touched by this batch, but its own log4j pin is
newly added here and does change what `invaract-spark-runner.jar`
bundles (compile-scope, like every other override in that module) — a
full `./dev/test` run with real `spark-submit` confirmed `Status: PASS`
and contract verification still passing after the rebuild.

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
- [x] Fix the second high-severity batch (ZooKeeper, Netty, aircompressor,
      commons-lang3, snappy-java, lz4-java fork switch; this change) —
      see §7b, including two new accepted risks (`commons-lang` 2.x EOL;
      `guava`, unreachable through this module's `local[*]`-only testing)
      and the `jackson-mapper-asl`/`protobuf-java` alerts already resolved
      without a code change.
- [x] Fix the third high-severity batch (aircompressor's second CVE; this
      change) — see §7c, including a third Thrift CVE left as accepted
      risk without needing to re-run the §7a Hive-compatibility
      experiment, and `protobuf-java` reconfirmed already-fixed.
- [x] Fix the two `jackson-databind` CVEs and one `jackson-core` CVE from
      §7b/§7c (this change) — see §7d. Bumped `jackson-core`/
      `jackson-databind`/`jackson-annotations`/`jackson-module-scala`
      together to `2.18.8`, as its own fully isolated pass given this
      exact corner's §7a history. Passed clean on the first attempt in
      both `spark-adapter` and `plugin` — the earlier regression was
      Arrow racing ahead of what `jackson-module-scala` tolerated, not
      evidence that Jackson bumps themselves are risky; this one moved
      all four pieces that need to agree as the matched set they are.
- [x] Fix the Spark History Server RCE (CVE-2025-54920, Direct dependency,
      High) — Spark 3.5.1 → 3.5.7 (this change). See §7e: it needed its
      own isolated pass, and it found a fourth regression (this one in
      `delta-spark:3.2.0`'s compatibility with the bumped Spark's DSv2
      write path, fixed by moving `deltaVersion` to `3.3.3` alongside it)
      — confirmed via `./dev/test` and `./dev/regression` with a real
      `spark-submit` 3.5.7, not just the unit suites.
- [x] Fix the first moderate-severity batch (this change) — see §7f:
      Jackson/Netty/log4j bumped across all three modules, two Hive CVEs
      tried and disproved via a real `HiveConnectorSpec` run (accepted
      risk — a split Hive 2.3.9/4.0.1 classpath breaks
      `HiveExternalCatalog`), a Jackson-databind CVE with no available
      2.x-line fix accepted as risk, and two alerts (Guava DoS,
      `commons-lang` recursion) confirmed as duplicates of §7b's existing
      accepted-risk decisions rather than new work.
- [ ] Walk the rest of the Scala/Maven bucket per §4's manual workflow,
      batched per §5 — one coordinate (or tightly-related group, per §7a's
      Netty→Arrow→Jackson chain) at a time, real test suite run after
      each, no matter how confident the reasoning sounds. Record each
      one's scope/inheritance status per §2's table format (see §7/§7a/§7b).
- [ ] Fix or explicitly document-and-accept every alert with no available
      patched version, per §3 — Derby (§7) and `commons-lang`/`guava`
      (§7b) are templates for how to document one, each for a different
      underlying reason (no fix exists at all; fix exists but breaks a
      dependent; fix exists but can't be verified reachable).
- [ ] Once the backlog is current, treat "zero unaddressed alerts older than
      the SLA in §3" as the steady-state target, not "zero alerts" — new
      ones will always arrive with new versions of Spark's own dependency
      tree.
