# Contract Registry — Design

**Status:** Design only, not yet implemented. This document is the
reviewable spec both the `registry-client` module (this repo) and the
`invaract-registry` server (a separate repo — see §2 for why) are built
against, the same role `docs/SEMANTIC_LINEAGE_FINGERPRINTING.md` played
for `fingerprint/` before it existed. Treat every "MUST"/"returns"/status
code below as the contract an implementation has to satisfy, not a
suggestion either side is free to drift from.

**Scope.** This document covers: why a registry exists at all (§1), the
two-repo split and what lives where (§2), the REST wire protocol every
implementation speaks (§3), the storage/concurrency contract a server
backend must satisfy (§4), how registering a version interacts with
existing compatibility checking (§5), the `registry-client` design (§6),
how `spark-adapter` attaches to a registry (§7), and the minimal v1 auth
model (§8). It explicitly does **not** design `invaract-registry`'s
internal server architecture beyond the contract it must expose (the
routes in §3, the semantics in §4) — that repo owns its own
implementation notes, the same way this repo doesn't design Delta Lake's
internals, only the boundary `spark-adapter` translates against.

**Grounding.** Every type referenced below already exists in
`contract/src/main/scala/com/invaract/contract/`: `Contract`,
`ContractVersion`, `ContractParser`, `ContractValidator`, and
`ContractCompatibility` (`CompatibilityReport`/`CompatibilityLevel`/
`verifyVersionBump`). This design reuses all of them as-is — there is no
new compatibility-checking logic anywhere in this plan, only a new place
that calls the logic `contract` already has.

---

## 1. Why a registry

`ROADMAP.md`'s Phase 3 ("Contract Registry & Governance") names this as
unbuilt future scope: registry design, version management, compatibility
checking, impact analysis, governance policies. Today every contract is a
standalone YAML file (`ContractParser.parseFile`); nothing in Invaract
knows about every contract an organization has at once. Two concrete
capabilities are explicitly named as blocked on that not existing, in
`docs/CONTRACT_MODEL.md`'s "What this does *not* do (yet)" section:

- **Cross-contract policies** — "every input must be some other
  contract's declared output," "no two contracts may target the same
  physical location." Both need a place that knows about every contract
  in an org at once.
- **Fingerprint-based drift control** — a policy comparing a write's
  semantic fingerprint (`fingerprint/`) against a previously-*approved*
  value. Nothing today stores an "approved" fingerprint anywhere.

This design builds the registry those depend on. It does not build the
policies themselves — see §9's non-goals.

## 2. Two-repo split

`invaract-registry` (the server) is a real, long-running deployable
service with its own release/ops lifecycle: something a platform team
runs continuously and redeploys on its own schedule, not a jar a Spark
job embeds. That is a fundamentally different shape of artifact than
every existing Invaract module — `contract`/`ir`/`spark-adapter`/
`fingerprint` are pure libraries with zero networked-service or database
dependency, and `plugin`/`runner`/`notification-kafka` follow the same
"jar, not service" shape. Putting a Javalin server, a `Dockerfile`, and a
container-image release pipeline into this repo's module graph would
stretch conventions (`dev/build`'s dependency-ordered jar builds, the
mutation-testing shard machinery, `release.yml`'s Sonatype-publish
lifecycle) built for a different kind of thing, and would couple the
server's release cadence to the engine's.

So:

- **`registry-client`** lives in *this* repo (Invaract) — a library, same
  shape as `fingerprint`, depending only on `contract`. Anything that
  wants to talk to a registry (a Spark job, a CI pipeline registering a
  new version) depends on this, regardless of where the server runs.
- **`invaract-registry`** (the server) lives in a separate repo,
  `mlltx/invaract-registry`. It consumes `com.invaract:invaract-contract`
  as an ordinary published dependency — the same relationship any
  external consumer of `contract` would have — resolved via GitHub
  Packages until Maven Central publishing lands (`docs/RELEASING.md`
  tracks that blocker). It has its own CI, its own `Dockerfile`, its own
  versioning, independent of Invaract's release cadence.
- This document (§3, §4) is the one place the wire protocol and the
  storage contract are specified; both repos implement against it rather
  than either one re-deriving the other's behavior from source.

## 3. REST wire protocol

Implemented by `invaract-registry`, consumed by `registry-client`'s
`HttpContractRegistryClient`. Request/response bodies are contract YAML
(`ContractParser.write`'s format) unless noted otherwise.

| Method | Path | Request | Success | Failure |
|---|---|---|---|---|
| `PUT` | `/contracts/{id}/versions/{version}` | Body: contract YAML. Query: `expectedPrevious=<version>` (omit for "this must be the first version ever registered"), `force=true` (optional, overrides a compatibility rejection) | `201 Created`, body: the registered contract's compatibility report (`CompatibilityReport`, JSON) | `400` (parse/validate failure — body: `ValidationResult`); `409 Conflict` (either a version-concurrency conflict, §4, or a rejected compatibility bump, §5 — body distinguishes which) |
| `GET` | `/contracts/{id}/versions/{version}` | `version` literal, or the string `latest` | `200 OK`, body: contract YAML | `404` (no such id, or no such version) |
| `GET` | `/contracts/{id}/versions` | — | `200 OK`, body: JSON array of `{version, status}` (`status` is `Contract.status`, already a field on the existing model) | `404` (no such id) |
| `GET` | `/contracts` | — | `200 OK`, body: JSON array of contract ids | — |

`GET /contracts` is deliberately part of v1 even though nothing consumes
it yet: it's the query cross-contract policies and impact analysis (§9)
will need once they exist — "list every contract this org has" — and
costs nothing extra to expose now, on top of a store that already has to
track it internally.

## 4. Storage and concurrency contract

Any backend `invaract-registry` uses — the in-memory reference
implementation it ships first, or a persistent one later (§9) — must
satisfy this trait:

```scala
trait RegistryStore {
  def currentVersion(contractId: String): Option[ContractVersion]
  def get(contractId: String, version: ContractVersion): Option[Contract]
  def listVersions(contractId: String): List[ContractVersion]
  def listContractIds(): List[String]

  /** Atomically registers `contract` as the new version for its id, but
    * only if the store's current latest version equals `expectedPrevious`
    * (None means "this must be the first version ever registered").
    * Returns VersionConflict if another writer already advanced past
    * `expectedPrevious` between the caller reading it and this call —
    * the caller must re-read and retry, the same shape as an
    * optimistic-concurrency failure against a real database.
    */
  def registerIfCurrentIs(
    contract: Contract,
    expectedPrevious: Option[ContractVersion]
  ): Either[RegistryError.VersionConflict, Unit]
}

sealed trait RegistryError
object RegistryError {
  case class VersionConflict(actualCurrent: Option[ContractVersion]) extends RegistryError
  case class CompatibilityRejected(report: CompatibilityReport) extends RegistryError
}
```

**This atomicity guarantee is not optional or v1-only** — it's the
contract any future backend has to satisfy, the same way `LocationResolver`
in `spark-adapter` is one trait with swappable implementations
(`NoOpLocationResolver`, `StaticMapLocationResolver`, a future
`HttpLocationResolver`). The reference implementation
(`InMemoryRegistryStore`, in `invaract-registry`) proves the semantics
with `java.util.concurrent.ConcurrentHashMap.compute` — genuine
per-key atomicity, not a `synchronized` block bolted onto a plain map —
verified by a concurrency stress test (N threads racing to register the
same next version; exactly one succeeds, the rest observe
`VersionConflict`). It is not durable across restarts; that's the
explicit, disclosed limitation of v1 (§9), not something this document
pretends is solved.

## 5. Governance on write

On every `PUT`, before calling `registerIfCurrentIs`:

1. `ContractParser.parse` the request body, then `ContractValidator.validate`
   it — reject with `400` on any `Error`-severity issue. Both already
   exist; no new parsing/validation logic.
2. If `store.currentVersion(id)` returns a prior version, fetch that
   `Contract` and compute `ContractCompatibility.diff(previous, next)` and
   `ContractCompatibility.verifyVersionBump(previous, next)` — again, both
   already exist in `contract`'s `ContractCompatibility.scala`.
3. If `verifyVersionBump` reports a problem (the declared version bump
   doesn't match the actual scope of change — e.g. a breaking schema
   change released as a PATCH) and the request didn't pass `force=true`,
   reject with `409 Conflict` and the `CompatibilityReport` in the body.
   `force=true` allows a deliberate override — the same "two honest
   options, never a silent third" philosophy CLAUDE.md's API Compatibility
   Requirement already applies to a different kind of compatibility break
   (an accidental break gets fixed; a deliberate one gets an explicit,
   visible marker — here, the caller explicitly opting into `force`,
   there, a documented `ProblemFilters.exclude[...]` entry).
4. Call `store.registerIfCurrentIs`; a `VersionConflict` becomes `409`
   with the actual current version in the body, so the caller can re-read
   and retry.

This is the mechanism `docs/CONTRACT_MODEL.md`'s "Version Compatibility"
section already anticipated ("This is the mechanism a future CI check
... would call on a pull request that modifies a contract file") — the
registry is that mechanism, running at registration time rather than in
a separate CI step.

## 6. `registry-client` design (this repo)

New module `registry-client/`, depending only on `contract`. Follows
`fingerprint/build.sbt`'s template: MiMa + Stryker4s wired from day one,
whole-module scope, no Maven Central publishing until it's ready (the
same "deferred, disclosed, not silently absent" treatment `fingerprint`
itself got before it was ready).

```scala
trait ContractRegistryClient {
  def get(contractId: String, version: ContractVersion): Contract
  def getLatest(contractId: String): Contract
  def register(
    contract: Contract,
    expectedPrevious: Option[ContractVersion],
    force: Boolean = false
  ): RegistrationResult
}
```

Real implementation, `HttpContractRegistryClient`: built on the JDK's own
`java.net.http.HttpClient` (available since JDK 11; this repo runs on
JDK 21 with `-target:jvm-1.8` *bytecode* — a bytecode-version restriction
on emitted classfiles, not a restriction on which JDK APIs are callable
at runtime). Zero new library dependencies, matching this repo's existing
"boring, minimal deps" instinct (the same reasoning `fingerprint`'s own
`build.sbt` gives for staying Spark-independent).

## 7. `spark-adapter` attachment — optional, not a compile dependency

This is the part that has to satisfy CLAUDE.md's External Attachability
Requirement: a platform team must be able to point an unmodified job at a
registry-hosted contract using only `spark-submit --conf`. It also has to
satisfy a narrower constraint: **using a registry must not change
anything about a job that doesn't.** `spark-adapter` does **not** take a
compile-time dependency on `registry-client` — the wrong precedent would
be copying how `spark-adapter` depends on `fingerprint` (a real,
always-bundled `libraryDependency`, appropriate because fingerprinting is
core, expected-by-default functionality). A contract registry is optional
infrastructure a platform team may never deploy, so it follows
`notification-kafka`'s precedent instead: opt-in, not built by
`./dev/build`, not part of `spark-adapter`'s own dependency footprint,
added to a job only via `--jars` when actually wanted.

Mechanism, reusing the exact existing `ref://<id>` pattern
(`LocationRef.scala`/`ContractLocationResolution.scala`) rather than
inventing a new one:

- `spark.invaract.contract`'s value gains a recognized
  `registry://<id>@<version>` form (`@latest` supported), detected by
  new, dependency-free string-parsing logic in
  `spark-adapter/src/main/scala/com/invaract/sparkadapter/registry/ContractSource.scala`
  (today, `InvaractSparkSessionExtension.checkRuleFor` passes that value
  straight to `ContractParser.parseFile` with no scheme detection at
  all).
- Actually fetching the `Contract` uses the same reflective,
  resolve-by-class-name mechanism `NotificationSinkFactory`/
  `CustomPolicyEvaluatorFactory` already establish elsewhere in this
  repo: a new `spark.invaract.registryClientClass` conf key, resolved via
  `Class.forName(...).getDeclaredConstructor().newInstance()`, cast to a
  `ContractRegistryClient`-shaped trait `spark-adapter` knows about
  without depending on `registry-client`'s implementation. The real
  `HttpContractRegistryClient` only needs to be on the classpath (via
  `--jars`) when a job actually uses `registry://`.
- New conf key `spark.invaract.registryUrl` — the base URL
  `HttpContractRegistryClient` talks to.

```
spark-submit \
  --conf spark.sql.extensions=com.invaract.sparkadapter.InvaractSparkSessionExtension \
  --conf spark.invaract.contract=registry://customer_orders@2.1.0 \
  --conf spark.invaract.registryUrl=https://registry.corp.internal \
  --jars invaract-spark-adapter-*.jar,invaract-registry-client-*.jar \
  my-job.jar
```

The "resolver the conf key can't express" case (the code-path bonus the
External Attachability Requirement's composition rule always leaves open
for `ref://`/`locationMap`) needs **no new spark-adapter API at all** here
— corrected after actually implementing this section: a job's own code
can already call `registry-client` directly (`client.getLatest(id)` or
`.get(id, version)`) to obtain a real `Contract`, then hand it to
`ContractEnforcementRule.forContract(contract)` exactly the way it always
could. `forContract` already takes a plain `Contract`, the same way
`ContractLocationResolution.resolve(...)`'s own explicit-code path
produces a `Contract` and hands it to the same existing overload — there
was never a missing overload to add, only a resolver to call before
`forContract`.

**Confirmed end state**: `./dev/build`, `./dev/test`, and
`./dev/regression` all continue to pass with zero registry-related code
present anywhere and no server running anywhere — nothing in the existing
critical path changes. `registry-client` is not part of `./dev/build`'s
default module set, the same as `plugin`/`runner`/`notification-kafka`
today.

## 8. Auth (v1)

Shipping a write-capable networked service with no auth consideration
isn't responsible even for a v1. `invaract-registry` ships one minimal,
pluggable mechanism: an `AuthProvider` trait (`authorize(request):
Boolean`) with a static shared-secret bearer-token implementation.
Real SSO/OIDC integration is explicit fast-follow (§9), not blocking —
the extension point exists from the start so it isn't a breaking
redesign later, the same reasoning `CustomPolicyEvaluator`'s reflective,
class-name-in-config shape already gives this repo for other
extension points.

## 9. Non-goals (explicit)

Named rather than silently absent, the same discipline
`docs/CONTRACT_MODEL.md`'s own "What this does *not* do (yet)" section
uses:

- **A persistent `RegistryStore` backend** with real cross-process
  compare-and-swap (a filesystem/Hadoop-FS conditional write, or a
  JDBC-backed row lock) — `invaract-registry`'s own future work. The
  trait in §4 is designed for it; the implementation is not this
  document's scope.
- **Real SSO/OIDC auth** — the `AuthProvider` extension point exists;
  a real provider is separate work in `invaract-registry`.
- **Cross-contract policies and fingerprint-based drift control**
  consuming this registry (§1's motivating examples) — this document
  builds the registry those depend on, not the policies themselves.
- **Impact analysis as a feature** — a UI or report beyond the
  `GET /contracts` query this design exposes.
- **Maven Central publishing for `registry-client`** — matches
  `fingerprint`'s own deferred-publishing precedent; no release metadata
  until it's ready.
