# Releasing to Maven Central

This is a maintainer-facing process document (see CLAUDE.md's docs/ vs.
docs-site/ split — this belongs here, not in docs-site/, because it
describes how *this project* cuts a release, not how a user of the
published engine installs or configures it). Once a release actually
exists on Central, add a "Installing via Maven Central" section to the
relevant docs-site/ guide with the real coordinate and a real
`libraryDependencies` snippet — per CLAUDE.md's Documentation Policy, that
update belongs in the PR/commit that makes the release genuinely usable,
not before.

## What gets published

Seven modules: `contract`, `ir`, `fingerprint`, `verification-core`, `adapter-testkit`,
`spark-adapter` and `beam-adapter` (the Apache Beam adapter; it depends on `contract`, `ir` and
`verification-core`, and on the kit in tests, so it is released after them and is independent of
`spark-adapter`). The first five plus `spark-adapter` are the verification engine CLAUDE.md's
"What's the product" section describes, and `spark-adapter`'s published POM depends on all the
others, so a release that left any of them out would be unresolvable. `adapter-testkit` is
test-scope infrastructure, but it is what a third-party adapter compiles its conformance tests
against, and `spark-adapter` itself lists it as a test dependency. All seven have Maven Central
publishing settings in their `build.sbt`/`project/*.sbt` (identical, copied from `ir`).

`plugin`, `runner`, `demo`, and `web` are the example harness — never
published, nothing to release. `notification-kafka` is an optional
extension a user currently builds from source themselves (see its own
`build.sbt` comment and docs-site's notification-sinks guide); it is not
part of this release process today. Publishing it to Central instead of
requiring a manual `sbt assembly` is a reasonable future improvement, but
a separate decision from getting the core engine onto Central.

`contract` is additionally published to **GitHub Packages'** Maven
registry (`https://maven.pkg.github.com/mlltx/Invaract`) in the same
release.yml run, alongside its Sonatype release — see that workflow's own
top comment and `docs/CONTRACT_REGISTRY.md`'s §2 for why: the separate
`mlltx/invaract-registry` repo needs a real, pinnable `contract`
coordinate to depend on, and can't wait for Sonatype namespace
verification (below) to land first. The other five have no GitHub Packages destination — nothing outside this repo depends
on them the way `invaract-registry` depends on `contract`.

## One-time setup (a human must do this; nothing here is automatable)

1. **Create a Sonatype Central Portal account** at
   [central.sonatype.com](https://central.sonatype.com) (the old
   OSSRH/Nexus staging host is retired — Central Portal is the only route
   onto Maven Central now).

2. **Verify the `com.invaract` namespace.** Central Portal verifies
   group-ID ownership either via GitHub OAuth (for an `io.github.<user>`
   namespace) or a DNS TXT record proving control of the domain (for a
   reverse-domain namespace like `com.invaract`). This project's
   `organization` is `com.invaract` in all six modules, so this step
   requires owning `invaract.com` and adding the TXT record Central
   Portal's namespace-verification flow gives you. See
   [central.sonatype.org's namespace docs](https://central.sonatype.org/register/central-portal/)
   for the current, authoritative steps — **do not attempt a release
   before this is verified**; every `sonatypeCentralRelease` call will be
   rejected until it is.

3. **Generate a GPG key pair** for signing releases (Central requires every
   published artifact — jar, sources jar, javadoc jar, pom — to be PGP
   signed):
   ```
   gpg --full-generate-key
   gpg --armor --export-secret-keys <key-id>   # → GitHub secret PGP_SECRET
   gpg --keyserver keyserver.ubuntu.com --send-keys <key-id>
   ```
   Central Portal validates signatures against public keyservers, so the
   public key must actually be published to one (keyserver.ubuntu.com or
   keys.openpgp.org both work).

4. **Generate a Sonatype user token** in the Central Portal UI (account
   settings → Generate User Token) — this is a token pair, not your
   account password.

5. **Add repository secrets** (Settings → Secrets and variables → Actions)
   for `.github/workflows/release.yml` to use:
   - `PGP_SECRET` — the full ASCII-armored output of `gpg --armor
     --export-secret-keys` from step 3.
   - `PGP_PASSPHRASE` — that key's passphrase.
   - `SONATYPE_USERNAME` / `SONATYPE_PASSWORD` — the token pair from step 4
     (not your Central Portal login).

None of this is checked into the repo, and none of it can be done from
inside a coding session — it requires owning the domain, controlling a
real keyserver-published GPG key, and a human with access to the
project's Sonatype account.

## Cutting a release

1. Decide which module(s) actually changed and need a version bump.
   `contract`/`ir`/`spark-adapter` version independently (already true
   today — all three are at `0.3.0`, each having bumped independently for
   its own deliberate break), so a release does not have to touch all
   three.
2. Bump `ThisBuild / version :=` in the changed module's `build.sbt`,
   following docs/VERSIONING.md's rules (pre-1.0: MINOR bump for any
   breaking change, PATCH for a non-breaking one).
3. If the change touched `ir`/`spark-adapter`'s public API, do **not** flip
   that module's `mimaPreviousArtifacts` in the same PR — CLAUDE.md's API
   Compatibility Requirement and this repo's own established pattern
   (see e.g. `ir/build.sbt`'s own comment trail) flip it in a *follow-up*
   PR once this one has actually landed on the base branch, since
   CI's `api-compatibility` job diffs against what's on `main`, not what's
   in the PR that's about to merge.
4. Run `./dev/test` (and `./dev/regression` if the change affects
   enforcement), plus scoped mutation testing for any touched `ir`/
   `spark-adapter` file, per CLAUDE.md's own requirements — a version bump
   is not exempt from any of this.
5. Merge to `main`.
6. Tag the merged commit and push the tag:
   ```
   git tag v2026.09.0   # any tag matching v* — the tag name itself isn't
                         # read by release.yml; each module's own
                         # ThisBuild/version is authoritative
   git push origin v2026.09.0
   ```
   This triggers `.github/workflows/release.yml`, which builds, signs, and
   releases `contract`, `ir`, `fingerprint`, `verification-core`, `adapter-testkit`, then
   `spark-adapter` and `beam-adapter` in that order (order matters: each module's own `libraryDependencies` resolve
   the earlier ones from the same runner's local Ivy cache — see
   `spark-adapter/build.sbt`'s comment on that dependency, and
   `dev/build`'s matching local-build pattern). `contract`'s step
   additionally publishes to GitHub Packages right after its Sonatype
   release — one tag, two destinations for that one module.
7. Watch the workflow run in the Actions tab. `sonatypeCentralRelease`
   uploads a signed bundle to Central Portal, which validates and releases
   it automatically — no manual "close and release" step, unlike the old
   OSSRH staging-repo flow.
8. Once released, the artifact typically appears on
   [central.sonatype.com](https://central.sonatype.com) within a few
   minutes and on search.maven.org somewhat after that (Central's own
   indexing, outside this project's control).
9. **Re-releasing a version that already exists on Central is rejected,
   not overwritten** — Central Portal (like OSSRH before it) does not
   allow mutating a released version. If a release step fails partway
   (e.g. `contract` releases but `spark-adapter` fails), fix the problem
   and re-run the workflow: whichever modules already succeeded are
   no-ops (their exact version already exists), and the failed one
   retries cleanly, since each module's `version` was not bumped again.
10. Update docs-site/ with the real installation coordinate (see the note
    at the top of this file) — this is the step that actually makes the
    release visible to a real Invaract user, per CLAUDE.md's Documentation
    Policy.

## Dry run on every publishing change

`.github/workflows/release-dry-run.yml` runs on any pull request that touches a `build.sbt`, a
`project/*.sbt`, `release.yml` or this file. It creates a throwaway GPG key, runs `publishLocal` and
`publishSigned` for all six modules in `release.yml`'s order, and checks that each produced a staged
bundle with a jar, a sources jar, a javadoc jar and a POM, each with a `.asc` signature. It uses no
secrets and uploads nothing, so it proves the build and signing configuration works; it cannot prove
Sonatype accepts the namespace, the real key or the credentials (the one-time setup above), which only a
real release does.

## Local dry run

Everything except the final `sonatypeCentralRelease` upload can be
exercised locally without touching Central at all:

```
cd contract && sbt clean test publishLocal publishSigned
```

`publishSigned` signs and writes to the local bundle directory
(`target/sonatype-staging/<version>/`) that `sonatypeCentralRelease` would
otherwise upload — inspect its contents (a real jar/sources/javadoc/pom,
each with a matching `.asc` signature) to confirm signing actually worked
before wiring up real CI secrets.

# The Python packages

The Python I/O adapter (docs/PYTHONIO_ADAPTER.md) is released to **PyPI**, separately from the Maven
modules above, by `.github/workflows/release-python.yml`, on a tag of its own (`python-v<version>`; a `v*`
tag never starts it, and it never starts `release.yml`).

## What gets published

- **`invaract`**, as five wheels: one per platform with a native checker built for it (Linux x86_64 and
  aarch64, macOS arm64 and x86_64), each carrying that checker at `invaract/bin/`, and one pure-Python
  wheel (`py3-none-any`) carrying the checker jar at `invaract/lib/`, which the package runs with Java
  on every other platform. pip picks the platform wheel where one matches. There is no source
  distribution: one built from source would carry no checker.
- **`invaract-lint`**, a source distribution and a pure-Python wheel.
- On the GitHub release for the tag: all of the above, the checker jar, and each native checker.

A platform wheel's tag is read from the checker it carries (`.github/scripts/python_wheel.py`): the
newest glibc symbol version the Linux program needs (`manylinux_<x>_<y>`), or the macOS program's
minimum OS version. A checker built on a newer runner image gets a narrower tag, never one that claims
more than it runs on. On macOS the linker records the build machine's own release as that minimum unless
told otherwise, so the workflow links the checker for macOS 11 (`MACOSX_DEPLOYMENT_TARGET` and
`-mmacosx-version-min`), and checks each wheel's tag. A further job installs each native wheel on an
older runner image than the one that built it (Ubuntu 22.04, macOS 14) with Python 3.10 and runs the
example job, so a wheel whose tag pip accepts there but whose checker does not run is caught before it
is published. `python_wheel.py version` refuses a release whose package, checker
(`pythonio-adapter/build.sbt`) and lint versions differ, and the workflow refuses a tag that does not
name that version.

## One-time setup (a human must do this)

1. **Register both names on PyPI**, `invaract` and `invaract-lint` (decision 6 of
   docs/PYTHON_IO_ADAPTER_DESIGN.md; both were free on 2026-11-01).
2. **Add a trusted publisher to each project** on PyPI (the project's Publishing settings): owner
   `mlltx`, repository `Invaract`, workflow `release-python.yml`, environment `pypi`. Trusted publishing
   uses a short-lived token GitHub issues to the workflow; no PyPI token is stored anywhere. Before a
   project exists, PyPI's "pending publisher" form does the same and creates the project on the first
   upload.
3. **Create the `pypi` environment** in the repository's settings (Settings → Environments). Restricting
   it to tags matching `python-v*`, and requiring a reviewer, keeps a publish from running on anything
   else.

## Cutting a release

1. Set one version in all three places: `__version__` in `pythonio-adapter/python/invaract/__init__.py`
   and in `pythonio-adapter/lint/invaract_lint/__init__.py` (each `pyproject.toml` reads its package's),
   and `ThisBuild / version` in `pythonio-adapter/build.sbt`. `python3 .github/scripts/python_wheel.py
   version` prints it, or names each place that differs.
2. Merge to `main`.
3. Tag the merged commit and push the tag:
   ```
   git tag python-v0.1.0
   git push origin python-v0.1.0
   ```
4. The workflow builds the checker jar, a native checker and wheel on each platform, the pure-Python
   wheel and the lint, installs each wheel in a new virtual environment and runs the example job with
   it (`python_wheel.py smoke`: it must run the checker the wheel carries, publish a passing run and
   refuse a failing one), then publishes to PyPI and creates the GitHub release.
5. PyPI refuses a version that already exists, as Central does. If the workflow fails after publishing
   one package and not the other, re-run the failed jobs; to change what a published version contains,
   release a new version.
6. Until docs/PYTHON_IO_ADAPTER_DESIGN.md's milestone 6 lands, each release note says what is still
   missing (object storage, Delta Lake and Iceberg); the workflow's release body does.

## Dry run

`release-python.yml` runs everything up to publishing on any pull request that changes how the packages
are built (either package's `pyproject.toml` or `__init__.py`, `_checker.py`, `pythonio-adapter/build.sbt`,
`python_wheel.py`, the workflow or this file), and on a manual run. It uses no secret and uploads nothing to PyPI; it cannot prove the
trusted publisher is set up, which only a real release does. Locally, with the jar built
(`cd pythonio-adapter && sbt assembly`):

```
python3 .github/scripts/python_wheel.py java --jar pythonio-adapter/target/scala-2.12/invaract-pythonio-checker-0.1.0.jar --out dist
python3 .github/scripts/python_wheel.py smoke --wheel dist/invaract-0.1.0-py3-none-any.whl --expect java --extra pandas
```
