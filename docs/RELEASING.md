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

Only `contract`, `ir`, and `spark-adapter` — the three modules CLAUDE.md's
"What's the product" section identifies as the verification engine, and
the only three with Maven Central publishing settings in their
`build.sbt`/`project/*.sbt` at all.

`plugin`, `runner`, `demo`, and `web` are the example harness — never
published, nothing to release. `notification-kafka` is an optional
extension a user currently builds from source themselves (see its own
`build.sbt` comment and docs-site's notification-sinks guide); it is not
part of this release process today. Publishing it to Central instead of
requiring a manual `sbt assembly` is a reasonable future improvement, but
a separate decision from getting the core engine onto Central.

## One-time setup (a human must do this; nothing here is automatable)

1. **Create a Sonatype Central Portal account** at
   [central.sonatype.com](https://central.sonatype.com) (the old
   OSSRH/Nexus staging host is retired — Central Portal is the only route
   onto Maven Central now).

2. **Verify the `com.invaract` namespace.** Central Portal verifies
   group-ID ownership either via GitHub OAuth (for an `io.github.<user>`
   namespace) or a DNS TXT record proving control of the domain (for a
   reverse-domain namespace like `com.invaract`). This project's
   `organization` is `com.invaract` in all three modules, so this step
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
   today — `contract`/`ir` are at `0.3.0`, `spark-adapter` at `0.2.0`), so
   a release does not have to touch all three.
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
   releases `contract`, `ir`, then `spark-adapter` in that order (order
   matters: `spark-adapter`'s own `libraryDependencies` resolve
   `contract`/`ir` from the same runner's local Ivy cache — see
   `spark-adapter/build.sbt`'s comment on that dependency, and
   `dev/build`'s matching local-build pattern).
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
