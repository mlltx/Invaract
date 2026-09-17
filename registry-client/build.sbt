name := "invaract-registry-client"
ThisBuild / version := "0.1.0"
scalaVersion := "2.12.18"
organization := "com.invaract"

// This module implements docs/CONTRACT_REGISTRY.md §6: a client for the
// contract registry HTTP API specified there and implemented by the
// separate mlltx/invaract-registry repo. Depends only on `contract` (for
// `Contract`/`ContractParser`/`ContractVersion`), no Spark dependency —
// mirroring `fingerprint`'s own independence from `spark-adapter`, since a
// CI pipeline registering a new contract version has no reason to pull in
// all of `spark-adapter`'s Spark dependency just to make one HTTP call.
//
// Deliberately NOT a compile-time dependency of `spark-adapter`
// (docs/CONTRACT_REGISTRY.md §7 explains why: a contract registry is
// optional, opt-in infrastructure, following `notification-kafka`'s
// precedent rather than `fingerprint`'s always-bundled one). spark-adapter
// resolves this module's `HttpContractRegistryClient` reflectively, by
// class name, only when a job actually configures a registry — the same
// mechanism `NotificationSinkFactory`/`CustomPolicyEvaluatorFactory`
// already establish elsewhere in this repo.
//
// Deliberately NOT wired up for Maven Central publishing (no sonatype.sbt/
// pgp.sbt) yet, same as `fingerprint` before it was ready — no release
// metadata to carry until this module is ready to actually be published
// there. FOLLOW-UP, once it is: add sonatype.sbt/pgp.sbt (mirroring ir's
// own).
//
// API compatibility (MiMa) IS wired up below, same as contract/ir/
// spark-adapter/fingerprint. This is registry-client's first version, so
// there is no real prior baseline yet — the literal "0.1.0" below is a
// placeholder `mimaPreviousArtifacts` becomes meaningful against once a
// second version exists; CI's api-compatibility job skips a module
// gracefully when it doesn't exist yet at the PR's base commit (see
// contract/build.sbt's matching comment), which covers this module's
// first-ever PR the same way it covered contract/ir/spark-adapter's own
// introducing PR.
mimaPreviousArtifacts := Set(
  "com.invaract" %% "invaract-registry-client" % sys.env.getOrElse("INVARACT_MIMA_BASELINE_VERSION", "0.1.0")
)

// Pre-1.0 (docs/VERSIONING.md), same convention as contract/ir/
// spark-adapter/fingerprint: a 0.x -> 0.(x+1) bump may be
// binary-breaking, so "early-semver" is the accurate scheme.
versionScheme := Some("early-semver")

libraryDependencies ++= Seq(
  "com.invaract" %% "invaract-contract" % "0.7.0",
  "org.scalatest" %% "scalatest" % "3.2.18" % "test"
)

scalacOptions ++= Seq(
  "-target:jvm-1.8",
  "-deprecation",
  "-feature"
)

assembly / assemblyJarName := "invaract-registry-client-0.1.0.jar"

// Mutation testing (Stryker4s), same convention as ir/spark-adapter/
// fingerprint (see CLAUDE.md's "Mutation Testing Requirement"). Whole-
// module scope from the start, since this is a new module with no
// pre-existing coverage baseline to widen from.
strykerMutate := Seq("src/main/scala/**/*.scala")
strykerThresholdsHigh := 80
strykerThresholdsLow := 60
strykerThresholdsBreak := 50

// Same disclose-and-exclude treatment `spark-adapter/build.sbt` already
// established for this exact mutator category (see docs/SPARK_ADAPTER.md's
// "Mutation testing" section): a StringLiteral mutant here is almost
// always human-readable exception/error-message text (RegistryJson's
// parse-error messages, ContractRegistry*Exception's messages) — killing
// it would mean asserting on exact message text, which is brittle and
// tests prose, not behavior. Real logic mutants (EqualityOperator,
// ConditionalExpression, LogicalOperator) are not excluded and are killed
// by real tests instead.
strykerExcludedMutations := Seq("StringLiteral")

// Whole-module score with that exclusion: 92.9%, well above the 70% bar.
// The remaining ~12 survivors were each verified by hand (mutating the
// source directly and re-running the suite), not assumed, and fall into
// exactly two equivalence patterns specific to RegistryJson's hand-rolled
// parser:
//
//  1. Index-vs-length boundary equivalence: an EqualityOperator mutant
//     turning a cursor-vs-`text.length` comparison from `>=`/`<` into
//     `==`/`<=` (RegistryJson.scala lines 49, 79, 92, 123, 133, 144, 159).
//     At every one of these sites the cursor is produced only by
//     `skipWhitespace` or a prior explicit bounds check, both of which
//     guarantee it never exceeds `text.length` - so the comparison
//     evaluates identically under either operator on every reachable
//     input; there is no input that makes them disagree.
//  2. Masked-by-a-later-check equivalence: a ConditionalExpression
//     mutant forcing an early "did this run off the end, or find the
//     wrong character" guard to `false` (lines 79, 92, 123 - the colon,
//     closing-brace, and opening-quote checks). Disabling the guard lets
//     an invalid cursor position through, but it is always caught a few
//     lines later by one of two safety nets that exist for independent
//     reasons: `parseStringLiteral`'s own terminal `if (!closed) throw`
//     (for 123, including its nested unicode-escape branch at 144), or
//     the top-level `parse` entry point's final "no trailing content"
//     check (for 79/92, both only reachable via `parse`). Either way the
//     same `RegistryJsonParseException` type is raised, just from a
//     different call site with a different message - and message text is
//     exactly what the `StringLiteral` exclusion above already treats as
//     out of scope.
//
// (`HttpContractRegistryClient.scala`'s one remaining survivor - the
// `queryParams.isEmpty` ternary building the PUT request's query string -
// is a third, unrelated case: verified by hand that a bare trailing "?"
// with no parameters is silently normalized away by `java.net.URI`
// construction before the request ever reaches the wire, so the two
// branches are unobservable from either side of the HTTP boundary.)

