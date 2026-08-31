name := "invaract-spark-adapter"
version := "0.1.0"
scalaVersion := "2.12.18"
organization := "com.example"

// 3.5.1 -> 3.5.7: CVE-2025-54920 (GHSA-jwp6-cvj8-fw65, Spark History
// Server Code Execution) - the Spark History Web UI's overly permissive
// Jackson polymorphic deserialization of event-log data lets an attacker
// with access to the event-log directory inject a malicious JSON payload
// that instantiates arbitrary classes, fixed in 3.5.7/4.0.1. A Direct
// (not transitive) dependency, unlike every other CVE fix in this file -
// there's no dependencyOverrides workaround for a bug in Spark's own
// code, so this is a real version bump, held back from every prior
// batch (see docs/CVE_REMEDIATION.md's worked examples) specifically
// because a Spark bump can shift many other pinned transitive versions
// at once. Checked before touching it, not assumed: fetched
// spark-core_2.12:3.5.7's own published POM and confirmed it still
// declares fasterxml.jackson.version=2.15.2 and
// jackson-module-scala_2.12:2.15.2, identical to 3.5.1 - the
// dependencyOverrides below win regardless of what any POM in the tree
// declares, so this doesn't reopen the Netty->Arrow->Jackson class of
// conflict. Delta/Iceberg/ClickHouse connector versions below are pinned
// independently of sparkVersion and stay as they were; only spark-core/
// spark-sql/spark-hive/spark-avro (Spark's own per-release artifacts)
// move with this bump.
val sparkVersion = "3.5.7"

// Test-scope only, not provided: empirical investigation (see
// docs/SPARK_ADAPTER.md's "Delta Lake support" section) found that Delta
// writes go through Spark's own generic SaveIntoDataSourceCommand +
// DataSourceRegister - both plain, public spark-sql classes already on
// the `provided` Spark dependency above. Translating them needs no
// Delta-specific type at all, so delta-spark is only needed here to spin
// up a real Delta-enabled session to test against (the same role
// com.h2database plays for the JDBC precedent below), never to compile
// or run the main translation code.
// 3.2.0 -> 3.3.3, moved together with the Spark 3.5.1 -> 3.5.7 bump above:
// staying on 3.2.0 while Spark moved to 3.5.7 broke this module's own
// ContractEnforcementRuleSpec (a genuine regression, confirmed by
// isolating the spec and by diffing Spark's DataFrameWriter/
// TableCapabilityCheck bytecode between 3.5.1 and 3.5.7 - the check logic
// itself is byte-identical, so the break is Delta 3.2.0 not having been
// built/tested against anything past Spark 3.5.0's DSv2 write path).
// `.format("delta").saveAsTable()` on a brand-new table started failing
// analysis with "Table ... does not support truncate in batch mode.",
// because Spark's ReplaceTableAsSelect(orCreate = true) gets rewritten to
// an OverwriteByExpression against a placeholder v2 relation whose
// capability set no longer includes TRUNCATE/OVERWRITE_BY_FILTER under
// 3.5.7 for a delta-spark 3.2.0-vintage DeltaCatalog. delta-io/delta's
// own release metadata (LATEST_RELEASED_SPARK_VERSION in build.sbt at
// each tag) shows 3.2.0 was built against Spark 3.5.0 and 3.3.3 against
// 3.5.6 - the closest published delta-spark release to our new Spark
// 3.5.7, and current enough that it no longer hits the previous pin's
// reason (delta-io/delta#3737, a NoSuchMethodError isolated to exactly
// Scala 2.12 + Spark 3.5.1 + Delta 3.2.1, whose own thread names
// upgrading past Spark 3.5.3 as a workaround - moot now at Spark 3.5.7).
// Confirmed by re-running ContractEnforcementRuleSpec (and the rest of
// this module's suite) against 3.3.3 before settling on it - see
// docs/SPARK_ADAPTER.md's Delta section / docs/connectors/delta.md for
// the full citation trail.
val deltaVersion = "3.3.3"

// Same test-scope-only reasoning as Delta above - the shaded "runtime" jar
// for exactly this Spark/Scala combination (3.5_2.12), needed only to spin
// up a real Iceberg-enabled session to test against. Checked the
// connector's own issue tracker before pinning, per Phase 0's "any known
// compatibility issues" step: 1.10.0 had a confirmed real bug on this exact
// combination (Avro 1.12 API used against Spark 3.5's bundled Avro 1.11,
// NoSuchMethodError on org.apache.avro.LogicalTypes.timestampNanos -
// apache/iceberg#14232), fixed via #14292 and folded into the Avro-1.12.1
// upgrade that landed before this version - see docs/SPARK_ADAPTER.md's
// Iceberg section for the citation.
val icebergVersion = "1.11.0"

// Hive support, unlike Delta/Iceberg above, is not an external connector
// library - it's Spark's own first-party integration module, split out of
// spark-sql into a separate artifact (`spark-hive`) precisely so a job
// that never touches Hive doesn't need Hive's metastore-client dependency
// footprint on its classpath. Same test-scope-only reasoning applies for
// the same reason: `enableHiveSupport()` needs this to spin up a real
// Hive-enabled session (an embedded Derby metastore, no external Hive
// install needed for local/test use - the same "no external service
// needed" property Iceberg's Hadoop-catalog test setup has), but the
// actual write-command classes this module recognizes
// (`InsertIntoHiveTable`/`CreateHiveTableAsSelectCommand`, both in
// `org.apache.spark.sql.hive.execution`) are matched by reflection/class-
// name string, the same convention `WriteCommandSupport.deltaRowLevelDml`
// uses for Delta's internal command classes - so no compile-time or
// runtime dependency on spark-hive is needed for a job that never enables
// Hive support. Pinned to the exact same sparkVersion as spark-core/
// spark-sql above (not a separately-versioned artifact) - Spark ships
// spark-hive per-Spark-release, not on its own version line the way
// Delta/Iceberg are.
val sparkHiveVersion = sparkVersion

// Avro support, unlike Parquet/CSV (Spark's own bundled FileFormat
// implementations), is a separate first-party artifact Spark splits out
// of spark-sql - a job that never touches Avro doesn't need Avro's own
// (org.apache.avro) dependency footprint on its classpath. Same
// test-scope-only reasoning as spark-hive above: needed only to spin up
// a real Avro-enabled read/write session to test against; the actual
// write-command shape it produces is the exact same generic
// InsertIntoHadoopFsRelationCommand/CreateDataSourceTableAsSelectCommand/
// WriteToStream family already recognized for Parquet/CSV, requiring no
// Avro-specific type in WriteCommandSupport.scala at all. Pinned to the
// exact same sparkVersion as spark-core/spark-sql/spark-hive above -
// spark-avro ships per-Spark-release, not on its own version line the
// way Delta/Iceberg are.
val sparkAvroVersion = sparkVersion

// ClickHouse support, unlike every prior connector, needs a real ClickHouse
// *server* to test against - not just a session extension/embedded
// metastore. Test-scope-only for the same reason as Delta/Iceberg above:
// spinning up a real clickhouse-spark-runtime-backed catalog session to
// test against, never compiled or run by the main translation code.
// Pinned to 0.10.0 - confirmed the latest release on Maven Central for
// exactly this Spark/Scala combination (clickhouse-spark-runtime-3.5_2.12)
// at onboarding time, per Phase 0's "any known compatibility issues" step
// (no blocking issue found against this exact combination). The real
// ClickHouse *server* itself (not this library) is provisioned by
// `ClickHouseTestServer` (test sources) as a standalone binary subprocess,
// not Docker/testcontainers - see that file's own doc comment for why.
val clickhouseVersion = "0.10.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "io.delta" %% "delta-spark" % deltaVersion % "test",
  "org.apache.spark" %% "spark-hive" % sparkHiveVersion % "test",
  "org.apache.spark" %% "spark-avro" % sparkAvroVersion % "test",
  "org.scalatest" %% "scalatest" % "3.2.18" % "test",
  "org.scalatestplus" %% "scalacheck-1-17" % "3.2.18.0" % "test",
  "org.apache.spark" %% "spark-core" % sparkVersion % "test" classifier "tests",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "test" classifier "tests",
  "com.h2database" % "h2" % "2.2.224" % "test",
  // org.lz4:lz4-java is unmaintained (the upstream project archived) and
  // vulnerable to CVE-2025-12183 (GHSA-vqf4-7m7x-wgfc, out-of-bounds
  // memory read via untrusted compressed input) and CVE-2025-66566
  // (GHSA-cmp6-m4wj-q63q, information leak via an insufficiently-cleared
  // output buffer). No fix was ever published under org.lz4 - confirmed
  // directly against Maven Central's own relocation POM for
  // org.lz4:lz4-java:1.8.1 (`<relocation><groupId>at.yawk.lz4</groupId>
  // </relocation>`), which points at the community fork that continues
  // receiving fixes. Added directly at 1.11.1 (the latest release,
  // covering both CVEs - 1.8.1 alone only fixes the first) rather than
  // relying on Ivy to follow the relocation, which sbt/Ivy has
  // historically been inconsistent about; the org.lz4 coordinate is
  // excluded below. Same `net.jpountz.lz4` Java package namespace as the
  // original (confirmed a drop-in replacement, not a rewrite), so
  // Spark's own shuffle-compression code - a real, exercised code path
  // even under local[*] for any shuffle stage, unlike some of the other
  // overrides in this file - needs no changes to keep working.
  "at.yawk.lz4" % "lz4-java" % "1.11.1" % "test"
)

// Not a plain unconditional entry, unlike every other test dependency
// above - see the "iceberg-spark-runtime-3.5_2.12:1.11.0's own classes"
// comment further down for why. Adding these to libraryDependencies at
// all is enough to break JDK 11: Spark's DataSource lookup uses
// ServiceLoader to scan *every* registered DataSourceRegister provider
// on the classpath (to find whichever one matches the requested format),
// which means simply having iceberg-spark-runtime resolvable is enough
// to make *any* .load()/.csv()/format-based read in *any* test suite -
// not just Iceberg-specific ones - try to load org.apache.iceberg.spark.
// IcebergSource and blow up on JDK 11, confirmed via a real CI failure
// (SparkPlanAdapterSpec's plain CSV-fixture read aborted this way).
// Excluding the dependency itself, not just IcebergConnectorSpec's own
// test run, is what actually fixes that - a per-test-class skip alone
// doesn't remove the jar from the classpath the ServiceLoader scans.
libraryDependencies ++= {
  if (scala.util.Properties.isJavaAtLeast("17"))
    Seq(
      "org.apache.iceberg" % "iceberg-spark-runtime-3.5_2.12" % icebergVersion % "test",
      // Confirmed empirically, not assumed: iceberg-spark-runtime-3.5_2.12's
      // SQL extensions parser (IcebergSparkSqlExtensionsParser.isIcebergProcedure,
      // exercised specifically by `CALL <catalog>.system.<proc>(...)` syntax -
      // Iceberg's maintenance-operation mechanism, e.g. rewrite_data_files/
      // expire_snapshots/rollback_to_snapshot) references scala.jdk.CollectionConverters,
      // a class the runtime jar needs but its own published POM doesn't declare
      // as a dependency - a real gap in Iceberg's own artifact for this
      // Spark/Scala combination, not a bug in this module. Needed here only so
      // this module's own test suite can exercise CALL-based Iceberg
      // maintenance ops against a real session; a real Invaract user running
      // Iceberg CALL procedures in their own job would need this on their
      // runtime classpath too, independent of anything spark-adapter does.
      "org.scala-lang.modules" %% "scala-collection-compat" % "2.13.0" % "test"
    )
  else Seq.empty
}

// Same "exclude the dependency itself, not just the test class" reasoning
// as Iceberg's block above, for a different underlying constraint: this
// module's own ClickHouseTestServer provisions a real ClickHouse *server*
// binary with no supported native Windows build (see that file's doc
// comment), not a JDK-version issue. Excluding clickhouse-spark-runtime
// itself on Windows, not just ClickHouseConnectorSpec.scala's own run,
// avoids the same Iceberg-taught risk: Spark's ServiceLoader-based
// DataSourceRegister lookup scans every provider resolvable on the
// classpath for *any* format-based read, so simply having this jar
// resolvable could affect unrelated tests if it behaves at all
// differently on Windows - not observed, but not worth risking given the
// precedent.
libraryDependencies ++= {
  if (scala.util.Properties.isWin) Seq.empty
  else Seq("com.clickhouse.spark" %% "clickhouse-spark-runtime-3.5" % clickhouseVersion % "test")
}

// The ClickHouse connector's .writeTo(...) path serializes batches via
// Arrow (its own bulk-load mechanism, not a generic Spark one - Delta/
// Iceberg's own .writeTo() tests never hit this). Spark 3.5.1 bundles
// arrow-vector/arrow-memory-* 12.0.1 (confirmed via the resolved test
// classpath), which predates a real, external JDK 21 incompatibility:
// JDK 21 changed DirectByteBuffer's private constructor signature from
// (long, int) to (long, long), and Arrow's MemoryUtil.directBuffer()
// reflectively depends on the old one - confirmed via a real
// UnsupportedOperationException on this exact combination, not assumed
// (apache/arrow#35053, fixed in Arrow 13.0.0). Not fixable via
// --add-opens (both java.base/java.nio and jdk.unsupported/sun.misc are
// already open below; the failure is a missing constructor overload, not
// a reflective-access denial).
//
// Bumped again, from the 14.0.1 this override originally landed at, to
// 17.0.0 - not for the JDK 21 fix (already covered by anything >= 13.0.0)
// but because 14.0.1's `arrow-memory-netty` reflectively reaches into
// Netty-internal `PoolArena` fields in a way that broke against *every*
// Netty version this module tried pinning for the CVE-2025-24970/
// CVE-2026-33871 fixes below (4.1.105.Final and then 4.1.132.Final both
// produced the identical `NoSuchFieldError: Class io.netty.buffer.PoolArena
// does not have member field 'int chunkSize'` on every ClickHouse write
// test). Root-caused, not just retried: Netty's own PR #13613 restructured
// `PoolArena` to hold `SizeClasses` as a field instead of extending it,
// landing somewhere around Netty 4.1.7x - meaning arrow-memory-netty
// 14.0.1 was never going to work with *any* Netty version modern enough to
// carry the CVE fixes, independent of which one was tried. Arrow's own
// issue tracker (apache/arrow#36713, apache/arrow#39265) confirms this is
// a known arrow-memory-netty/Netty compatibility break, resolved from
// Arrow's side in 17.0.0 - re-verified against this module's own full
// suite before relying on the issue tracker alone (see
// docs/CVE_REMEDIATION.md's worked examples for the run this landed on).
// Overridden for the *test* classpath only, same as before: Arrow itself
// is never a compile/runtime dependency of this module, only pulled in
// transitively by Spark's `provided`/test dependencies, so this does not
// affect the shipped spark-adapter jar's runtime behavior for real users.
dependencyOverrides ++= Seq(
  "org.apache.arrow" % "arrow-vector" % "17.0.0",
  "org.apache.arrow" % "arrow-memory-core" % "17.0.0",
  "org.apache.arrow" % "arrow-memory-netty" % "17.0.0",
  // Confirmed via a real test failure, not assumed: Arrow 17.0.0's own
  // dependency management pulls jackson-core/jackson-databind/
  // jackson-annotations 2.17.1, which wins eviction over Spark 3.5.1's
  // own 2.15.2 - and Spark's `jackson-module-scala_2.12` enforces a
  // strict version check on init: `JsonMappingException: Scala module
  // 2.15.2 requires Jackson Databind version >= 2.15.0 and < 2.16.0 -
  // Found jackson-databind version 2.17.1`. Since that check runs in a
  // static initializer Spark's own error-formatting machinery depends on
  // (org.apache.spark.ErrorClassesJsonReader), a mismatch here breaks
  // nearly every suite in the module, not just Arrow-adjacent ones - so
  // whatever these four are pinned to, they move together, never
  // partially, the same discipline as the Netty family above.
  //
  // 2.15.2 -> 2.18.8: two jackson-databind CVEs found in a later alert
  // batch - CVE-2026-54512 (PolymorphicTypeValidator bypass via generic
  // type parameters - a type ID like "java.util.ArrayList<com.evil.Gadget>"
  // only validates the raw container class, not the nested type argument)
  // and CVE-2026-54513 (a parallel bypass via allowIfSubTypeIsArray -
  // an array's component type isn't validated, only that the value is
  // *an* array) - plus one jackson-core CVE (GHSA-r7wm-3cxj-wff9, an
  // incomplete-fix follow-up to GHSA-72hv-8253-57qq: the async parser's
  // maxNumberLength limit isn't enforced when a number's digits arrive
  // split across chunks and the buffer is still mid-accumulation).
  // 2.18.8 is the complete fix for all three (the jackson-core CVE's
  // first advisory was only partially fixed at 2.18.6). Bumped
  // jackson-module-scala to the matching 2.18.8 alongside the other
  // three, rather than just raising the pin on the first three again -
  // that's what actually resolves the strict-version-check conflict from
  // the paragraph above, this time in the other direction (Jackson
  // ahead of what jackson-module-scala's own pin expects, instead of
  // behind it).
  "com.fasterxml.jackson.core" % "jackson-core" % "2.18.8",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.18.8",
  "com.fasterxml.jackson.core" % "jackson-annotations" % "2.18.8",
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.18.8",
  // CVE remediation (see docs/CVE_REMEDIATION.md) for transitive jars
  // pulled in by Spark/Delta/Hive's own dependency trees - same
  // dependencyOverrides pattern as Arrow above, not a change to what this
  // module compiles or ships (all four coordinates below arrive via
  // `provided`/`test`-scope Spark deps, confirmed via
  // `sbt Test/dependencyTree`), just what version lands on the classpath
  // this module's tests actually run against.
  //
  // 1.11.2 -> 1.11.4: CVE-2024-47561 (GHSA-r7pg-v2c8-mfg3, CVSS 9.3,
  // arbitrary code execution when parsing an untrusted Avro schema),
  // fixed in 1.11.4/1.12.0. 1.11.4 chosen over 1.12.0 to stay in the same
  // minor line Spark 3.5.1 already resolves (org.apache.avro:avro:1.11.2,
  // confirmed via dependencyTree), minimizing behavioral drift for a
  // security-only patch release.
  "org.apache.avro" % "avro" % "1.11.4",
  // 3.6.3 -> 3.9.2: CVE-2023-44981 (authorization bypass when SASL Quorum
  // Peer authentication is enabled - an attacker omits the instance part
  // of the SASL auth ID to bypass the server-list check). 3.9.2 is one of
  // the advisory's own named fixed releases (3.7.2/3.8.3/3.9.1+, with
  // 3.9.2/3.8.4 called out as the recommended patch). This module only
  // ever uses ZooKeeper as a transitive client library pulled in by
  // Spark/Hive - nothing here runs a quorum or configures SASL peer auth -
  // but the classpath should carry the fixed version regardless.
  //
  // 3.9.2 -> 3.9.5: two more CVEs found in a later alert batch, both
  // fixed within the 3.9.x line this module is already on. CVE-2026-24308
  // (ZKConfig logs configuration values, including potential credentials/
  // connection strings, at INFO level - fixed 3.9.5) and CVE-2024-51504
  // (Admin Server's IPAuthenticationProvider trusts a spoofable
  // X-Forwarded-For header for IP-based auth - fixed 3.9.3, so 3.9.5
  // covers it too). Same "transitive, unexercised client library" caveat
  // as above - this module runs no ZooKeeper Admin Server and does no
  // config-value logging of its own - but the classpath should still
  // carry the fixed version.
  "org.apache.zookeeper" % "zookeeper" % "3.9.5",
  // Netty pinned to a single consistent version across every io.netty
  // artifact this tree resolves (confirmed the full set via
  // `sbt Test/dependencyTree` - `netty-tcnative-*` excluded, since those
  // use OpenSSL's own version line, not Netty's 4.1.x one, and no CVE was
  // flagged against them). Two real reasons to pin explicitly rather than
  // let Ivy pick a winner, both learned the hard way in this module:
  //
  //  1. **Consistency, not just currency**: the ZooKeeper 3.9.2 override
  //     above, on its own, pulled Netty 4.1.105.Final for 9 of these
  //     artifacts while everything else stayed on 4.1.96.Final - Ivy
  //     doesn't evict cleanly across a tree this deep, so both jars ended
  //     up on the classpath simultaneously. Arrow's `arrow-memory-netty`
  //     (pinned above for the JDK 21 fix) reflectively reaches into
  //     Netty-internal `PoolArena` fields in a way that only works
  //     against the exact version it was validated against:
  //     `NoSuchFieldError: Class io.netty.buffer.PoolArena does not have
  //     member field 'int chunkSize'` on every ClickHouse write test the
  //     first time this happened. Every `io.netty` artifact in the tree
  //     must move together, never partially.
  //  2. **CVE remediation** (see docs/CVE_REMEDIATION.md): 4.1.96.Final
  //     itself was vulnerable to CVE-2025-24970 (SslHandler packet
  //     validation, fixed 4.1.118.Final) and CVE-2026-33871 (HTTP/2
  //     CONTINUATION-frame flood DoS, fixed 4.1.132.Final) - the first
  //     bump this override made. A later alert batch found four more,
  //     all fixed at or below 4.1.136.Final: CVE-2025-55163 (MadeYouReset
  //     HTTP/2 DDoS - malformed control frames bypass the max-concurrent-
  //     streams limit, fixed 4.1.124.Final), CVE-2026-44249 (IPv6 subnet
  //     filter bypass via incorrect comparator masking in
  //     IpSubnetFilterRule, fixed 4.1.135.Final), and two ByteBuf-leak/
  //     infinite-loop DoS bugs in SpdyHttpDecoder and Bzip2Decoder (both
  //     fixed 4.1.136.Final). 4.1.136.Final is the highest of all six fix
  //     floors, so it covers every one of them.
  //
  // 4.1.132.Final (this override's first CVE-motivated target) was tried
  // against arrow-memory-netty 14.0.1 and failed with the identical
  // PoolArena error from lesson 1 - see the Arrow override's own comment
  // above for the root cause and the fix, which was bumping Arrow to
  // 17.0.0, not backing off Netty. The later bump to 4.1.136.Final (a
  // 4-patch-version delta, not a 36-version jump like 96->132) still gets
  // the same full-suite check before merging, per docs/CVE_REMEDIATION.md
  // section 5 - never assumed safe just because a bigger jump already
  // cleared.
  "io.netty" % "netty-all" % "4.1.136.Final",
  "io.netty" % "netty-buffer" % "4.1.136.Final",
  "io.netty" % "netty-codec" % "4.1.136.Final",
  "io.netty" % "netty-codec-http" % "4.1.136.Final",
  "io.netty" % "netty-codec-http2" % "4.1.136.Final",
  "io.netty" % "netty-codec-socks" % "4.1.136.Final",
  "io.netty" % "netty-common" % "4.1.136.Final",
  "io.netty" % "netty-handler" % "4.1.136.Final",
  "io.netty" % "netty-handler-proxy" % "4.1.136.Final",
  "io.netty" % "netty-resolver" % "4.1.136.Final",
  "io.netty" % "netty-transport" % "4.1.136.Final",
  "io.netty" % "netty-transport-classes-epoll" % "4.1.136.Final",
  "io.netty" % "netty-transport-classes-kqueue" % "4.1.136.Final",
  "io.netty" % "netty-transport-native-epoll" % "4.1.136.Final",
  "io.netty" % "netty-transport-native-kqueue" % "4.1.136.Final",
  "io.netty" % "netty-transport-native-unix-common" % "4.1.136.Final",
  // CVE-2022-46751 (GHSA-hedq-r4mx-jhh8, XXE - Ivy's XML parsing of its
  // own config/Ivy files/Maven POMs allowed external DTD expansion),
  // fixed in 2.5.2. Confirmed via `sbt Test/dependencyTree` that 2.5.1 is
  // this module's actual resolved winner (2.4.0 already evicted by it).
  "org.apache.ivy" % "ivy" % "2.5.2",
  // 0.12.0 -> 0.13.0 ONLY, not further. Pulled in transitively by
  // spark-hive's Hive 2.3.9 dependency tree (the only module libthrift
  // appears in - confirmed via `sbt Test/dependencyTree`, same
  // Thrift-based Hive metastore client path as the Derby entry below).
  // Two real CVEs here, and - like Derby - a real Hive-compatibility wall
  // found by testing, not assumed:
  //
  //  - CVE-2019-0205 (loop with an unreachable exit condition - a
  //    malicious payload can put a client/server into an infinite loop),
  //    fixed in 0.13.0. Safe to take.
  //  - CVE-2020-13949 (a short malicious RPC message can trigger a large
  //    memory allocation), fixed in 0.14.0 - NOT safe to take. Tried it
  //    first; broke HiveConnectorSpec outright with
  //    `NoClassDefFoundError: org/apache/thrift/transport/TFramedTransport`.
  //    Confirmed by inspecting the actual jars (`unzip -l`) across every
  //    0.1x release: 0.13.0 still has
  //    `org/apache/thrift/transport/TFramedTransport.class`; 0.14.0
  //    onward moved it to `org/apache/thrift/transport/layered/`
  //    (0.14.1/0.14.2 confirmed same). Hive 2.3.9's compiled code
  //    references the pre-0.14.0 package by name, exactly like Derby's
  //    EmbeddedDriver - no libthrift release both fixes CVE-2020-13949
  //    and keeps that package path.
  //
  // Accepted risk for CVE-2020-13949 (see docs/CVE_REMEDIATION.md section
  // 3): not reachable through this module's own tests regardless -
  // HiveConnectorSpec's `enableHiveSupport()` session sets no
  // `hive.metastore.uris`, so it runs Hive's *embedded* metastore
  // (in-process calls, per Hive's own architecture), never a real Thrift
  // RPC server that could receive the "malicious RPC client" payload this
  // CVE describes.
  //
  // A third CVE, found in a later alert batch: CVE-2026-43869 (CWE-297,
  // Improper Validation of Certificate with Host Mismatch -
  // TSSLTransportFactory.java's Java TLS transport skips hostname
  // verification, so a client will trust a certificate issued for the
  // wrong host), fixed in 0.23.0. Not even attempted - 0.23.0 is far
  // past 0.14.0, the exact point already confirmed above to break Hive
  // 2.3.9's package expectations, so it carries the same packaging-break
  // risk at a larger version delta, with no reason to expect it resolved
  // itself in between. Also accepted risk, for an even more direct
  // reachability reason than CVE-2020-13949 above: this CVE is
  // specifically about validating the hostname on a *TLS* Thrift
  // connection, and
  // HiveConnectorSpec's embedded metastore is not just non-networked but
  // never establishes a real socket connection of any kind, TLS or
  // otherwise - there's no certificate to mis-validate.
  "org.apache.thrift" % "libthrift" % "0.13.0",
  // 0.25 -> 2.0.3 (via 0.27). CVE-2024-36114 (GHSA-973x-65j7-xcf4) - every
  // Aircompressor decompressor (LZ4/LZO/Snappy/Zstandard) used
  // sun.misc.Unsafe for unchecked out-of-bounds memory access, malformed
  // input turning into a JVM crash or a leak of adjacent process memory;
  // 0.26 alone wasn't sufficient (the advisory names 0.27 as the real
  // fix), and 0.27 is the same version Spark's own upstream moved to for
  // the 3.5.x line (SPARK-48494, backported to branch-3.5). A later
  // alert batch found a second, more specific issue still present at
  // 0.27: CVE-2025-67721 (GHSA-vx9q-rhv9-3jvg) - a crafted zero-offset
  // input makes the Snappy/LZ4 decompressors copy from not-yet-written
  // positions in a *reused* output buffer, leaking prior buffer contents
  // - fixed in 2.0.3 (the project renumbered from the 0.x line directly
  // to 2.0.x; nothing published in between). Checked the jump for a
  // Derby/Thrift-style repackaging break before trusting it, not
  // assumed: `unzip -l` on both jars shows an identical class list,
  // including the io.airlift.compress.hadoop adapter package Spark's own
  // codec integration actually touches - same classes, same names, in
  // both versions.
  "io.airlift" % "aircompressor" % "2.0.3",
  // 3.14.0 -> 3.18.0: CVE-2025-48924 (GHSA-j288-q9x7-2f5v) -
  // ClassUtils.getClass(...) recurses without a depth limit and can
  // StackOverflowError on a sufficiently long class-name input.
  "org.apache.commons" % "commons-lang3" % "3.18.0",
  // 1.1.10.3 -> 1.1.10.4: CVE-2023-43642 (GHSA-55g7-9cwv-5qfv) -
  // SnappyInputStream has no upper-bound check on the declared chunk
  // length, so a crafted input can force an inappropriately large heap
  // allocation (OutOfMemoryError DoS).
  "org.xerial.snappy" % "snappy-java" % "1.1.10.4"
)

// NOT overridden, unlike everything above - commons-lang:commons-lang
// (the pre-package-rename 2.x line, distinct from org.apache.commons:
// commons-lang3 above - they coexist on the classpath and are not
// interchangeable) has CVE-2025-48924 too (same ClassUtils.getClass(...)
// recursion bug, in the 2.x codebase this was forked from) with no
// available fix: the CVE's affected range is 2.0-2.6, this module
// resolves the actual last-ever 2.x release (2.6, confirmed via
// `sbt Test/dependencyTree`), and the fix landed only in commons-lang3
// 3.18.0 above - there is no 2.7 and never will be, the 2.x line is EOL
// (confirmed: multiple downstream trackers list "no fix planned" for the
// legacy 2.x branch). Excluding it outright, the way jackson-mapper-asl
// was excluded above, was considered and rejected: unlike that case,
// commons-lang 2.x's org.apache.commons.lang package (pre-rename) is a
// real, separate namespace from commons-lang3's org.apache.commons.lang3
// - old Hadoop-ecosystem code transitively pulling this in may reference
// org.apache.commons.lang.* classes directly that commons-lang3 does not
// provide, so removing the jar risks a NoClassDefFoundError this module
// has no way to verify is safe without exercising every code path that
// might reach it. Accepted risk (see docs/CVE_REMEDIATION.md section 3).

// NOT overridden, unlike Avro/ZooKeeper/Netty above - CVE-2022-46337
// (GHSA-rcjc-c4pj-xxrp, LDAP injection in Derby's
// LDAPAuthenticationSchemeImpl) has no compatible fix for this module's
// Derby use, on any JDK. Investigated, not assumed:
//
//  - Confirmed against Maven Central's own version listing that
//    10.17.1.0 is the *only* published fixed coordinate at all - the
//    advisory names 10.14.3/10.15.2.1/10.16.1.2 as lower-JDK backports,
//    but none of those three were ever actually published (see
//    DERBY-7178, "Wrong 10.14 backport patch version"); only
//    10.14.1.0/10.14.2.0 (both still vulnerable), 10.15.1.3, 10.15.2.0,
//    10.16.1.1 (also vulnerable per the advisory's own ranges), and
//    10.17.1.0 exist.
//  - Tried 10.17.1.0 anyway (JDK 21+-only, since Derby's own release
//    notes say 10.17 doesn't support Java below 21) and it broke
//    HiveConnectorSpec outright: confirmed by inspecting the actual jars
//    (`unzip -l`) that 10.17.1.0 no longer contains
//    `org/apache/derby/jdbc/EmbeddedDriver.class` at all - Derby
//    restructured its packaging between these releases - while Hive
//    2.3.9's own metastore code (DataNucleus/JDO, not this repo's code)
//    hardcodes exactly that class name as its default
//    `javax.jdo.option.ConnectionDriverName`. The real failure:
//    `DatastoreDriverNotFoundException: The specified datastore driver
//    ("org.apache.derby.jdbc.EmbeddedDriver") was not found in the
//    CLASSPATH`. Fixing this would mean patching Hive's own metastore
//    config, not a dependency bump - out of scope for a transitive CVE
//    override, and this module's tests only use the embedded metastore
//    as test-scope plumbing (see HiveConnectorSpec's own doc comment),
//    not something worth that risk to fix.
//
// Accepted risk (see docs/CVE_REMEDIATION.md section 3): this module's
// own test setup never configures LDAP authentication at all -
// HiveConnectorSpec's embedded metastore JDBC URL
// (`jdbc:derby:;databaseName=...;create=true`) sets no
// `derby.authentication.provider`, so the specific vulnerable code path
// (LDAPAuthenticationSchemeImpl) is never reachable through this module's
// tests regardless of version. Re-check when Hive's own metastore client
// moves off Derby 10.14.x-era packaging expectations (a Spark/Hive
// version bump, not something fixable here).

// org.codehaus.jackson:jackson-mapper-asl:1.9.13, pulled in transitively
// by spark-hive's own Hive 2.3.9 dependency tree (hive-common/hive-exec/
// hive-metastore - confirmed the single occurrence via
// `sbt Test/dependencyTree`), has CVE-2019-10202 (GHSA-c27h-mcmw-48hv,
// CVSS 9.8, unsafe polymorphic deserialization) with no available fix:
// this is old Jackson 1.x (Codehaus, not FasterXML), abandoned since
// 2013 - 1.9.13 is the artifact's last-ever release, so there is no
// version to override to (see docs/CVE_REMEDIATION.md section 3's
// "no available patched version" case). Excluded outright rather than
// accepted: no source file in this module imports
// org.codehaus.jackson.* directly (confirmed via grep), so it's dead
// weight pulled in for Hive-internal JSON serde this module's own tests
// never exercise, not something removing it can plausibly break.
excludeDependencies ++= Seq(
  ExclusionRule("org.codehaus.jackson", "jackson-mapper-asl"),
  // See the at.yawk.lz4 addition in libraryDependencies above for the
  // full CVE detail and why the fork exists - this is the other half of
  // that fix, removing the unmaintained org.lz4 coordinate so only the
  // fork's classes are on the classpath.
  ExclusionRule("org.lz4", "lz4-java")
)

// NOT overridden - com.google.guava:guava:16.0.1 (CVE-2018-10237,
// GHSA-w787-jrh4-2xh8, unbounded memory allocation via
// AtomicDoubleArray/CompoundOrdering serialization, fixed 24.1.1+).
// Traced to its actual source, not assumed: `sbt Test/dependencyTree`
// shows it arrives via org.apache.curator:curator-client:2.13.0, which
// backs Spark's ZooKeeper-based standalone-cluster recovery mode
// (`spark.deploy.recoveryMode=ZOOKEEPER`) - infrastructure this module
// never configures or exercises, since CLAUDE.md's Execution Model has
// every test and the demo harness run against a `local[*]` master, which
// never touches Curator/cluster-recovery code at all. That reachability
// gap is also why a full-suite pass here couldn't actually prove a bump
// safe the way it did for Netty/Avro/ZooKeeper: if Curator's own
// compiled code (built against Guava 16.0.1's decade-old API surface)
// never gets loaded under local[*], a version conflict wouldn't surface
// as a test failure regardless of whether it's really compatible - a
// green run would be confirming nothing. Given that and the CVE's
// Moderate severity (the lowest-urgency tier in
// docs/CVE_REMEDIATION.md's bucket list), left as an accepted risk
// rather than pushed through on an assumption a passing suite can't
// back up. Re-evaluate if this module ever needs to exercise Spark's
// cluster-recovery code paths for real.

unmanagedJars in Compile += file("../ir/target/scala-2.12/invaract-ir-0.1.0.jar")
unmanagedJars in Compile += file("../contract/target/scala-2.12/invaract-contract-0.1.0.jar")

assembly / assemblyJarName := "invaract-spark-adapter-0.1.0.jar"
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

Test / parallelExecution := false

// iceberg-spark-runtime-3.5_2.12:1.11.0's own classes are compiled to
// class file version 61 (Java 17) - confirmed via CI, not assumed:
// UnsupportedClassVersionError on org.apache.iceberg.spark.SparkCatalog/
// IcebergSource under this repo's JDK-11 test matrix leg
// (.github/workflows/test.yml). A genuine, external constraint of the
// library itself, not something fixable here - unlike Delta 3.2.0 above,
// which loads fine under JDK 11. The dependency itself is excluded under
// JDK <17 above (see that comment for why a per-test-class skip alone
// isn't enough - Spark's ServiceLoader-based DataSourceRegister lookup
// touches every provider on the classpath for *any* format-based read).
// With the dependency gone, IcebergConnectorSpec.scala's own
// org.apache.iceberg/org.apache.spark.sql.connector.iceberg imports
// would fail to *compile* under JDK <17 - so its source file is excluded
// from that build too. Every other spark-adapter source file is
// dependency-free of Iceberg (confirmed by grepping src/ - only this
// file and FailClosedCommands.scala reference it at all, and that one
// only via string literals, never a real import - see its own header
// comment), so nothing else needs excluding. The module's own compiled
// bytecode target (-target:jvm-1.8 below) is unaffected; this is purely
// a test-only dependency's own runtime floor, not a product
// compatibility change.
Test / unmanagedSources / excludeFilter := {
  val icebergExcluded =
    if (scala.util.Properties.isJavaAtLeast("17")) (Test / unmanagedSources / excludeFilter).value
    else (Test / unmanagedSources / excludeFilter).value || "IcebergConnectorSpec.scala"
  // ClickHouse has no supported native Windows server build (a hard
  // platform constraint, unlike Iceberg's JDK-version one above) -
  // ClickHouseTestServer/ClickHouseConnectorSpec.scala are excluded on
  // Windows only. Every other test file is dependency-free of ClickHouse
  // (only these two reference it), so nothing else needs excluding.
  if (scala.util.Properties.isWin)
    icebergExcluded || "ClickHouseTestServer.scala" || "ClickHouseConnectorSpec.scala" || "ClickHouseConnectorProbeSpec.scala"
  else icebergExcluded
}

// Spark reflectively accesses JDK-internal classes (e.g.
// sun.nio.ch.DirectBuffer in org.apache.spark.storage.StorageUtils) that
// JDK 17+'s module system closes off by default. spark-submit's own launch
// scripts inject the necessary --add-opens flags automatically for JDK 17+,
// which is why `./dev/test`'s real spark-submit run needs no changes here;
// a plain `sbt test` JVM gets none of that, so it's reproduced explicitly
// for the forked test JVM below. This is Spark's own documented flag set
// for JDK 17+ compatibility (see spark-defaults.conf.template).
Test / fork := true
Test / javaOptions ++= Seq(
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
  "--add-opens=java.base/java.io=ALL-UNNAMED",
  "--add-opens=java.base/java.net=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
  "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
  "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
  // ClickHouse's Spark connector serializes writes via Apache Arrow
  // (ClickHouseArrowStreamWriter), whose MemoryUtil needs reflective
  // access to sun.misc.Unsafe/DirectByteBuffer's package-private
  // constructor - confirmed empirically (a real
  // UnsupportedOperationException on this forked JDK 21 test JVM, not
  // assumed): sun.misc.Unsafe lives in the jdk.unsupported module, which
  // none of the java.base opens above reach.
  "--add-opens=jdk.unsupported/sun.misc=ALL-UNNAMED"
)

scalacOptions ++= Seq(
  "-target:jvm-1.8",
  "-deprecation",
  "-feature"
)

// Mutation testing (Stryker4s) config: see stryker4s.conf for reporters.
// `mutate`/`thresholds` are set here rather than in stryker4s.conf, whose
// equivalent keys were observed not to take effect via the config file in
// this sbt/plugin version combination - these sbt settings do work.
// Whole-module scope (widened from just StructuralVerifier.scala once the
// initial narrow pass's score was reviewed). `break` is what makes CI's
// mutation-testing job fail when the score regresses below it.
strykerMutate := Seq("src/main/scala/**/*.scala")

// After genuine test uplift closed every real (non-StringLiteral) gap
// this module's coverage tooling can reach, the only mutants still
// surviving are ~80 StringLiteral mutants on human-readable
// message/remediation/type-name text (see docs/SPARK_ADAPTER.md's
// "Mutation testing" section) - the category CLAUDE.md's "Mutation
// Testing Requirement" already names as a documented, acceptable
// exclusion, since a test asserting an exact error-message string is
// brittle and doesn't verify behavior. Excluding it here makes that
// exclusion explicit and repo-wide instead of an ad hoc per-PR judgment
// call, and lets the break threshold reflect the module's real behavioral
// mutation coverage rather than being capped by unrelated prose.
strykerExcludedMutations := Seq("StringLiteral")

// Real measured score after the exclusion above is 91.53% (of total) /
// 93.1% (of covered code) - 54/59 mutants killed, the same 5 documented,
// left-on-purpose survivors as before (JDBCRelation near-equivalence,
// unwrapWriteWrapper's Spark-3.5.1-unreachable branch, and the
// Hive-relation fallback with no metastore available to exercise it
// here). Thresholds below match the incremental PR check's values
// (.github/workflows/test.yml) rather than hugging 91.53% exactly, so a
// small, explainable regression doesn't fail CI outright.
strykerThresholdsHigh := 90
strykerThresholdsLow := 80
strykerThresholdsBreak := 70

// API compatibility (MiMa) - see contract/build.sbt's comment for the full
// rationale (no Maven Central release yet, so CI's `api-compatibility` job
// compares against the PR's own base branch instead) and
// docs/SPARK_ADAPTER.md's "API compatibility" section.
//
// Renamed invariant-spark-adapter -> invaract-spark-adapter by the rebrand
// PR, which is now on the base branch - see contract/build.sbt's comment
// for why this coordinate must match base-ref's own published name.
mimaPreviousArtifacts := Set("com.example" %% "invaract-spark-adapter" % "0.1.0")
