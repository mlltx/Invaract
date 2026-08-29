// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.example.sparkadapter

import java.io.File
import java.net.{HttpURLConnection, URI}
import java.nio.file.{Files, Path, Paths, StandardCopyOption}

/** Provisions and manages a real, standalone ClickHouse server for
  * `ClickHouseConnectorSpec` - not Docker/testcontainers.
  *
  * Every other connector's test infrastructure needed only an embedded
  * session extension (Delta/Iceberg) or an in-process metastore (Hive's
  * embedded Derby) - ClickHouse's connector talks to a genuinely separate
  * server process over HTTP/native protocol, so something has to provide
  * one. Docker/testcontainers (the connector's own upstream test suite
  * uses this) was considered and rejected for this repository's onboarding
  * session specifically: the sandboxed environment doing this pass has no
  * usable Docker daemon (`ulimit: error setting limit (Operation not
  * permitted)`, a hard container restriction), so tests written against
  * testcontainers could never be run or verified locally, only hoped to
  * pass in CI - a real regression from every prior connector's practice
  * of running and confirming its own tests before considering them done.
  *
  * ClickHouse ships a genuine standalone server binary with no daemon
  * dependencies (confirmed directly: downloaded, started, and queried a
  * real server this way during this connector's Phase 0 investigation) -
  * this class launches that binary as a plain subprocess instead.
  *
  * Platform scope: Linux and macOS only. ClickHouse has no supported
  * native Windows server build (unlike JDK 17's Iceberg exclusion, which
  * is a version constraint, this is a hard platform constraint - see
  * `build.sbt`'s exclusion of `ClickHouseConnectorSpec.scala` on Windows).
  * macOS provisioning (`clickhouse-macos`/`clickhouse-macos-aarch64`
  * release assets, confirmed to exist via a direct HTTP check) is
  * implemented but **not independently runtime-verified by this session** -
  * no macOS environment was available to actually start and query the
  * server there the way Linux was. Linux (the platform this session
  * actually ran on) is fully verified: real `CREATE TABLE`/`INSERT`/
  * `SELECT` against a real started server, both during investigation and
  * via this class's own use in `ClickHouseConnectorSpec`.
  */
private[sparkadapter] object ClickHouseTestServer {
  // Pinned to the same LTS release investigated in Phase 0 (25.3.3.42) -
  // deliberately not "latest", the same reasoning as every other pinned
  // test-only dependency in this module (Delta/Iceberg).
  private val ClickHouseRelease = "v25.3.3.42-lts"
  private val ClickHouseVersion = "25.3.3.42"

  private def cacheRoot: Path =
    Paths.get(System.getProperty("user.home"), ".cache", "invaract-test", "clickhouse", ClickHouseVersion)

  private def platformKey: String = {
    val osName = System.getProperty("os.name", "").toLowerCase
    val arch = System.getProperty("os.arch", "").toLowerCase
    val isArm = arch.contains("aarch64") || arch.contains("arm64")
    if (osName.contains("mac")) if (isArm) "macos-aarch64" else "macos"
    else if (osName.contains("linux")) if (isArm) "linux-arm64" else "linux-amd64"
    else throw new UnsupportedOperationException(
      s"ClickHouseTestServer has no supported binary provisioning for os.name=$osName - " +
        "this spec should have been excluded on this platform (see build.sbt)."
    )
  }

  private def downloadUrl(key: String): String = key match {
    case "linux-amd64" =>
      s"https://github.com/ClickHouse/ClickHouse/releases/download/$ClickHouseRelease/clickhouse-common-static-$ClickHouseVersion-amd64.tgz"
    case "linux-arm64" =>
      s"https://github.com/ClickHouse/ClickHouse/releases/download/$ClickHouseRelease/clickhouse-common-static-$ClickHouseVersion-arm64.tgz"
    case "macos" =>
      s"https://github.com/ClickHouse/ClickHouse/releases/download/$ClickHouseRelease/clickhouse-macos"
    case "macos-aarch64" =>
      s"https://github.com/ClickHouse/ClickHouse/releases/download/$ClickHouseRelease/clickhouse-macos-aarch64"
  }

  private def isTgz(key: String): Boolean = key.startsWith("linux")

  /** Downloads (if not already cached) and returns the path to a real,
    * executable `clickhouse` binary for the current platform.
    */
  def binaryPath(): Path = synchronized {
    val key = platformKey
    val binPath = cacheRoot.resolve(key).resolve("clickhouse")
    if (!Files.exists(binPath)) {
      Files.createDirectories(binPath.getParent)
      val url = downloadUrl(key)
      if (isTgz(key)) {
        val tgzPath = cacheRoot.resolve(key).resolve("clickhouse.tgz")
        downloadTo(url, tgzPath)
        extractSingleBinary(tgzPath, binPath)
        Files.deleteIfExists(tgzPath)
      } else {
        downloadTo(url, binPath)
      }
      binPath.toFile.setExecutable(true)
    }
    binPath
  }

  private def downloadTo(url: String, dest: Path): Unit = {
    val conn = new URI(url).toURL.openConnection().asInstanceOf[HttpURLConnection]
    conn.setInstanceFollowRedirects(true)
    conn.setConnectTimeout(30000)
    conn.setReadTimeout(120000)
    val in = conn.getInputStream
    try Files.copy(in, dest, StandardCopyOption.REPLACE_EXISTING)
    finally in.close()
  }

  // The Linux release ships as a .tgz containing usr/bin/clickhouse plus
  // supporting files this test only needs the one binary from - extracted
  // via a plain `tar` subprocess (present on every Linux/macOS CI runner
  // and this sandbox) rather than a Java tar library dependency just for
  // this.
  private def extractSingleBinary(tgz: Path, dest: Path): Unit = {
    val extractDir = tgz.getParent.resolve("extract")
    Files.createDirectories(extractDir)
    val proc = new ProcessBuilder("tar", "xzf", tgz.toString, "-C", extractDir.toString)
      .redirectErrorStream(true)
      .start()
    val exit = proc.waitFor()
    if (exit != 0) {
      val output = scala.io.Source.fromInputStream(proc.getInputStream).mkString
      throw new RuntimeException(s"Failed to extract ClickHouse static build (exit $exit): $output")
    }
    // Matching bare filename "clickhouse" isn't precise enough - the
    // archive also ships a bash-completion script at
    // usr/share/bash-completion/completions/clickhouse with the identical
    // filename (confirmed the hard way: a first attempt at this matched
    // that 1KB text file instead of the real ~580MB binary, and the
    // resulting "server" silently never started). The real binary always
    // lives at .../usr/bin/clickhouse in this archive layout.
    val found = Files.walk(extractDir)
      .filter(p => p.getFileName.toString == "clickhouse" && Files.isRegularFile(p) &&
        p.getParent.getFileName.toString == "bin")
      .findFirst()
    if (!found.isPresent) throw new RuntimeException(s"No usr/bin/clickhouse binary found under $extractDir after extraction")
    Files.move(found.get(), dest, StandardCopyOption.REPLACE_EXISTING)
    deleteRecursively(extractDir.toFile)
  }

  private def deleteRecursively(f: File): Unit = {
    if (f.isDirectory) f.listFiles().foreach(deleteRecursively)
    f.delete()
  }
}

/** One running server instance, bound to caller-chosen ports so parallel
  * test runs (or a developer's own ClickHouse, if any) don't collide.
  */
private[sparkadapter] class ClickHouseTestServer(httpPort: Int, tcpPort: Int) {
  private var process: Process = _
  private val dataDir: Path = Files.createTempDirectory("invaract-clickhouse-data")
  private val configFile: Path = dataDir.resolve("config.xml")

  def start(): Unit = {
    val bin = ClickHouseTestServer.binaryPath()
    Files.write(configFile,
      s"""<clickhouse>
         |  <logger><level>warning</level><console>true</console></logger>
         |  <http_port>$httpPort</http_port>
         |  <tcp_port>$tcpPort</tcp_port>
         |  <path>${dataDir.toString.replace('\\', '/')}/</path>
         |  <mark_cache_size>536870912</mark_cache_size>
         |  <listen_host>127.0.0.1</listen_host>
         |  <users>
         |    <default>
         |      <password></password>
         |      <networks><ip>::/0</ip></networks>
         |      <profile>default</profile>
         |      <quota>default</quota>
         |    </default>
         |  </users>
         |  <profiles><default></default></profiles>
         |  <quotas><default></default></quotas>
         |</clickhouse>
         |""".stripMargin.getBytes("UTF-8")
    )
    process = new ProcessBuilder(bin.toString, "server", s"--config-file=$configFile")
      .redirectErrorStream(true)
      .redirectOutput(dataDir.resolve("server.log").toFile)
      .start()
    waitForReady()
  }

  private def waitForReady(): Unit = {
    val deadline = System.currentTimeMillis() + 30000
    var ready = false
    while (!ready && System.currentTimeMillis() < deadline) {
      try {
        val conn = new URI(s"http://127.0.0.1:$httpPort/ping").toURL.openConnection().asInstanceOf[HttpURLConnection]
        conn.setConnectTimeout(1000)
        conn.setReadTimeout(1000)
        ready = conn.getResponseCode == 200
      } catch { case _: Exception => Thread.sleep(300) }
    }
    if (!ready) throw new RuntimeException(s"ClickHouse server did not become ready within 30s (http port $httpPort)")
  }

  def stop(): Unit = {
    if (process != null) {
      process.destroy()
      if (!process.waitFor(10, java.util.concurrent.TimeUnit.SECONDS)) process.destroyForcibly()
    }
    deleteRecursively(dataDir.toFile)
  }

  private def deleteRecursively(f: File): Unit = {
    if (f.isDirectory) f.listFiles().foreach(deleteRecursively)
    f.delete()
  }
}
