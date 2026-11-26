// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.verification.openlineage

import com.invaract.verification.{ConfigSource, InvaractConf}

import org.scalatest.funsuite.AnyFunSuite

import java.nio.charset.StandardCharsets
import java.nio.file.Files

/** Every OpenLineage setting: its default, and a value it cannot use refused with the setting's name. */
class OpenLineageSettingsSpec extends AnyFunSuite {
  private val noEnv: String => Option[String] = _ => None

  private def file(lines: String*): String = {
    val f = Files.createTempFile("openlineage-", ".properties")
    Files.write(f, lines.mkString("\n").getBytes(StandardCharsets.UTF_8))
    f.toString
  }

  private def from(settings: Map[String, String], env: Map[String, String] = Map.empty) =
    OpenLineageSettings.from(ConfigSource.fromMap(settings), env.get)

  private def parse(properties: (String, String)*): OpenLineageSettings =
    OpenLineageSettings.parse(properties.toMap, None, None, "test", noEnv)

  private def refused(properties: (String, String)*): String =
    intercept[IllegalArgumentException](parse(properties: _*)).getMessage

  test("not asked for: no settings at all, or only a namespace") {
    assert(from(Map.empty).isEmpty)
    assert(from(Map(InvaractConf.OpenLineageNamespace -> "ns")).isEmpty)
    assert(from(Map(InvaractConf.OpenLineageUrl -> "  ")).isEmpty, "a blank URL asks for nothing")
  }

  test("openLineageUrl alone: http with every default") {
    val s = from(Map(InvaractConf.OpenLineageUrl -> "http://marquez:5000/")).get
    assert(s.transport == TransportSettings.Http("http://marquez:5000", "api/v1/lineage", Map.empty, None, gzip = false, 5000L))
    assert(s == OpenLineageSettings(s.transport), "every other setting is its default")
    assert(s.namespace == "default" && s.emission == Emission.PerCheck && !s.failuresOnly && s.parent.isEmpty)
    assert((s.queueCapacity, s.retryMaxAttempts, s.retryInitialBackoffMs, s.retryMaxBackoffMs) == ((1000, 3, 200L, 5000L)))
    assert((s.circuitFailures, s.circuitCooldownMs, s.maxEventBytes, s.shutdownTimeoutMs) == ((5, 30000L, 1048576, 5000L)))
    assert((s.schemaMaxFields, s.columnLineageMaxFields, s.deadLetterPath, s.disabledFacets) == ((1000, 1000, None, Set.empty)))
  }

  test("the configuration file sets everything; openLineageUrl and openLineageNamespace win over its own") {
    val path = file(
      "transport=http", "url=http://file-url:1", "endpoint=/custom/path", "header.X-Team=orders", "compression=GZIP",
      "timeoutMs=250", "namespace=file-ns", "jobName=orders_daily", "queue.capacity=7", "retry.maxAttempts=4",
      "retry.initialBackoffMs=10", "retry.maxBackoffMs=40", "circuit.failures=2", "circuit.cooldownMs=99",
      "deadLetter.path=/tmp/dl.jsonl", "maxEventBytes=2048", "shutdownTimeoutMs=123", "facets.disabled=schema, columnLineage",
      "schema.maxFields=3", "columnLineage.maxFields=2", "failuresOnly=true", "emission=endOfRun"
    )
    val s = from(Map(InvaractConf.OpenLineageConfig -> path, InvaractConf.OpenLineageUrl -> "https://setting-url", InvaractConf.OpenLineageNamespace -> "setting-ns")).get
    assert(s.transport == TransportSettings.Http("https://setting-url", "custom/path", Map("X-Team" -> "orders"), None, gzip = true, 250L))
    assert(s == OpenLineageSettings(s.transport, "setting-ns", Some("orders_daily"), None, 7, 4, 10L, 40L, 2, 99L, Some("/tmp/dl.jsonl"), 2048, 123L,
      Set("schema", "columnLineage"), 3, 2, failuresOnly = true, Emission.EndOfRun))
    val fileOnly = from(Map(InvaractConf.OpenLineageConfig -> path)).get
    assert(fileOnly.transport.asInstanceOf[TransportSettings.Http].url == "http://file-url:1" && fileOnly.namespace == "file-ns")
  }

  test("file and console transports") {
    assert(parse("transport" -> "file", "path" -> "/tmp/ol.jsonl").transport == TransportSettings.File("/tmp/ol.jsonl"))
    assert(parse("transport" -> "CONSOLE").transport == TransportSettings.Console)
    assert(refused("transport" -> "file").contains("the file transport needs a 'path'"))
    assert(refused("transport" -> "kafka").contains("'transport' must be http, file or console, not 'kafka'"))
    assert(refused().contains("no transport"))
  }

  test("the URL must be http(s) with a host; the message never shows its credentials") {
    assert(refused("url" -> "ftp://h").contains("must be http:// or https://"))
    assert(refused("url" -> "http:///nohost").contains("with a host"))
    assert(refused("url" -> "not a url").contains("is not a URL"))
    val m = refused("url" -> "ftp://user:secret@h")
    assert(m.contains("ftp://***@h") && !m.contains("secret"))
    assert(refused("transport" -> "http").contains("the http transport needs a URL"))
  }

  test("the API key comes from the environment only: apiKeyEnv, else OPENLINEAGE_API_KEY; never shown") {
    def http(s: OpenLineageSettings) = s.transport.asInstanceOf[TransportSettings.Http]
    val named = OpenLineageSettings.parse(Map("url" -> "http://h", "apiKeyEnv" -> "MY_KEY"), None, None, "t", Map("MY_KEY" -> "k1", "OPENLINEAGE_API_KEY" -> "k2").get)
    assert(http(named).apiKey.contains("k1"))
    val default = OpenLineageSettings.parse(Map("url" -> "http://h"), None, None, "t", Map("OPENLINEAGE_API_KEY" -> "k2").get)
    assert(http(default).apiKey.contains("k2"))
    assert(http(OpenLineageSettings.parse(Map("url" -> "http://h"), None, None, "t", Map("OPENLINEAGE_API_KEY" -> "").get)).apiKey.isEmpty)
    val missing = intercept[IllegalArgumentException](OpenLineageSettings.parse(Map("url" -> "http://h", "apiKeyEnv" -> "NOPE"), None, None, "t", noEnv))
    assert(missing.getMessage.contains("'apiKeyEnv' names environment variable 'NOPE', which is not set"))
    assert(!named.toString.contains("k1") && named.toString.contains("apiKey=set"), "toString never shows the key")
    assert(http(default.copy(transport = http(default).copy(apiKey = None))).toString.contains("apiKey=none"))
    val both = intercept[IllegalArgumentException](OpenLineageSettings.parse(Map("url" -> "http://h", "header.authorization" -> "x"), None, None, "t", Map("OPENLINEAGE_API_KEY" -> "k").get))
    assert(both.getMessage.contains("keep one"))
    assert(http(OpenLineageSettings.parse(Map("url" -> "http://h", "header.Authorization" -> "Basic x"), None, None, "t", noEnv)).headers == Map("Authorization" -> "Basic x"))
  }

  test("header names must be tokens; an empty one is ignored") {
    assert(refused("url" -> "http://h", "header.Bad Name" -> "v").contains("'header.Bad Name' is not a valid header name"))
    assert(parse("url" -> "http://h", "header." -> "v").transport.asInstanceOf[TransportSettings.Http].headers.isEmpty)
  }

  test("numbers: whole, and not below their minimum") {
    assert(refused("url" -> "http://h", "timeoutMs" -> "fast").contains("'timeoutMs' must be a whole number, not 'fast'"))
    assert(refused("url" -> "http://h", "timeoutMs" -> "0").contains("'timeoutMs' must be at least 1, not 0"))
    assert(parse("url" -> "http://h", "timeoutMs" -> "1").transport.asInstanceOf[TransportSettings.Http].timeoutMs == 1L)
    assert(refused("url" -> "http://h", "queue.capacity" -> "0").contains("at least 1"))
    assert(parse("url" -> "http://h", "queue.capacity" -> "1").queueCapacity == 1)
    assert(refused("url" -> "http://h", "queue.capacity" -> "3000000000").contains("at most 2147483647"))
    assert(refused("url" -> "http://h", "maxEventBytes" -> "1023").contains("at least 1024"))
    assert(parse("url" -> "http://h", "maxEventBytes" -> "1024").maxEventBytes == 1024)
    assert(parse("url" -> "http://h", "retry.initialBackoffMs" -> "0", "schema.maxFields" -> "0").schemaMaxFields == 0)
    assert(refused("url" -> "http://h", "retry.maxAttempts" -> "0").contains("'retry.maxAttempts' must be at least 1"))
    assert(refused("url" -> "http://h", "circuit.failures" -> "0").contains("'circuit.failures' must be at least 1"))
  }

  test("numbers: the largest whole number a setting holds is accepted") {
    assert(parse("url" -> "http://h", "queue.capacity" -> "2147483647").queueCapacity == Int.MaxValue)
    assert(refused("url" -> "http://h", "queue.capacity" -> "2147483648").contains("at most 2147483647"))
  }

  test("flags, compression and emission take only their values") {
    assert(refused("url" -> "http://h", "failuresOnly" -> "yes").contains("'failuresOnly' must be true or false, not 'yes'"))
    assert(!parse("url" -> "http://h", "failuresOnly" -> "False").failuresOnly)
    assert(refused("url" -> "http://h", "compression" -> "zstd").contains("'compression' must be none or gzip"))
    assert(!parse("url" -> "http://h", "compression" -> "none").transport.asInstanceOf[TransportSettings.Http].gzip)
    assert(refused("url" -> "http://h", "emission" -> "sometimes").contains("perCheck or endOfRun"))
    assert(parse("url" -> "http://h", "emission" -> "PerCheck").emission == Emission.PerCheck)
  }

  test("an unknown key or facet is refused, naming it, rather than silently doing nothing") {
    assert(refused("url" -> "http://h", "namspace" -> "x").contains("unknown key(s) namspace"))
    assert(refused("url" -> "http://h", "facets.disabled" -> "schema,colunmLineage").contains("unknown facet(s) colunmLineage"))
    assert(OpenLineageSettings.Facets.contains("version") && !OpenLineageSettings.Facets.contains("datasetVersion"))
    val s = parse("url" -> "http://h", "facets.disabled" -> " schema ,, ")
    assert(!s.enabled("schema") && s.enabled("columnLineage"))
  }

  test("the parent run: all three keys, or OPENLINEAGE_PARENT_ID; a run id must be a UUID") {
    val uuid = "01890b4c-bb6b-7a8c-8f5b-2a6c35dd2a0e"
    val s = parse("url" -> "http://h", "parent.runId" -> uuid.toUpperCase, "parent.job.namespace" -> "airflow", "parent.job.name" -> "dag.task")
    assert(s.parent.contains(ParentRun("airflow", "dag.task", uuid)))
    assert(refused("url" -> "http://h", "parent.runId" -> uuid).contains("needs all of parent.runId, parent.job.namespace, parent.job.name"))
    assert(refused("url" -> "http://h", "parent.runId" -> "nope", "parent.job.namespace" -> "a", "parent.job.name" -> "b").contains("parent.runId must be a UUID"))
    def fromEnv(v: String) = OpenLineageSettings.parse(Map("url" -> "http://h"), None, None, "t", Map("OPENLINEAGE_PARENT_ID" -> v).get)
    assert(fromEnv(s"airflow/dag/with/slashes/$uuid").parent.contains(ParentRun("airflow", "dag/with/slashes", uuid)))
    List("nodelimiters", s"/name/$uuid", s"ns//$uuid", "ns/name/", s"ns/$uuid").foreach { bad =>
      assert(intercept[IllegalArgumentException](fromEnv(bad)).getMessage.contains("OPENLINEAGE_PARENT_ID must be"), bad)
    }
    assert(fromEnv("  ").parent.isEmpty)
  }

  test("the namespace: openLineageNamespace, else the file's, else OPENLINEAGE_NAMESPACE, else default") {
    val path = file("url=http://h")
    assert(from(Map(InvaractConf.OpenLineageConfig -> path), Map("OPENLINEAGE_NAMESPACE" -> "env-ns")).get.namespace == "env-ns")
    assert(from(Map(InvaractConf.OpenLineageConfig -> file("url=http://h", "namespace=file-ns")), Map("OPENLINEAGE_NAMESPACE" -> "env-ns")).get.namespace == "file-ns")
    assert(from(Map(InvaractConf.OpenLineageConfig -> path)).get.namespace == "default")
  }

  test("a configuration file that is not there is refused, naming the setting") {
    val m = intercept[IllegalArgumentException](from(Map(InvaractConf.OpenLineageConfig -> "/no/such/file.properties"))).getMessage
    assert(m.contains("'openLineageConfig' names '/no/such/file.properties', which is not a file that exists."))
    val inFile = intercept[IllegalArgumentException](from(Map(InvaractConf.OpenLineageConfig -> file("transport=bogus")))).getMessage
    assert(inFile.startsWith("OpenLineage settings ('") && inFile.contains(".properties')"), "says which file")
    assert(intercept[IllegalArgumentException](from(Map(InvaractConf.OpenLineageUrl -> "ftp://x"))).getMessage.contains("(openLineageUrl)"))
  }

  test("redact removes user:password@ only") {
    assert(OpenLineageSettings.redact("https://u:p@host/x") == "https://***@host/x")
    assert(OpenLineageSettings.redact("https://host/x@y") == "https://host/x@y")
  }
}
