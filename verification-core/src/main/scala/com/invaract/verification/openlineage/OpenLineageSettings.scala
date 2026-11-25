// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.verification.openlineage

import com.invaract.verification.{ConfigSource, InvaractConf}

import java.io.{File, FileInputStream, InputStreamReader}
import java.nio.charset.StandardCharsets
import scala.collection.JavaConverters._

/** Where OpenLineage events go. */
sealed trait TransportSettings

object TransportSettings {

  /** POST to `url` + `endpoint`. `apiKey` is the bearer token's value, read from the environment variable `apiKeyEnv`
    * names; it never appears in a configuration file, and `toString` never shows it. */
  final case class Http(
      url: String,
      endpoint: String,
      headers: Map[String, String],
      apiKey: Option[String],
      gzip: Boolean,
      timeoutMs: Long
  ) extends TransportSettings {
    override def toString: String =
      s"Http(${OpenLineageSettings.redact(url)}, $endpoint, headers=${headers.keys.toList.sorted}, apiKey=${apiKey.fold("none")(_ => "set")}, gzip=$gzip, timeoutMs=$timeoutMs)"
  }

  /** Appends one event per line to `path`. */
  final case class File(path: String) extends TransportSettings

  /** Logs each event at INFO. */
  case object Console extends TransportSettings
}

/** A run of an orchestrator this job's run belongs to (OpenLineage's `parent` run facet). */
final case class ParentRun(namespace: String, name: String, runId: String)

/** Whether each check is published as it happens or only the run's start and end. */
sealed trait Emission
object Emission {
  case object PerCheck extends Emission
  case object EndOfRun extends Emission
}

/** Every OpenLineage setting (docs/OPENLINEAGE.md, section 3), validated: built by `from`, which refuses a value it cannot
  * use, naming the setting, rather than publishing something nobody asked for. */
final case class OpenLineageSettings(
    transport: TransportSettings,
    namespace: String = "default",
    jobName: Option[String] = None,
    parent: Option[ParentRun] = None,
    queueCapacity: Int = 10000,
    retryMaxAttempts: Int = 3,
    retryInitialBackoffMs: Long = 200L,
    retryMaxBackoffMs: Long = 5000L,
    circuitFailures: Int = 5,
    circuitCooldownMs: Long = 30000L,
    deadLetterPath: Option[String] = None,
    maxEventBytes: Int = 1048576,
    shutdownTimeoutMs: Long = 5000L,
    disabledFacets: Set[String] = Set.empty,
    schemaMaxFields: Int = 1000,
    columnLineageMaxFields: Int = 1000,
    failuresOnly: Boolean = false,
    emission: Emission = Emission.PerCheck
) {
  def enabled(facet: String): Boolean = !disabledFacets.contains(facet)
}

object OpenLineageSettings {

  /** The facets a configuration can switch off (`facets.disabled`), by the name they are published under. */
  val Facets: Set[String] = Set(
    "schema", "columnLineage", "dataQualityAssertions", "outputStatistics", "version", "lifecycleStateChange",
    "storage", "symlinks", "catalog", "processing_engine", "parent", "errorMessage", "jobType", "invaract_contract", "invaract_run"
  )

  /** The configuration file's keys; any other is refused, so a misspelt key cannot silently do nothing. */
  private val FixedKeys: Set[String] = Set(
    "transport", "url", "endpoint", "apiKeyEnv", "compression", "timeoutMs", "path", "namespace", "jobName",
    "parent.runId", "parent.job.namespace", "parent.job.name", "queue.capacity", "retry.maxAttempts",
    "retry.initialBackoffMs", "retry.maxBackoffMs", "circuit.failures", "circuit.cooldownMs", "deadLetter.path",
    "maxEventBytes", "shutdownTimeoutMs", "facets.disabled", "schema.maxFields", "columnLineage.maxFields",
    "failuresOnly", "emission"
  )

  /** The settings `config` asks for, or `None` when OpenLineage is not asked for (neither `openLineageUrl` nor
    * `openLineageConfig` set). `environment` is where `apiKeyEnv`, `OPENLINEAGE_NAMESPACE` and `OPENLINEAGE_PARENT_ID`
    * are read. Throws `IllegalArgumentException`, naming the setting, for a value it cannot use. */
  def from(config: ConfigSource, environment: String => Option[String] = name => Option(System.getenv(name))): Option[OpenLineageSettings] = {
    val url = config.get(InvaractConf.OpenLineageUrl).map(_.trim).filter(_.nonEmpty)
    val file = config.get(InvaractConf.OpenLineageConfig).map(_.trim).filter(_.nonEmpty)
    if (url.isEmpty && file.isEmpty) None
    else {
      val properties = file.fold(Map.empty[String, String])(path => load(path, config.describe(InvaractConf.OpenLineageConfig)))
      val namespace = config.get(InvaractConf.OpenLineageNamespace).map(_.trim).filter(_.nonEmpty)
      Some(parse(properties, url, namespace, file.fold(config.describe(InvaractConf.OpenLineageUrl))(f => s"'$f'"), environment))
    }
  }

  /** `properties` read as the configuration file; `url` and `namespace` from the neutral settings win over the file's own.
    * `source` names where the values came from, for error messages. */
  private[openlineage] def parse(
      properties: Map[String, String],
      url: Option[String],
      namespace: Option[String],
      source: String,
      environment: String => Option[String]
  ): OpenLineageSettings = {
    def fail(message: String): Nothing = throw new IllegalArgumentException(s"OpenLineage settings ($source): $message")
    val unknown = properties.keySet.filterNot(k => FixedKeys.contains(k) || k.startsWith("header.")).toList.sorted
    if (unknown.nonEmpty) fail(s"unknown key(s) ${unknown.mkString(", ")}; the keys are listed in docs/OPENLINEAGE.md, section 3")

    def text(key: String): Option[String] = properties.get(key).map(_.trim).filter(_.nonEmpty)
    def long(key: String, default: Long, min: Long): Long = text(key).fold(default) { v =>
      val n = try v.toLong catch { case _: NumberFormatException => fail(s"'$key' must be a whole number, not '$v'") }
      if (n < min) fail(s"'$key' must be at least $min, not $n") else n
    }
    def int(key: String, default: Int, min: Int): Int = {
      val n = long(key, default.toLong, min.toLong)
      if (n > Int.MaxValue) fail(s"'$key' must be at most ${Int.MaxValue}, not $n") else n.toInt
    }
    def flag(key: String): Boolean = text(key).exists { v =>
      v.toLowerCase match {
        case "true"  => true
        case "false" => false
        case _       => fail(s"'$key' must be true or false, not '$v'")
      }
    }

    val effectiveUrl = url.orElse(text("url"))
    val transport = text("transport").map(_.toLowerCase).orElse(effectiveUrl.map(_ => "http")).getOrElse(
      fail("no transport: set 'transport' (http, file or console), or a URL")
    ) match {
      case "http" =>
        val base = effectiveUrl.getOrElse(fail("the http transport needs a URL ('url', or the openLineageUrl setting)"))
        val parsed = try new java.net.URI(base) catch { case e: java.net.URISyntaxException => fail(s"'$base' is not a URL: ${e.getReason}") }
        if (!Set("http", "https").contains(Option(parsed.getScheme).map(_.toLowerCase).getOrElse("")) || Option(parsed.getHost).isEmpty)
          fail(s"the URL must be http:// or https:// with a host, not '${redact(base)}'")
        val keyEnv = text("apiKeyEnv")
        val apiKey = keyEnv match {
          case Some(variable) => Some(environment(variable).filter(_.nonEmpty).getOrElse(fail(s"'apiKeyEnv' names environment variable '$variable', which is not set")))
          case None           => environment("OPENLINEAGE_API_KEY").filter(_.nonEmpty)
        }
        val headers = properties.collect { case (k, v) if k.startsWith("header.") && k.length > "header.".length => k.stripPrefix("header.") -> v }
        headers.keys.foreach { name =>
          if (!name.matches("[!#$%&'*+.^_`|~0-9A-Za-z-]+")) fail(s"'header.$name' is not a valid header name")
          if (name.equalsIgnoreCase("Authorization") && apiKey.isDefined) fail("'header.Authorization' and an API key both set the Authorization header; keep one")
        }
        val gzip = text("compression").map(_.toLowerCase) match {
          case None | Some("none") => false
          case Some("gzip")        => true
          case Some(other)         => fail(s"'compression' must be none or gzip, not '$other'")
        }
        TransportSettings.Http(base.stripSuffix("/"), text("endpoint").getOrElse("api/v1/lineage").stripPrefix("/"), headers, apiKey, gzip, long("timeoutMs", 5000L, 1L))
      case "file" => TransportSettings.File(text("path").getOrElse(fail("the file transport needs a 'path'")))
      case "console" => TransportSettings.Console
      case other => fail(s"'transport' must be http, file or console, not '$other'")
    }

    val disabled = text("facets.disabled").toList.flatMap(_.split(",").map(_.trim).filter(_.nonEmpty)).toSet
    val unknownFacets = (disabled -- Facets).toList.sorted
    if (unknownFacets.nonEmpty) fail(s"'facets.disabled' names unknown facet(s) ${unknownFacets.mkString(", ")}; the facets are ${Facets.toList.sorted.mkString(", ")}")

    val parentKeys = List("parent.runId", "parent.job.namespace", "parent.job.name")
    val parent = parentKeys.map(text) match {
      case List(Some(run), Some(ns), Some(name)) => Some(ParentRun(ns, name, uuid("parent.runId", run, fail)))
      case List(None, None, None)                => environment("OPENLINEAGE_PARENT_ID").map(_.trim).filter(_.nonEmpty).map(parentId(_, fail))
      case _                                     => fail(s"the parent run needs all of ${parentKeys.mkString(", ")}")
    }

    val emission = text("emission").map(_.toLowerCase) match {
      case None | Some("percheck") => Emission.PerCheck
      case Some("endofrun")        => Emission.EndOfRun
      case Some(other)             => fail(s"'emission' must be perCheck or endOfRun, not '$other'")
    }

    OpenLineageSettings(
      transport = transport,
      namespace = namespace.orElse(text("namespace")).orElse(environment("OPENLINEAGE_NAMESPACE").map(_.trim).filter(_.nonEmpty)).getOrElse("default"),
      jobName = text("jobName"),
      parent = parent,
      queueCapacity = int("queue.capacity", 10000, 1),
      retryMaxAttempts = int("retry.maxAttempts", 3, 1),
      retryInitialBackoffMs = long("retry.initialBackoffMs", 200L, 0L),
      retryMaxBackoffMs = long("retry.maxBackoffMs", 5000L, 0L),
      circuitFailures = int("circuit.failures", 5, 1),
      circuitCooldownMs = long("circuit.cooldownMs", 30000L, 0L),
      deadLetterPath = text("deadLetter.path"),
      maxEventBytes = int("maxEventBytes", 1048576, 1024),
      shutdownTimeoutMs = long("shutdownTimeoutMs", 5000L, 0L),
      disabledFacets = disabled,
      schemaMaxFields = int("schema.maxFields", 1000, 0),
      columnLineageMaxFields = int("columnLineage.maxFields", 1000, 0),
      failuresOnly = flag("failuresOnly"),
      emission = emission
    )
  }

  /** `OPENLINEAGE_PARENT_ID`: `<namespace>/<name>/<runId>` (Airflow's form). A job name may itself hold `/`. */
  private def parentId(value: String, fail: String => Nothing): ParentRun = {
    val first = value.indexOf('/')
    val last = value.lastIndexOf('/')
    if (first <= 0 || last <= first + 1 || last == value.length - 1)
      fail(s"OPENLINEAGE_PARENT_ID must be <namespace>/<name>/<runId>, not '$value'")
    ParentRun(value.substring(0, first), value.substring(first + 1, last), uuid("OPENLINEAGE_PARENT_ID's run id", value.substring(last + 1), fail))
  }

  private def uuid(what: String, value: String, fail: String => Nothing): String =
    try java.util.UUID.fromString(value).toString
    catch { case _: IllegalArgumentException => fail(s"$what must be a UUID, not '$value'") }

  private def load(path: String, setting: String): Map[String, String] = {
    val file = new File(path)
    if (!file.isFile) throw new IllegalArgumentException(s"'$setting' names '$path', which is not a file that exists.")
    val properties = new java.util.Properties()
    val in = new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8)
    try properties.load(in) finally in.close()
    properties.asScala.toMap
  }

  /** `url` with any `user:password@` removed, for a log line or an error message. */
  def redact(url: String): String = url.replaceAll("(?<=://)[^/@]*@", "***@")
}
