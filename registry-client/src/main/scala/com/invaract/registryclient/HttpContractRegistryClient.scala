// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.registryclient

import com.invaract.contract.{CompatibilityChange, CompatibilityLevel, CompatibilityReport, Contract, ContractParser, ContractVersion}

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.net.http.HttpRequest.BodyPublishers
import java.net.http.HttpResponse.BodyHandlers
import java.nio.charset.StandardCharsets
import java.time.Duration

import RegistryJson._

/** Real `ContractRegistryClient` implementation, talking to the wire
  * protocol in docs/CONTRACT_REGISTRY.md §3. Built on the JDK's own
  * `java.net.http.HttpClient` (available since JDK 11; this repo already
  * runs on JDK 21 with `-target:jvm-1.8` *bytecode* — a restriction on
  * emitted classfile version, not on which JDK APIs are callable at
  * runtime) — zero new library dependencies, matching this repo's
  * existing "boring, minimal deps" instinct.
  *
  * A no-arg constructor plus this `apply`-style companion object factory
  * exists so `spark-adapter` can resolve this class reflectively by name
  * (`spark.invaract.registryClientClass`, defaulting to this class'
  * fully-qualified name) the same way `NotificationSinkFactory` resolves
  * a `NotificationSink` — see docs/CONTRACT_REGISTRY.md §7.
  * `configure(baseUrl)` is called once, immediately after construction,
  * before any other method — mirroring `NotificationSink.configure`'s own
  * two-phase "no-arg construct, then configure" shape.
  */
class HttpContractRegistryClient extends ContractRegistryClient {

  private var baseUrl: String = _

  private lazy val httpClient: HttpClient =
    HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build()

  /** Sets the registry's base URL (e.g. `https://registry.corp.internal`,
    * no trailing slash required). Must be called once before any other
    * method on this instance.
    */
  def configure(baseUrl: String): Unit = {
    this.baseUrl = baseUrl.stripSuffix("/")
  }

  override def get(contractId: String, version: ContractVersion): Contract =
    fetchContract(contractId, version.toString)

  override def getLatest(contractId: String): Contract =
    fetchContract(contractId, "latest")

  private def fetchContract(contractId: String, versionSegment: String): Contract = {
    val request = requestBuilder(s"/contracts/${encode(contractId)}/versions/${encode(versionSegment)}")
      .GET()
      .build()
    val response = send(request)
    response.statusCode() match {
      case 200 => ContractParser.parse(response.body())
      case 404 =>
        val version = if (versionSegment == "latest") None else Some(versionSegment)
        throw new ContractRegistryNotFoundException(contractId, version)
      case other => throw unexpectedStatus(other, response.body())
    }
  }

  override def listVersions(contractId: String): List[VersionInfo] = {
    val request = requestBuilder(s"/contracts/${encode(contractId)}/versions").GET().build()
    val response = send(request)
    response.statusCode() match {
      case 200 =>
        parse(response.body()) match {
          case JArray(items) =>
            items.map {
              case JObject(fields) =>
                val version = fields.get("version") match {
                  case Some(JString(v)) => ContractVersion.parse(v)
                  case _                => throw malformedResponse("versions[].version", response.body())
                }
                val status = fields.get("status") match {
                  case Some(JString(s)) => s
                  case _                => throw malformedResponse("versions[].status", response.body())
                }
                VersionInfo(version, status)
              case _ => throw malformedResponse("versions[]", response.body())
            }
          case _ => throw malformedResponse("versions", response.body())
        }
      case 404 => throw new ContractRegistryNotFoundException(contractId, None)
      case other => throw unexpectedStatus(other, response.body())
    }
  }

  override def listContractIds(): List[String] = {
    val request = requestBuilder("/contracts").GET().build()
    val response = send(request)
    response.statusCode() match {
      case 200 =>
        parse(response.body()) match {
          case JArray(items) =>
            items.map {
              case JString(id) => id
              case _            => throw malformedResponse("contracts[]", response.body())
            }
          case _ => throw malformedResponse("contracts", response.body())
        }
      case other => throw unexpectedStatus(other, response.body())
    }
  }

  override def register(
    contract: Contract,
    expectedPrevious: Option[ContractVersion],
    force: Boolean
  ): CompatibilityReport = {
    val queryParams = List(
      expectedPrevious.map(v => s"expectedPrevious=${encode(v.toString)}"),
      if (force) Some("force=true") else None
    ).flatten
    val query = if (queryParams.isEmpty) "" else "?" + queryParams.mkString("&")
    val path = s"/contracts/${encode(contract.id)}/versions/${encode(contract.version.toString)}$query"

    val request = requestBuilder(path)
      .PUT(BodyPublishers.ofString(ContractParser.write(contract), StandardCharsets.UTF_8))
      .header("Content-Type", "application/yaml")
      .build()
    val response = send(request)
    response.statusCode() match {
      case 201 => parseCompatibilityReport(response.body())
      case 400 => throw new ContractRegistryValidationException(parseMessages(response.body()))
      case 409 => throw parseConflict(response.body())
      case other => throw unexpectedStatus(other, response.body())
    }
  }

  private def parseConflict(body: String): ContractRegistryException =
    parse(body) match {
      case JObject(fields) =>
        fields.get("reason") match {
          case Some(JString("version_conflict")) =>
            val actual = fields.get("actualCurrent") match {
              case Some(JString(v)) => Some(ContractVersion.parse(v))
              case Some(JNull) | None => None
              case _ => throw malformedResponse("actualCurrent", body)
            }
            new ContractRegistryConflictException(actual)
          case Some(JString("compatibility_rejected")) =>
            new ContractRegistryCompatibilityException(parseCompatibilityReport(body))
          case _ => throw malformedResponse("reason", body)
        }
      case _ => throw malformedResponse("<conflict body>", body)
    }

  private def parseMessages(body: String): List[String] =
    parse(body) match {
      case JObject(fields) =>
        fields.get("messages") match {
          case Some(JArray(items)) =>
            items.map {
              case JString(m) => m
              case _          => throw malformedResponse("messages[]", body)
            }
          case _ => throw malformedResponse("messages", body)
        }
      case _ => throw malformedResponse("<validation error body>", body)
    }

  /** Wire vocabulary for `CompatibilityLevel`: the case objects' own
    * names (`"Patch"`/`"Minor"`/`"Breaking"`), not the MAJOR/MINOR/PATCH
    * version-bump vocabulary `ContractCompatibility`'s own `describe`
    * uses privately for a *different* purpose (translating a level into
    * "which version digit must change") — conflating the two would make
    * "Breaking" (a diff classification) indistinguishable from "MAJOR"
    * (a version-bump size) on the wire for no benefit. Both this client
    * and the `invaract-registry` server must agree on this exact
    * vocabulary; docs/CONTRACT_REGISTRY.md §3 is the shared source of
    * truth for it.
    */
  private def parseCompatibilityReport(body: String): CompatibilityReport =
    parse(body) match {
      case JObject(fields) =>
        val changes = fields.get("changes") match {
          case Some(JArray(items)) =>
            items.map {
              case JObject(changeFields) =>
                val level = changeFields.get("level") match {
                  case Some(JString("Patch"))    => CompatibilityLevel.Patch
                  case Some(JString("Minor"))    => CompatibilityLevel.Minor
                  case Some(JString("Breaking")) => CompatibilityLevel.Breaking
                  case _ => throw malformedResponse("changes[].level", body)
                }
                val path = changeFields.get("path") match {
                  case Some(JString(p)) => p
                  case _                => throw malformedResponse("changes[].path", body)
                }
                val description = changeFields.get("description") match {
                  case Some(JString(d)) => d
                  case _                => throw malformedResponse("changes[].description", body)
                }
                CompatibilityChange(level, path, description)
              case _ => throw malformedResponse("changes[]", body)
            }
          case _ => throw malformedResponse("changes", body)
        }
        CompatibilityReport(changes)
      case _ => throw malformedResponse("<compatibility report body>", body)
    }

  private def requestBuilder(path: String): HttpRequest.Builder =
    HttpRequest.newBuilder(URI.create(s"$baseUrl$path")).timeout(Duration.ofSeconds(30))

  private def send(request: HttpRequest): HttpResponse[String] =
    httpClient.send(request, BodyHandlers.ofString(StandardCharsets.UTF_8))

  private def encode(segment: String): String =
    java.net.URLEncoder.encode(segment, "UTF-8")

  private def malformedResponse(field: String, body: String): ContractRegistryException =
    new ContractRegistryException(s"Malformed registry response: missing/invalid '$field' in body: $body")

  private def unexpectedStatus(status: Int, body: String): ContractRegistryException =
    new ContractRegistryException(s"Unexpected registry response status $status: $body")
}
