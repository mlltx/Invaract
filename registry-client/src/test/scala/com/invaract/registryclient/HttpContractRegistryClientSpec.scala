// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.registryclient

import com.invaract.contract.{Contract, ContractParser, ContractVersion}

import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import org.scalatest.funsuite.AnyFunSuite

import java.io.ByteArrayOutputStream
import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets

/** Exercises `HttpContractRegistryClient` against a real, in-process HTTP
  * server (the JDK's own `com.sun.net.httpserver.HttpServer` — no mocking
  * library, no new test dependency), the same "real thing over a mock"
  * instinct `spark-adapter`'s own test suite applies with a real `local[*]`
  * `SparkSession` (see ARCHITECTURE.md's ADR-005). Each test wires a
  * canned response for the wire shapes docs/CONTRACT_REGISTRY.md §3
  * specifies and asserts the client parses/raises exactly what that
  * response means.
  */
class HttpContractRegistryClientSpec extends AnyFunSuite {

  private val sampleContractYaml =
    """id: customer_orders
      |version: "1.0.0"
      |status: active
      |inputs:
      |  - name: orders
      |    location: raw.orders
      |    format: table
      |    schema:
      |      fields:
      |        - name: order_id
      |          type: string
      |          required: true
      |          nullable: false
      |outputs:
      |  - name: customer_orders
      |    location: gold.customer_orders
      |    format: table
      |    schema:
      |      fields:
      |        - name: customer_id
      |          type: string
      |          required: true
      |          nullable: false
      |""".stripMargin

  private def sampleContract: Contract = ContractParser.parse(sampleContractYaml)

  /** Starts a real HTTP server on an ephemeral port, running `handler` for
    * every request, hands `test` the resulting `http://localhost:<port>`
    * base URL, and always stops the server afterward (even if `test`
    * throws) so a failing assertion never leaks a bound port across
    * tests.
    */
  private def withServer(handler: HttpExchange => Unit)(test: String => Unit): Unit = {
    val server = HttpServer.create(new InetSocketAddress("localhost", 0), 0)
    server.createContext("/", new HttpHandler {
      override def handle(exchange: HttpExchange): Unit = handler(exchange)
    })
    server.start()
    try {
      test(s"http://localhost:${server.getAddress.getPort}")
    } finally {
      server.stop(0)
    }
  }

  private def respond(exchange: HttpExchange, status: Int, body: String, contentType: String = "application/json"): Unit = {
    val bytes = body.getBytes(StandardCharsets.UTF_8)
    exchange.getResponseHeaders.add("Content-Type", contentType)
    exchange.sendResponseHeaders(status, bytes.length)
    val out = exchange.getResponseBody
    try out.write(bytes) finally out.close()
  }

  private def requestBody(exchange: HttpExchange): String = {
    val buffer = new ByteArrayOutputStream()
    val in = exchange.getRequestBody
    val chunk = new Array[Byte](4096)
    var read = in.read(chunk)
    while (read != -1) {
      buffer.write(chunk, 0, read)
      read = in.read(chunk)
    }
    buffer.toString(StandardCharsets.UTF_8.name())
  }

  private def newClient(baseUrl: String): HttpContractRegistryClient = {
    val client = new HttpContractRegistryClient
    client.configure(baseUrl)
    client
  }

  test("get returns the parsed Contract on 200") {
    withServer { exchange =>
      assert(exchange.getRequestURI.getPath == "/contracts/customer_orders/versions/1.0.0")
      respond(exchange, 200, sampleContractYaml, "application/yaml")
    } { baseUrl =>
      val contract = newClient(baseUrl).get("customer_orders", ContractVersion(1, 0, 0))
      assert(contract.id == "customer_orders")
      assert(contract.version == ContractVersion(1, 0, 0))
    }
  }

  test("get throws ContractRegistryNotFoundException on 404, naming the requested version") {
    withServer { exchange => respond(exchange, 404, "") } { baseUrl =>
      val ex = intercept[ContractRegistryNotFoundException] {
        newClient(baseUrl).get("missing_contract", ContractVersion(1, 0, 0))
      }
      assert(ex.getMessage.contains("missing_contract"))
      assert(ex.getMessage.contains("1.0.0"))
    }
  }

  test("getLatest requests the 'latest' version segment") {
    withServer { exchange =>
      assert(exchange.getRequestURI.getPath == "/contracts/customer_orders/versions/latest")
      respond(exchange, 200, sampleContractYaml, "application/yaml")
    } { baseUrl =>
      val contract = newClient(baseUrl).getLatest("customer_orders")
      assert(contract.id == "customer_orders")
    }
  }

  test("getLatest throws ContractRegistryNotFoundException with no version named, on 404") {
    withServer { exchange => respond(exchange, 404, "") } { baseUrl =>
      val ex = intercept[ContractRegistryNotFoundException] {
        newClient(baseUrl).getLatest("missing_contract")
      }
      assert(ex.getMessage.contains("missing_contract"))
      assert(!ex.getMessage.contains("at version"))
    }
  }

  test("listVersions parses a JSON array of version/status objects") {
    withServer { exchange =>
      assert(exchange.getRequestURI.getPath == "/contracts/customer_orders/versions")
      respond(exchange, 200, """[{"version":"1.0.0","status":"active"},{"version":"2.0.0","status":"draft"}]""")
    } { baseUrl =>
      val versions = newClient(baseUrl).listVersions("customer_orders")
      assert(versions == List(
        VersionInfo(ContractVersion(1, 0, 0), "active"),
        VersionInfo(ContractVersion(2, 0, 0), "draft")
      ))
    }
  }

  test("listVersions throws ContractRegistryNotFoundException on 404") {
    withServer { exchange => respond(exchange, 404, "") } { baseUrl =>
      intercept[ContractRegistryNotFoundException] {
        newClient(baseUrl).listVersions("missing_contract")
      }
    }
  }

  test("listContractIds parses a flat JSON array of strings") {
    withServer { exchange =>
      assert(exchange.getRequestURI.getPath == "/contracts")
      respond(exchange, 200, """["customer_orders", "shipments"]""")
    } { baseUrl =>
      assert(newClient(baseUrl).listContractIds() == List("customer_orders", "shipments"))
    }
  }

  test("register sends the contract as a YAML PUT body and parses the compatibility report on 201") {
    withServer { exchange =>
      assert(exchange.getRequestMethod == "PUT")
      assert(exchange.getRequestURI.getPath == "/contracts/customer_orders/versions/1.0.0")
      val sentBody = requestBody(exchange)
      assert(ContractParser.parse(sentBody).id == "customer_orders")
      respond(exchange, 201, """{"changes":[{"level":"Minor","path":"outputs.x","description":"Dataset added"}]}""")
    } { baseUrl =>
      val report = newClient(baseUrl).register(sampleContract, expectedPrevious = None)
      assert(report.changes.size == 1)
      assert(report.requiredLevel == com.invaract.contract.CompatibilityLevel.Minor)
    }
  }

  test("register includes expectedPrevious and force in the query string when set") {
    withServer { exchange =>
      val query = exchange.getRequestURI.getQuery
      assert(query.contains("expectedPrevious=0.9.0"))
      assert(query.contains("force=true"))
      respond(exchange, 201, """{"changes":[]}""")
    } { baseUrl =>
      newClient(baseUrl).register(sampleContract, expectedPrevious = Some(ContractVersion(0, 9, 0)), force = true)
    }
  }

  test("register omits query parameters entirely when expectedPrevious is None and force is false") {
    withServer { exchange =>
      // Stronger than checking getQuery().isEmpty: a URI ending in a bare
      // "?" with nothing after it also reports an empty (non-null) query
      // string, so that alone wouldn't distinguish "no query component at
      // all" from "an empty one" - checking the raw request line/URI text
      // for the literal '?' character does.
      assert(!exchange.getRequestURI.toString.contains("?"))
      respond(exchange, 201, """{"changes":[]}""")
    } { baseUrl =>
      newClient(baseUrl).register(sampleContract, expectedPrevious = None)
    }
  }

  test("register throws ContractRegistryValidationException with the server's messages on 400") {
    withServer { exchange =>
      respond(exchange, 400, """{"messages":["outputs must not be empty","id is required"]}""")
    } { baseUrl =>
      val ex = intercept[ContractRegistryValidationException] {
        newClient(baseUrl).register(sampleContract, expectedPrevious = None)
      }
      assert(ex.messages == List("outputs must not be empty", "id is required"))
    }
  }

  test("register throws ContractRegistryConflictException with the actual current version on a version_conflict 409") {
    withServer { exchange =>
      respond(exchange, 409, """{"reason":"version_conflict","actualCurrent":"1.2.0"}""")
    } { baseUrl =>
      val ex = intercept[ContractRegistryConflictException] {
        newClient(baseUrl).register(sampleContract, expectedPrevious = Some(ContractVersion(1, 0, 0)))
      }
      assert(ex.actualCurrent.contains(ContractVersion(1, 2, 0)))
    }
  }

  test("register's ContractRegistryConflictException carries None when the registry reports no version registered yet") {
    withServer { exchange =>
      respond(exchange, 409, """{"reason":"version_conflict","actualCurrent":null}""")
    } { baseUrl =>
      val ex = intercept[ContractRegistryConflictException] {
        newClient(baseUrl).register(sampleContract, expectedPrevious = Some(ContractVersion(1, 0, 0)))
      }
      assert(ex.actualCurrent.isEmpty)
    }
  }

  test("register throws ContractRegistryCompatibilityException with the report on a compatibility_rejected 409") {
    withServer { exchange =>
      respond(
        exchange,
        409,
        """{"reason":"compatibility_rejected","changes":[{"level":"Breaking","path":"outputs.x","description":"Field removed"}]}"""
      )
    } { baseUrl =>
      val ex = intercept[ContractRegistryCompatibilityException] {
        newClient(baseUrl).register(sampleContract, expectedPrevious = Some(ContractVersion(1, 0, 0)))
      }
      assert(ex.report.isBreaking)
      assert(ex.report.breakingChanges.map(_.description) == List("Field removed"))
    }
  }

  test("an unexpected status code raises a plain ContractRegistryException naming the status") {
    withServer { exchange => respond(exchange, 500, "boom") } { baseUrl =>
      val ex = intercept[ContractRegistryException] {
        newClient(baseUrl).listContractIds()
      }
      assert(ex.getMessage.contains("500"))
      assert(ex.getMessage.contains("boom"))
    }
  }
}
