// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.verification.location

import org.scalatest.funsuite.AnyFunSuite

import java.net.ServerSocket
import java.time.Duration

/** `HttpLocationResolver` against a real HTTP server (`TestLocationService`): what it asks, what it takes for a location,
  * and how it fails - always closed, naming the reference and the URL. */
class HttpLocationResolverSpec extends AnyFunSuite {

  private def failure(resolver: HttpLocationResolver, id: String): String =
    intercept[LocationResolutionException](resolver.resolve(id)).getMessage

  test("resolves an id to the location the service gives, asking GET <base>/locations/<id> with no token unless given one") {
    val service = TestLocationService.holding("gold_out" -> "s3://lake/gold/out")
    try {
      assert(new HttpLocationResolver(service.url).resolve("gold_out") == "s3://lake/gold/out")
      assert(service.paths == Vector("/locations/gold_out"))
      assert(service.authorizations == Vector(None))
      assert(new HttpLocationResolver(service.url, Some("s3cr3t")).resolve("gold_out") == "s3://lake/gold/out")
      assert(service.authorizations.last.contains("Bearer s3cr3t"))
    } finally service.close()
  }

  test("a trailing slash on the base URL is ignored, and the id is percent-encoded, a space as %20") {
    val service = new TestLocationService(Map("/v1/locations/a%20b%2Fc" -> (200 -> """{"location": "/data/a b"}""")))
    try {
      val resolver = new HttpLocationResolver(s" ${service.url}/v1/ ")
      assert(resolver.urlOf("a b/c") == s"${service.url}/v1/locations/a%20b%2Fc")
      assert(resolver.resolve("a b/c") == "/data/a b")
    } finally service.close()
  }

  test("each id is asked once per resolver, however many times it is resolved") {
    val service = TestLocationService.holding("a" -> "/a", "b" -> "/b")
    try {
      val resolver = new HttpLocationResolver(service.url)
      assert(List("a", "b", "a", "b", "a").map(resolver.resolve) == List("/a", "/b", "/a", "/b", "/a"))
      assert(service.paths == Vector("/locations/a", "/locations/b"))
    } finally service.close()
  }

  test("a location is trimmed, other fields are ignored, and a redirect is followed") {
    val service = new TestLocationService(Map(
      "/locations/x"     -> (200 -> """{"id": "x", "location": "  /data/x \n", "owner": "orders-team"}"""),
      "/locations/moved" -> (302 -> "/locations/x")
    ))
    try {
      assert(new HttpLocationResolver(service.url).resolve("x") == "/data/x")
      assert(new HttpLocationResolver(service.url).resolve("moved") == "/data/x")
    } finally service.close()
  }

  test("an id the service does not hold, or any status but 200, fails naming the reference, the URL and the status") {
    val service = new TestLocationService(Map("/locations/broken" -> (500 -> "  database down  "), "/locations/busy" -> (503 -> "")))
    try {
      val resolver = new HttpLocationResolver(service.url)
      assert(failure(resolver, "gone") == s"The location service could not resolve 'ref://gone' (${service.url}/locations/gone): it has no location for that id")
      assert(failure(resolver, "broken").endsWith("(" + service.url + "/locations/broken): it answered HTTP 500: database down"))
      assert(failure(resolver, "busy").endsWith("/locations/busy): it answered HTTP 503"))
    } finally service.close()
  }

  test("a failure is not cached: an id that failed is asked again") {
    val service = new TestLocationService(Map.empty)
    try {
      val resolver = new HttpLocationResolver(service.url)
      failure(resolver, "x")
      failure(resolver, "x")
      assert(service.paths == Vector("/locations/x", "/locations/x"))
    } finally service.close()
  }

  test("a service that cannot be reached, does not answer in time, or is not a URL fails closed") {
    val port = { val s = new ServerSocket(0); try s.getLocalPort finally s.close() }
    assert(failure(new HttpLocationResolver(s"http://127.0.0.1:$port"), "x").contains("): the service could not be reached: "))
    val slow = new TestLocationService(Map("/locations/x" -> (200 -> """{"location": "/x"}""")), delayMs = 2000)
    try {
      val message = failure(new HttpLocationResolver(slow.url, timeout = Duration.ofMillis(200)), "x")
      assert(message.contains("the service could not be reached: HttpTimeoutException"), message)
    } finally slow.close()
    assert(failure(new HttpLocationResolver("not a url"), "x").contains("): 'not a url' is not a URL ("))
  }

  test("an answer that names no location is refused, saying why") {
    def answer(body: String) = HttpLocationResolver.locationIn(body)
    assert(answer("""{"location": "s3://b/x"}""") == Right("s3://b/x"))
    assert(answer("""{"location": "   "}""") == Left("its answer gives an empty location"))
    assert(answer("""{"location": "ref://other"}""") == Left("its answer is another reference ('ref://other'), not a location"))
    assert(answer("""{"location": 42}""") == Left("its answer's 'location' is not text (42)"))
    assert(answer("""{"path": "/x"}""") == Left("""its answer has no 'location': {"path": "/x"}"""))
    assert(answer("""["/x"]""") == Left("""its answer is not a JSON object: ["/x"]"""))
    assert(answer("{not json") == Left("its answer is not a JSON object: {not json"))
    assert(answer("") == Left("its answer is not a JSON object"))
  }

  test("a 200 whose answer names no location fails, quoting the reason") {
    val service = new TestLocationService(Map("/locations/x" -> (200 -> """{"location": ""}""")))
    try assert(failure(new HttpLocationResolver(service.url), "x").endsWith("/locations/x): its answer gives an empty location"))
    finally service.close()
  }

  test("a quoted body is cut at 200 characters") {
    assert(HttpLocationResolver.excerpt("x" * 500) == ": " + "x" * 200)
    assert(HttpLocationResolver.excerpt(" \n ") == "")
    assert(HttpLocationResolver.excerpt(null) == "")
  }
}
