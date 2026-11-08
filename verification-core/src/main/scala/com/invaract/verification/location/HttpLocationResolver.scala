// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.verification.location

import org.yaml.snakeyaml.{LoaderOptions, Yaml}
import org.yaml.snakeyaml.constructor.SafeConstructor

import java.net.URI
import java.net.URLEncoder
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.nio.charset.StandardCharsets
import java.time.Duration
import scala.collection.mutable
import scala.util.control.NonFatal

/** A `LocationResolver` that asks a location service over HTTP where each `ref://<id>` is: the platform's own catalog of
  * where datasets live, so one contract runs unchanged in every environment while the service answers per environment.
  *
  * The protocol (docs-site's "Resolve Dataset Locations" guide) is one request per id:
  * {{{
  * GET <baseUrl>/locations/<id>            (the id percent-encoded; Authorization: Bearer <token> when a token is given)
  * 200 {"location": "s3://lake/orders"}    (any other fields are ignored)
  * 404                                     the service has no location for the id
  * }}}
  * Fail-closed, as every resolver is: anything but a 200 whose body names a location (another status, a body that is not a
  * JSON object, an empty location, a location that is itself a `ref://` reference, a service that cannot be reached or
  * does not answer within `timeout`) throws `LocationResolutionException`, naming the id and the URL asked. Each id is asked
  * once per resolver, however many datasets name it.
  *
  * @param baseUrl the service's base URL (`https://locations.corp.internal/v1`); a trailing `/` is ignored
  * @param token   a bearer token for the `Authorization` header, if the service needs one
  * @param timeout how long one request may take, connecting included
  */
final class HttpLocationResolver(baseUrl: String, token: Option[String] = None, timeout: Duration = HttpLocationResolver.DefaultTimeout)
    extends LocationResolver {

  private val base = baseUrl.trim.stripSuffix("/")
  private lazy val client = HttpClient.newBuilder().connectTimeout(timeout).followRedirects(HttpClient.Redirect.NORMAL).build()
  private val resolved = mutable.Map.empty[String, String]

  override def resolve(id: String): String = synchronized(resolved.getOrElseUpdate(id, fetch(id)))

  /** The URL `id`'s location is asked at. */
  def urlOf(id: String): String =
    s"$base${HttpLocationResolver.Path}${URLEncoder.encode(id, StandardCharsets.UTF_8.name).replace("+", "%20")}"

  private def fetch(id: String): String = {
    val url = urlOf(id)
    def failed(why: String, cause: Throwable = null) =
      new LocationResolutionException(s"The location service could not resolve '${LocationRef.Scheme}$id' ($url): $why", cause)
    val response =
      try {
        val request = HttpRequest.newBuilder(URI.create(url)).timeout(timeout).header("Accept", "application/json").GET()
        token.foreach(t => request.header("Authorization", s"Bearer $t"))
        client.send(request.build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8))
      } catch {
        case e: IllegalArgumentException => throw failed(s"'$baseUrl' is not a URL (${e.getMessage})", e)
        case e: InterruptedException     => Thread.currentThread().interrupt(); throw failed("interrupted", e)
        case NonFatal(e)                 => throw failed(s"the service could not be reached: ${e.getClass.getSimpleName}: ${e.getMessage}", e)
      }
    response.statusCode() match {
      case 200 =>
        HttpLocationResolver.locationIn(response.body()) match {
          case Right(location) => location
          case Left(why)       => throw failed(why)
        }
      case 404    => throw failed("it has no location for that id")
      case status => throw failed(s"it answered HTTP $status${HttpLocationResolver.excerpt(response.body())}")
    }
  }
}

object HttpLocationResolver {

  /** Where, under the base URL, an id's location is asked. */
  val Path = "/locations/"

  /** How long one request may take when the caller does not say. */
  val DefaultTimeout: Duration = Duration.ofSeconds(10)

  /** The location a 200 response's body gives, or why it gives none. */
  private[location] def locationIn(body: String): Either[String, String] = {
    val parsed =
      try Option(new Yaml(new SafeConstructor(new LoaderOptions)).load[AnyRef](body))
      catch { case NonFatal(_) => None }
    parsed match {
      case Some(fields: java.util.Map[_, _]) =>
        Option(fields.get("location")) match {
          case Some(location: String) if location.trim.isEmpty => Left("its answer gives an empty location")
          case Some(location: String) if LocationRef.id(location.trim).isDefined =>
            Left(s"its answer is another reference ('${location.trim}'), not a location")
          case Some(location: String) => Right(location.trim)
          case Some(other)            => Left(s"its answer's 'location' is not text ($other)")
          case None                   => Left(s"its answer has no 'location'${excerpt(body)}")
        }
      case _ => Left(s"its answer is not a JSON object${excerpt(body)}")
    }
  }

  /** A response body as an error quotes it: its first 200 characters, or nothing when it is empty. */
  private[location] def excerpt(body: String): String = {
    val text = Option(body).getOrElse("").trim
    if (text.isEmpty) "" else s": ${text.take(200)}"
  }
}
