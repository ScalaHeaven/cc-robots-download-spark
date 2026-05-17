//> using scala "3.8.3"

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.time.Duration
import java.util.Locale

import scala.util.matching.Regex

final case class CountrySuffixRow(
    countryKey: String,
    countryName: String,
    suffix: String
)

private val IndexUrl = URI("https://www.iana.org/domains/root/db")
private val OutputPath =
  Path.of("src/main/resources/sitemap-filter/country-suffixes.tsv")
private val RequestTimeout = Duration.ofSeconds(60)

private val IndexRowPattern: Regex =
  """<tr>\s*<td>\s*<span class="domain tld"><a href="([^"]+)">\.([^<]+)</a></span></td>\s*<td>([^<]+)</td>""".r

private val CountryDesignationPattern: Regex =
  """<!-- \(Country-code top-level domain designated for (.*?)\) -->""".r

@main def extractPrefixes(args: String*): Unit =
  if args.nonEmpty then
    throw IllegalArgumentException(
      "Usage: scala-cli run scripts/ExtractPrefixes.scala"
    )

  val client = HttpClient
    .newBuilder()
    .connectTimeout(RequestTimeout)
    .followRedirects(HttpClient.Redirect.NORMAL)
    .build()

  val rows = extractCountrySuffixRows(client, fetchText(client, IndexUrl))

  Files.writeString(OutputPath, renderRows(rows), StandardCharsets.UTF_8)
  println(s"wrote ${rows.size} rows")

private def extractCountrySuffixRows(
    client: HttpClient,
    index: String
): Vector[CountrySuffixRow] =
  val rows =
    for
      case (href, "country-code") <- extractIndexRows(index)
      tldId = tldIdFromHref(href)
      page = fetchText(client, IndexUrl.resolve(href))
      countryName = extractCountryName(page, tldId)
    yield CountrySuffixRow(
      countryKey = countryKey(countryName),
      countryName = countryName,
      suffix = s".$tldId"
    )

  rows.toVector.distinct.sortBy(row => (row.countryKey, row.suffix))

private def extractIndexRows(index: String): Iterator[(String, String)] =
  IndexRowPattern.findAllMatchIn(index).map { rowMatch =>
    val href = rowMatch.group(1)
    val tldType = htmlUnescape(rowMatch.group(3)).trim

    href -> tldType
  }

private def tldIdFromHref(href: String): String =
  href
    .split("/")
    .lastOption
    .getOrElse(href)
    .stripSuffix(".html")
    .toLowerCase(Locale.ROOT)

private def extractCountryName(page: String, tldId: String): String =
  CountryDesignationPattern
    .findFirstMatchIn(page)
    .map(countryMatch => htmlUnescape(countryMatch.group(1)).trim)
    .getOrElse {
      throw IllegalStateException(s"Missing country designation for $tldId")
    }

private def fetchText(client: HttpClient, uri: URI): String =
  val request = HttpRequest
    .newBuilder(uri)
    .timeout(RequestTimeout)
    .GET()
    .build()
  val response =
    client.send(
      request,
      HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8)
    )

  if response.statusCode() >= 200 && response.statusCode() < 300 then
    response.body()
  else
    throw IllegalStateException(
      s"GET $uri failed with HTTP ${response.statusCode()}"
    )

private def countryKey(countryName: String): String =
  countryName
    .toLowerCase(Locale.ROOT)
    .replaceAll("[^a-z0-9]+", "-")
    .stripPrefix("-")
    .stripSuffix("-")

private def renderRows(rows: Vector[CountrySuffixRow]): String =
  "country_key\tcountry_name\tsuffix\n" +
    rows
      .map(row => s"${row.countryKey}\t${row.countryName}\t${row.suffix}\n")
      .mkString

private def htmlUnescape(value: String): String =
  """&(#x[0-9A-Fa-f]+|#[0-9]+|[A-Za-z]+);""".r.replaceAllIn(
    value,
    entityMatch =>
      Regex.quoteReplacement(
        decodeHtmlEntity(entityMatch.group(1)).getOrElse(entityMatch.group(0))
      )
  )

private def decodeHtmlEntity(entity: String): Option[String] =
  entity match
    case "amp"                       => Some("&")
    case "apos"                      => Some("'")
    case "gt"                        => Some(">")
    case "lt"                        => Some("<")
    case "nbsp"                      => Some(Character.toString(160.toChar))
    case "quot"                      => Some("\"")
    case hex if hex.startsWith("#x") =>
      decodeCodePoint(hex.stripPrefix("#x"), 16)
    case decimal if decimal.startsWith("#") =>
      decodeCodePoint(decimal.stripPrefix("#"), 10)
    case _ =>
      None

private def decodeCodePoint(value: String, radix: Int): Option[String] =
  try Some(Character.toString(Integer.parseInt(value, radix)))
  catch case _: IllegalArgumentException => None
