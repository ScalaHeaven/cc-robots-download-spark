import java.io.InputStream
import java.net.{IDN, URI}
import java.nio.charset.StandardCharsets
import java.util.Locale

import scala.io.Source
import scala.util.Using

final case class CountrySuffix(
    countryKey: String,
    countryName: String,
    suffix: String
)

final case class SitemapClassification(
    countryKey: String,
    countryName: String,
    matchedSuffix: String,
    languageRegion: String
)

final case class SelectedCountry(
    countryKey: String,
    countryName: String
)

object SitemapCountryLocaleClassifier {
  val UnknownLanguageRegion = "unknown"

  def loadDefaultCountrySuffixes(): Vector[CountrySuffix] = {
    val resourceName = "sitemap-filter/country-suffixes.tsv"
    val stream = Option(
      Thread
        .currentThread()
        .getContextClassLoader()
        .getResourceAsStream(
          resourceName
        )
    ).getOrElse {
      throw IllegalStateException(s"Missing resource: $resourceName")
    }

    loadCountrySuffixes(stream)
  }

  def loadCountrySuffixes(stream: InputStream): Vector[CountrySuffix] =
    Using.resource(
      Source.fromInputStream(stream, StandardCharsets.UTF_8.name())
    ) { source =>
      source
        .getLines()
        .drop(1)
        .map(_.split("\t", -1).toVector)
        .collect {
          case Vector(countryKey, countryName, suffix)
              if countryKey.nonEmpty && countryName.nonEmpty && suffix.nonEmpty =>
            CountrySuffix(
              countryKey,
              countryName,
              normalizeSuffix(suffix)
            )
        }
        .toVector
    }

  def classify(
      robotsHost: String,
      sitemapUrl: String,
      suffixes: Vector[CountrySuffix]
  ): Option[SitemapClassification] =
    classifyCountry(robotsHost, suffixes)
      .orElse(extractHost(sitemapUrl).flatMap(classifyCountry(_, suffixes)))
      .map { suffix =>
        SitemapClassification(
          suffix.countryKey,
          suffix.countryName,
          suffix.suffix,
          detectLanguageRegion(sitemapUrl).getOrElse(UnknownLanguageRegion)
        )
      }

  def classifyCountry(
      host: String,
      suffixes: Vector[CountrySuffix]
  ): Option[CountrySuffix] = {
    val normalizedHost = normalizeHost(host)

    suffixes
      .sortBy(suffix => -suffix.suffix.length)
      .find { suffix =>
        normalizedHost == suffix.suffix.stripPrefix(".") ||
        normalizedHost.endsWith(suffix.suffix)
      }
  }

  def selectCountry(
      query: String,
      suffixes: Vector[CountrySuffix]
  ): Option[SelectedCountry] = {
    val normalizedQuery = normalizeCountryQuery(query)
    val normalizedSuffix = normalizeSuffix(query)

    suffixes
      .find(suffix =>
        normalizeCountryQuery(suffix.countryKey) == normalizedQuery ||
          normalizeCountryQuery(suffix.countryName) == normalizedQuery ||
          suffix.suffix == normalizedSuffix
      )
      .map(suffix => SelectedCountry(suffix.countryKey, suffix.countryName))
  }

  def countrySuffixes(
      selectedCountry: SelectedCountry,
      suffixes: Vector[CountrySuffix]
  ): Vector[CountrySuffix] =
    suffixes.filter(_.countryKey == selectedCountry.countryKey).distinct

  def supportedCountryKeys(suffixes: Vector[CountrySuffix]): Vector[String] =
    suffixes.map(_.countryKey).distinct.sorted

  def detectLanguageRegion(sitemapUrl: String): Option[String] = {
    val tokens = extractHost(sitemapUrl).toVector.flatMap(hostLocaleTokens) ++
      extractPath(sitemapUrl).toVector.flatMap(localeTokens)

    tokens.collectFirst {
      case "uk" | "uk-ua" | "ua" | "ua-uk" => "uk-UA"
      case "ru" | "ru-ru"                  => "ru-RU"
      case "en-gb" | "en-uk" | "gb"        => "en-GB"
    }
  }

  def extractHost(url: String): Option[String] =
    parseUri(url)
      .flatMap(uri => Option(uri.getHost()))
      .orElse {
        val withScheme = s"https://$url"
        parseUri(withScheme).flatMap(uri => Option(uri.getHost()))
      }

  private def extractPath(url: String): Option[String] =
    parseUri(url).flatMap(uri => Option(uri.getRawPath()))

  private def parseUri(value: String): Option[URI] =
    try {
      Some(URI(value.trim))
    } catch {
      case _: Exception =>
        None
    }

  private def hostLocaleTokens(host: String): Vector[String] =
    localeTokens(host.split("\\.").toVector.dropRight(1).mkString("."))

  private def localeTokens(value: String): Vector[String] =
    value
      .split("[^A-Za-z0-9]+")
      .toVector
      .map(_.toLowerCase(Locale.ROOT))
      .filter(_.nonEmpty)
      .sliding(2)
      .collect { case Vector(left, right) =>
        s"$left-$right"
      }
      .toVector ++
      value
        .split("[^A-Za-z0-9]+")
        .toVector
        .map(_.toLowerCase(Locale.ROOT))
        .filter(_.nonEmpty)

  private def normalizeSuffix(suffix: String): String = {
    val withoutDot = suffix.trim.stripPrefix(".")
    s".${normalizeHost(withoutDot)}"
  }

  private def normalizeCountryQuery(country: String): String =
    country.trim
      .toLowerCase(Locale.ROOT)
      .replaceAll("[^a-z0-9]+", "-")
      .stripPrefix("-")
      .stripSuffix("-")

  private def normalizeHost(host: String): String = {
    val withoutTrailingDot = host.trim.stripSuffix(".")
    val ascii =
      try {
        IDN.toASCII(withoutTrailingDot, IDN.ALLOW_UNASSIGNED)
      } catch {
        case _: IllegalArgumentException =>
          withoutTrailingDot
      }

    ascii.toLowerCase(Locale.ROOT)
  }
}
