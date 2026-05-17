import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets

class SitemapCountryLocaleClassifierSuite extends munit.FunSuite {
  private val suffixes =
    SitemapCountryLocaleClassifier.loadCountrySuffixes(
      ByteArrayInputStream(
        """country_key	country_name	suffix
          |ukraine	Ukraine	.ua
          |ukraine	Ukraine	.укр
          |ukraine	Ukraine	.xn--j1amh
          |russia	Russia	.ru
          |russia	Russia	.рф
          |russia	Russia	.xn--p1ai
          |united-kingdom	United Kingdom	.uk
          |""".stripMargin.getBytes(StandardCharsets.UTF_8)
      )
    )

  test("classifies target country TLD and IDN suffixes") {
    assertCountry("example.ua", "ukraine", ".ua")
    assertCountry("example.укр", "ukraine", ".xn--j1amh")
    assertCountry("example.xn--j1amh", "ukraine", ".xn--j1amh")
    assertCountry("example.ru", "russia", ".ru")
    assertCountry("example.рф", "russia", ".xn--p1ai")
    assertCountry("example.xn--p1ai", "russia", ".xn--p1ai")
    assertCountry("example.co.uk", "united-kingdom", ".uk")
  }

  test("rejects non-target and reserved suffixes") {
    assertEquals(classifyCountry("example.com"), None)
    assertEquals(classifyCountry("example.de"), None)
    assertEquals(classifyCountry("example.gb"), None)
  }

  test("falls back from robots host to sitemap URL host") {
    val classification = SitemapCountryLocaleClassifier.classify(
      "unknown-host",
      "https://news.example.ua/sitemap.xml",
      suffixes
    )

    assertEquals(classification.map(_.countryKey), Some("ukraine"))
  }

  test("detects locale markers from path and subdomain tokens") {
    assertLanguage("https://example.ua/uk/sitemap.xml", "uk-UA")
    assertLanguage("https://example.ua/uk-ua/sitemap.xml", "uk-UA")
    assertLanguage("https://ua.example.ua/sitemap.xml", "uk-UA")
    assertLanguage("https://example.ru/ru-ru/sitemap.xml", "ru-RU")
    assertLanguage("https://ru.example.ru/sitemap.xml", "ru-RU")
    assertLanguage("https://example.co.uk/en-gb/sitemap.xml", "en-GB")
    assertLanguage("https://gb.example.co.uk/sitemap.xml", "en-GB")
  }

  test("treats uk alone as Ukrainian language marker") {
    assertLanguage("https://example.co.uk/uk/sitemap.xml", "uk-UA")
    assertLanguage("https://example.co.uk/en-uk/sitemap.xml", "en-GB")
  }

  test("does not treat country TLD tokens as locale markers") {
    assertEquals(
      SitemapCountryLocaleClassifier.detectLanguageRegion(
        "https://example.co.uk/sitemap.xml"
      ),
      None
    )
  }

  test("does not crash on malformed URLs") {
    val classification = SitemapCountryLocaleClassifier.classify(
      "example.ua",
      "://not a valid url",
      suffixes
    )

    assertEquals(classification.map(_.languageRegion), Some("unknown"))
  }

  private def assertCountry(
      host: String,
      countryKey: String,
      suffix: String
  ): Unit = {
    val classification = classifyCountry(host)

    assertEquals(classification.map(_.countryKey), Some(countryKey))
    assertEquals(classification.map(_.suffix), Some(suffix))
  }

  private def assertLanguage(url: String, languageRegion: String): Unit =
    assertEquals(
      SitemapCountryLocaleClassifier.detectLanguageRegion(url),
      Some(languageRegion)
    )

  private def classifyCountry(host: String): Option[CountrySuffix] =
    SitemapCountryLocaleClassifier.classifyCountry(host, suffixes)
}
