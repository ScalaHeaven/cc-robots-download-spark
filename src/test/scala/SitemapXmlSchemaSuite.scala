import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets

class SitemapXmlSchemaSuite extends munit.FunSuite {
  test("parses valid urlset loc entries") {
    val xml =
      """<?xml version="1.0" encoding="UTF-8"?>
        |<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
        |  <url>
        |    <loc>https://example.com/a</loc>
        |    <image:image xmlns:image="http://www.google.com/schemas/sitemap-image/1.1">
        |      <image:loc>https://example.com/image.jpg</image:loc>
        |    </image:image>
        |  </url>
        |  <url><loc>https://example.com/b</loc></url>
        |</urlset>
        |""".stripMargin

    assertEquals(
      parse(xml),
      Right(
        SitemapXmlDocument.UrlSet(
          Vector("https://example.com/a", "https://example.com/b")
        )
      )
    )
  }

  test("parses valid sitemap index loc entries") {
    val xml =
      """<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
        |  <sitemap><loc>https://example.com/sitemap-1.xml</loc></sitemap>
        |  <sitemap><loc>https://example.com/sitemap-2.xml.gz</loc></sitemap>
        |</sitemapindex>
        |""".stripMargin

    assertEquals(
      parse(xml),
      Right(
        SitemapXmlDocument.SitemapIndex(
          Vector(
            "https://example.com/sitemap-1.xml",
            "https://example.com/sitemap-2.xml.gz"
          )
        )
      )
    )
  }

  test("rejects unsupported root element") {
    val result = parse("<html><body>not a sitemap</body></html>")

    assert(result.isLeft)
    assert(result.left.exists(_.message.contains("Unsupported sitemap root")))
  }

  private def parse(
      xml: String
  ): Either[SitemapXmlValidationError, SitemapXmlDocument] =
    SitemapXmlSchema.parse(
      ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8))
    )
}
