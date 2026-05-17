import java.io.ByteArrayInputStream
import java.net.URI
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

  test("parses plain text sitemap URL lists") {
    val text =
      """https://example.com/a
        |https://example.com/b
        |https://example.com/a
        |""".stripMargin

    assertEquals(
      parse(text),
      Right(
        SitemapXmlDocument.UrlSet(
          Vector("https://example.com/a", "https://example.com/b")
        )
      )
    )
  }

  test("parses RSS feed item links as sitemap URLs") {
    val xml =
      """<rss version="2.0">
        |  <channel>
        |    <title>Example</title>
        |    <link>https://example.com/</link>
        |    <item><link>https://example.com/a</link></item>
        |    <item><link>https://example.com/b</link></item>
        |  </channel>
        |</rss>
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

  test("parses Atom feed alternate links as sitemap URLs") {
    val xml =
      """<feed xmlns="http://www.w3.org/2005/Atom">
        |  <title>Example</title>
        |  <entry>
        |    <link href="https://example.com/a" />
        |    <link rel="self" href="https://example.com/feed-entry-a" />
        |  </entry>
        |  <entry>
        |    <link rel="alternate" href="https://example.com/b" />
        |  </entry>
        |</feed>
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

  test("rejects unsupported root element") {
    val result = parse("<html><body>not a sitemap</body></html>")

    assert(result.isLeft)
    assert(result.left.exists(_.message.contains("Unsupported sitemap root")))
  }

  test("rejects malformed plain text sitemap rows") {
    val result = parse("https://example.com/a\nnot a url\n")

    assert(result.isLeft)
    assert(result.left.exists(_.message.contains("Invalid text sitemap URL")))
  }

  test("resolves and validates extracted loc URLs") {
    val baseUri = URI.create("https://example.com/sitemaps/root.xml")

    assertEquals(
      SitemapXmlSchema.resolveLoc(baseUri, "https://example.com/page"),
      Some("https://example.com/page")
    )
    assertEquals(
      SitemapXmlSchema.resolveLoc(baseUri, "/page"),
      Some("https://example.com/page")
    )
    assertEquals(
      SitemapXmlSchema.resolveLoc(baseUri, "mailto:a@example.com"),
      None
    )
    assertEquals(SitemapXmlSchema.resolveLoc(baseUri, "https://"), None)
    assertEquals(
      SitemapXmlSchema.resolveLoc(baseUri, "https://example.com/a b"),
      None
    )
  }

  private def parse(
      xml: String
  ): Either[SitemapXmlValidationError, SitemapXmlDocument] =
    SitemapXmlSchema.parse(
      ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8))
    )
}
