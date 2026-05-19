class LocalSitemapDownloadPipelineSuite extends munit.FunSuite {
  test(
    "formats downloaded sitemap links without source host or scheme columns"
  ) {
    val expected = Vector(
      "https://example.com/sitemap_index.xml",
      "https://example.com/post-sitemap.xml",
      "https://example.com/posts/one"
    ).mkString("\t")

    val row = LocalSitemapDownloadPipeline.extractedLinkTsvRow(
      "https://example.com/sitemap_index.xml",
      "https://example.com/post-sitemap.xml",
      "https://example.com/posts/one"
    )

    assertEquals(row, expected)
  }

  test("sanitizes TSV control characters in downloaded sitemap link fields") {
    val expected = Vector(
      "https://example.com/sitemap index.xml",
      "https://example.com/post-sitemap.xml",
      "https://example.com/posts/one "
    ).mkString("\t")

    val row = LocalSitemapDownloadPipeline.extractedLinkTsvRow(
      "https://example.com/sitemap\tindex.xml",
      "https://example.com/post-sitemap.xml",
      "https://example.com/posts/one\n"
    )

    assertEquals(row, expected)
  }

  test("reads sitemap URL from local sitemap TSV rows") {
    val row = Vector(
      "target/commoncrawl-robots/example.com/http-date-hash.txt",
      "example.com",
      "https",
      "https://example.com/sitemap.xml"
    ).mkString("\t")

    assertEquals(
      LocalSitemapDownloadPipeline.parseSitemapInputRow(row),
      Some(SitemapDownloadInputRow("https://example.com/sitemap.xml"))
    )
  }

  test("reads sitemap URL from filtered country TSV rows") {
    val row = Vector(
      "target/commoncrawl-robots/example.ua/http-date-hash.txt",
      "example.ua",
      "https",
      "https://example.ua/sitemap.xml",
      "ukraine",
      "Ukraine",
      ".ua",
      "uk-UA"
    ).mkString("\t")

    assertEquals(
      LocalSitemapDownloadPipeline.parseSitemapInputRow(row),
      Some(SitemapDownloadInputRow("https://example.ua/sitemap.xml"))
    )
  }

  test("reads sitemap URL from WARC country sitemap TSV rows") {
    val row = CommonCrawlCountrySitemapsPipeline.extractedCountrySitemapTsvRow(
      "crawl-data/CC-MAIN-2026-17/segments/example/robotstxt/example.warc.gz",
      "2026-05-19T00:00:00Z",
      "https://example.ua/robots.txt",
      "https://example.ua/sitemap.xml",
      SitemapClassification(
        "ukraine",
        "Ukraine",
        ".ua",
        "uk-UA"
      )
    )

    assertEquals(
      LocalSitemapDownloadPipeline.parseSitemapInputRow(row),
      Some(SitemapDownloadInputRow("https://example.ua/sitemap.xml"))
    )
  }

  test("rejects sitemap TSV rows without a fourth field") {
    assertEquals(
      LocalSitemapDownloadPipeline.parseSitemapInputRow(
        "https://example.com/sitemap.xml"
      ),
      None
    )
  }
}
