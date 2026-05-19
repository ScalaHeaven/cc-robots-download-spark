class CommonCrawlCountrySitemapsPipelineSuite extends munit.FunSuite {
  test("formats country sitemap rows with sitemap URL in fourth field") {
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
      row.split("\t", -1).toVector,
      Vector(
        "crawl-data/CC-MAIN-2026-17/segments/example/robotstxt/example.warc.gz",
        "2026-05-19T00:00:00Z",
        "https://example.ua/robots.txt",
        "https://example.ua/sitemap.xml",
        "ukraine",
        "Ukraine",
        ".ua",
        "uk-UA"
      )
    )
  }

  test("sanitizes TSV control characters in country sitemap rows") {
    val row = CommonCrawlCountrySitemapsPipeline.extractedCountrySitemapTsvRow(
      "archive\tpath",
      "date\rvalue",
      "https://example.ua/robots.txt",
      "https://example.ua/sitemap.xml\n",
      SitemapClassification(
        "ukraine",
        "Ukraine",
        ".ua",
        "unknown"
      )
    )

    val fields = row.split("\t", -1).toVector

    assertEquals(fields(0), "archive path")
    assertEquals(fields(1), "date value")
    assertEquals(fields(3), "https://example.ua/sitemap.xml ")
  }
}
