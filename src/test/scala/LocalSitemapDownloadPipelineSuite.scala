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
}
