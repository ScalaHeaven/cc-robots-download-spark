class RobotsTxtParserSuite extends munit.FunSuite {
  test("keeps usable groups valid when owner mistakes produce warnings") {
    val robotsTxt = RobotsTxtParser.parse(
      """User-agent: *
        |Disallow: /private
        |Crawl-delay: eventually
        |""".stripMargin
    )

    assert(robotsTxt.isValid)
    assertEquals(robotsTxt.groups.size, 1)
    assertEquals(robotsTxt.warnings.map(_.lineNumber), Vector(3))
  }

  test("accepts sitemap-only robots files") {
    val robotsTxt = RobotsTxtParser.parse(
      "Sitemap: https://example.com/sitemap.xml\n"
    )

    assert(robotsTxt.isValid)
    assertEquals(
      robotsTxt.sitemaps,
      Vector("https://example.com/sitemap.xml")
    )
  }

  test("accepts root-relative sitemap values") {
    val robotsTxt = RobotsTxtParser.parse("Sitemap: /sitemap.xml\n")

    assert(robotsTxt.isValid)
    assertEquals(robotsTxt.sitemaps, Vector("/sitemap.xml"))
  }

  test("rejects directive-looking sitemap values") {
    val robotsTxt = RobotsTxtParser.parse(
      """Sitemap: User-agent: *
        |Sitemap: https://example.com/sitemap.xml
        |""".stripMargin
    )

    assert(robotsTxt.isValid)
    assertEquals(
      robotsTxt.sitemaps,
      Vector("https://example.com/sitemap.xml")
    )
    assertEquals(robotsTxt.warnings.map(_.lineNumber), Vector(1))
  }

  test("parses user-agent fields with UTF-8 byte order mark") {
    val robotsTxt = RobotsTxtParser.parse(
      "\uFEFFUser-agent: *\nDisallow: /tmp\n"
    )

    assert(robotsTxt.isValid)
    assertEquals(robotsTxt.groups.size, 1)
    assertEquals(robotsTxt.groups.map(_.userAgents), Vector(Vector("*")))
  }

  test("rejects bodies with no usable robots directives") {
    val robotsTxt = RobotsTxtParser.parse(
      "<html><title>Not found</title></html>\n"
    )

    assert(!robotsTxt.isValid)
  }

  test("records misplaced rules as owner mistakes without creating a group") {
    val robotsTxt = RobotsTxtParser.parse(
      """Disallow: /private
        |User-agent: *
        |Allow: /
        |""".stripMargin
    )

    assert(robotsTxt.isValid)
    assertEquals(robotsTxt.groups.size, 1)
    assertEquals(
      robotsTxt.groups.map(_.rules),
      Vector(Vector(RobotsRule.Allow("/")))
    )
    assertEquals(robotsTxt.warnings.map(_.lineNumber), Vector(1))
  }
}
