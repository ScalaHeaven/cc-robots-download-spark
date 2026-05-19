class CliSuite extends munit.FunSuite {
  test("parses max files for default robots command") {
    val command = Cli.parseArgs(
      Array(
        "https://data.commoncrawl.org/crawl-data/CC-MAIN-2026-17/robotstxt.paths.gz",
        "target/commoncrawl-robots",
        "--max-files",
        "5000"
      )
    )

    assertEquals(command.map(runConfig), Right(Some(5000)))
  }

  test("parses max files for explicit robots command") {
    val command = Cli.parseArgs(
      Array(
        "robots",
        "--max-files=5000",
        "https://data.commoncrawl.org/crawl-data/CC-MAIN-2026-17/robotstxt.paths.gz",
        "target/commoncrawl-robots"
      )
    )

    assertEquals(command.map(runConfig), Right(Some(5000)))
  }

  test("rejects non-positive max files") {
    val command = Cli.parseArgs(Array("robots", "--max-files", "0"))

    assert(command.isLeft)
    assert(command.left.exists(_.contains("positive integer")))
  }

  test("rejects max files on local sitemap commands") {
    val command =
      Cli.parseArgs(Array("local-sitemaps", "--max-files", "5000"))

    assert(command.isLeft)
    assert(command.left.exists(_.contains("only supported for robots")))
  }

  test("parses download sitemaps command") {
    val command = Cli.parseArgs(
      Array(
        "download-sitemaps",
        "target/filtered-sitemaps/country/anguilla",
        "target/downloaded-sitemap-links"
      )
    )

    assertEquals(
      command,
      Right(
        CliCommand.Run(
          JobConfig(
            "target/filtered-sitemaps/country/anguilla",
            "target/downloaded-sitemap-links",
            Cli.DefaultLocalSitemapsMaster,
            PipelineMode.DownloadSitemaps,
            None
          )
        )
      )
    )
  }

  test("parses country sitemaps command") {
    val command = Cli.parseArgs(
      Array(
        "country-sitemaps",
        "united-kingdom",
        "target/commoncrawl-sitemaps",
        "target/filtered-sitemaps/country/united-kingdom"
      )
    )

    assertEquals(
      command,
      Right(
        CliCommand.Run(
          JobConfig(
            "target/commoncrawl-sitemaps",
            "target/filtered-sitemaps/country/united-kingdom",
            Cli.DefaultLocalSitemapsMaster,
            PipelineMode.CountrySitemaps,
            None,
            countryFilter = Some("united-kingdom")
          )
        )
      )
    )
  }

  test("country sitemaps defaults output path from country key") {
    val command = Cli.parseArgs(Array("country-sitemaps", "United Kingdom"))

    assertEquals(
      command.map(runConfigForCountry),
      Right(
        (
          "target/commoncrawl-sitemaps",
          "target/filtered-sitemaps/country/united-kingdom",
          Some("United Kingdom")
        )
      )
    )
  }

  test("parses download timeout options") {
    val command = Cli.parseArgs(
      Array(
        "download-sitemaps",
        "target/filtered-sitemaps/country/anguilla",
        "target/downloaded-sitemap-links",
        "--download-connect-timeout-seconds",
        "5",
        "--download-read-timeout-seconds=15"
      )
    )

    assertEquals(
      command.map(runConfigWithTimeouts),
      Right(
        (
          PipelineMode.DownloadSitemaps,
          DownloadTimeoutConfig(
            connectTimeoutSeconds = 5,
            readTimeoutSeconds = 15
          )
        )
      )
    )
  }

  test("rejects non-positive download timeouts") {
    val command = Cli.parseArgs(
      Array("download-sitemaps", "--download-read-timeout-seconds", "0")
    )

    assert(command.isLeft)
    assert(command.left.exists(_.contains("positive integer")))
  }

  private def runConfig(command: CliCommand): Option[Int] =
    command match {
      case CliCommand.Run(config) => config.maxFiles
      case CliCommand.ShowUsage   => None
    }

  private def runConfigWithTimeouts(
      command: CliCommand
  ): (PipelineMode, DownloadTimeoutConfig) =
    command match {
      case CliCommand.Run(config) =>
        (config.pipeline, config.downloadTimeouts)
      case CliCommand.ShowUsage =>
        fail("expected run config")
    }

  private def runConfigForCountry(
      command: CliCommand
  ): (String, String, Option[String]) =
    command match {
      case CliCommand.Run(config) =>
        (config.pathsUrl, config.outputPath, config.countryFilter)
      case CliCommand.ShowUsage =>
        fail("expected run config")
    }
}
