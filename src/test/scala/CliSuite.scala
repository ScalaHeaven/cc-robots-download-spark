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

    val localCountryCommand =
      Cli.parseArgs(
        Array("local-country-sitemaps", "ukraine", "--max-files", "1")
      )

    assert(localCountryCommand.isLeft)
    assert(localCountryCommand.left.exists(_.contains("country-sitemaps")))
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
        "https://data.commoncrawl.org/crawl-data/CC-MAIN-2026-17/robotstxt.paths.gz",
        "target/filtered-sitemaps/country/united-kingdom",
        "--max-files",
        "5000"
      )
    )

    assertEquals(
      command,
      Right(
        CliCommand.Run(
          JobConfig(
            "https://data.commoncrawl.org/crawl-data/CC-MAIN-2026-17/robotstxt.paths.gz",
            "target/filtered-sitemaps/country/united-kingdom",
            Cli.DefaultMaster,
            PipelineMode.CountrySitemaps,
            Some(5000),
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
          PipelineMode.CountrySitemaps,
          Cli.DefaultPathsUrl,
          "target/filtered-sitemaps/country/united-kingdom",
          Some("United Kingdom"),
          None
        )
      )
    )
  }

  test("parses local country sitemaps command") {
    val command = Cli.parseArgs(
      Array(
        "local-country-sitemaps",
        "ukraine",
        "target/commoncrawl-sitemaps",
        "target/filtered-sitemaps/country/ukraine"
      )
    )

    assertEquals(
      command.map(runConfigForCountry),
      Right(
        (
          PipelineMode.LocalCountrySitemaps,
          "target/commoncrawl-sitemaps",
          "target/filtered-sitemaps/country/ukraine",
          Some("ukraine"),
          None
        )
      )
    )
  }

  test("parses download policy options") {
    val command = Cli.parseArgs(
      Array(
        "download-sitemaps",
        "target/filtered-sitemaps/country/anguilla",
        "target/downloaded-sitemap-links",
        "--download-connect-timeout-seconds",
        "5",
        "--download-read-timeout-seconds=15",
        "--download-delay-seconds",
        "2"
      )
    )

    assertEquals(
      command.map(runConfigWithDownloadPolicy),
      Right(
        (
          PipelineMode.DownloadSitemaps,
          DownloadPolicyConfig(
            connectTimeoutSeconds = 5,
            readTimeoutSeconds = 15,
            delaySeconds = 2
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

  test("rejects negative download delay") {
    val command =
      Cli.parseArgs(Array("robots", "--download-delay-seconds=-1"))

    assert(command.isLeft)
    assert(command.left.exists(_.contains("non-negative integer")))
  }

  private def runConfig(command: CliCommand): Option[Int] =
    command match {
      case CliCommand.Run(config) => config.maxFiles
      case CliCommand.ShowUsage   => None
    }

  private def runConfigWithDownloadPolicy(
      command: CliCommand
  ): (PipelineMode, DownloadPolicyConfig) =
    command match {
      case CliCommand.Run(config) =>
        (config.pipeline, config.downloadPolicy)
      case CliCommand.ShowUsage =>
        fail("expected run config")
    }

  private def runConfigForCountry(
      command: CliCommand
  ): (PipelineMode, String, String, Option[String], Option[Int]) =
    command match {
      case CliCommand.Run(config) =>
        (
          config.pipeline,
          config.pathsUrl,
          config.outputPath,
          config.countryFilter,
          config.maxFiles
        )
      case CliCommand.ShowUsage =>
        fail("expected run config")
    }
}
