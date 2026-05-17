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

  private def runConfig(command: CliCommand): Option[Int] =
    command match {
      case CliCommand.Run(config) => config.maxFiles
      case CliCommand.ShowUsage   => None
    }
}
