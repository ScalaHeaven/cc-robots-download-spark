final case class JobConfig(
    pathsUrl: String,
    outputPath: String,
    master: String,
    pipeline: PipelineMode
)

enum PipelineMode:
  case Robots
  case Sitemaps

enum CliCommand:
  case Run(config: JobConfig)
  case ShowUsage

object Cli {
  val DefaultPathsUrl =
    "https://data.commoncrawl.org/crawl-data/CC-MAIN-2026-17/robotstxt.paths.gz"
  val DefaultOutputPath = "target/commoncrawl-robots"
  val DefaultSitemapsOutputPath = "target/commoncrawl-sitemaps"
  val DefaultMaster = "local-cluster[10,1,200]"

  def parseArgs(args: Array[String]): Either[String, CliCommand] =
    args.toList match {
      case Nil =>
        Right(
          CliCommand.Run(
            JobConfig(
              DefaultPathsUrl,
              DefaultOutputPath,
              DefaultMaster,
              PipelineMode.Robots
            )
          )
        )
      case "--help" :: Nil =>
        Right(CliCommand.ShowUsage)
      case "robots" :: remaining =>
        parseRunConfig(remaining, DefaultOutputPath, PipelineMode.Robots)
      case "sitemaps" :: remaining =>
        parseRunConfig(
          remaining,
          DefaultSitemapsOutputPath,
          PipelineMode.Sitemaps
        )
      case pathsUrl :: outputPath :: Nil =>
        Right(
          CliCommand.Run(
            JobConfig(
              pathsUrl,
              outputPath,
              DefaultMaster,
              PipelineMode.Robots
            )
          )
        )
      case pathsUrl :: outputPath :: master :: Nil =>
        Right(
          CliCommand.Run(
            JobConfig(pathsUrl, outputPath, master, PipelineMode.Robots)
          )
        )
      case _ =>
        Left(usage)
    }

  private def parseRunConfig(
      args: List[String],
      defaultOutputPath: String,
      pipeline: PipelineMode
  ): Either[String, CliCommand] =
    args match {
      case Nil =>
        Right(
          CliCommand.Run(
            JobConfig(
              DefaultPathsUrl,
              defaultOutputPath,
              DefaultMaster,
              pipeline
            )
          )
        )
      case pathsUrl :: outputPath :: Nil =>
        Right(
          CliCommand.Run(
            JobConfig(pathsUrl, outputPath, DefaultMaster, pipeline)
          )
        )
      case pathsUrl :: outputPath :: master :: Nil =>
        Right(
          CliCommand.Run(JobConfig(pathsUrl, outputPath, master, pipeline))
        )
      case _ =>
        Left(usage)
    }

  def usage: String =
    s"""Usage:
       |  sbt "run [paths_gz_url] [output_dir] [spark_master]"
       |  sbt "run robots [paths_gz_url] [output_dir] [spark_master]"
       |  sbt "run sitemaps [paths_gz_url] [output_dir] [spark_master]"
       |
       |Defaults:
       |  paths_gz_url          $DefaultPathsUrl
       |  robots output_dir     $DefaultOutputPath
       |  sitemaps output_dir   $DefaultSitemapsOutputPath
       |  spark_master          $DefaultMaster
       |
       |Pass a Common Crawl robotstxt.paths.gz URL. A sibling wat.paths.gz URL is
       |also accepted and is resolved to robotstxt.paths.gz before downloading.
       |""".stripMargin
}
