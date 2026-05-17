final case class JobConfig(
    pathsUrl: String,
    outputPath: String,
    master: String
)

enum CliCommand:
  case Run(config: JobConfig)
  case ShowUsage

object Cli {
  val DefaultPathsUrl =
    "https://data.commoncrawl.org/crawl-data/CC-MAIN-2026-17/robotstxt.paths.gz"
  val DefaultOutputPath = "target/commoncrawl-robots"
  val DefaultMaster = "local-cluster[10,1,200]"

  def parseArgs(args: Array[String]): Either[String, CliCommand] =
    args.toList match {
      case Nil =>
        Right(
          CliCommand.Run(
            JobConfig(DefaultPathsUrl, DefaultOutputPath, DefaultMaster)
          )
        )
      case "--help" :: Nil =>
        Right(CliCommand.ShowUsage)
      case pathsUrl :: outputPath :: Nil =>
        Right(CliCommand.Run(JobConfig(pathsUrl, outputPath, DefaultMaster)))
      case pathsUrl :: outputPath :: master :: Nil =>
        Right(CliCommand.Run(JobConfig(pathsUrl, outputPath, master)))
      case _ =>
        Left(usage)
    }

  def usage: String =
    s"""Usage: sbt "run [paths_gz_url] [output_dir] [spark_master]"
       |
       |Defaults:
       |  paths_gz_url $DefaultPathsUrl
       |  output_dir   $DefaultOutputPath
       |  spark_master $DefaultMaster
       |
       |Pass a Common Crawl robotstxt.paths.gz URL. A sibling wat.paths.gz URL is
       |also accepted and is resolved to robotstxt.paths.gz before downloading.
       |""".stripMargin
}
