import scala.util.Try

final case class JobConfig(
    pathsUrl: String,
    outputPath: String,
    master: String,
    pipeline: PipelineMode,
    maxFiles: Option[Int] = None
)

enum PipelineMode:
  case Robots
  case Sitemaps
  case LocalSitemaps
  case FilterSitemaps
  case DownloadSitemaps

enum CliCommand:
  case Run(config: JobConfig)
  case ShowUsage

object Cli {
  val DefaultPathsUrl =
    "https://data.commoncrawl.org/crawl-data/CC-MAIN-2026-17/robotstxt.paths.gz"
  val DefaultOutputPath = "target/commoncrawl-robots"
  val DefaultSitemapsOutputPath = "target/commoncrawl-sitemaps"
  val DefaultFilteredSitemapsOutputPath = "target/filtered-sitemaps"
  val DefaultDownloadedSitemapLinksOutputPath =
    "target/downloaded-sitemap-links"
  val DefaultMaster = "local-cluster[1,1,200]"
  val DefaultLocalSitemapsMaster = "local[*]"

  def parseArgs(args: Array[String]): Either[String, CliCommand] =
    splitMaxFiles(args.toList).flatMap { case (positionArgs, maxFiles) =>
      positionArgs match {
        case Nil =>
          Right(
            CliCommand.Run(
              JobConfig(
                DefaultPathsUrl,
                DefaultOutputPath,
                DefaultMaster,
                PipelineMode.Robots,
                maxFiles
              )
            )
          )
        case "--help" :: Nil =>
          if (maxFiles.isEmpty) {
            Right(CliCommand.ShowUsage)
          } else {
            Left(usage)
          }
        case "robots" :: remaining =>
          parseRunConfig(
            remaining,
            DefaultOutputPath,
            PipelineMode.Robots,
            maxFiles = maxFiles
          )
        case "sitemaps" :: remaining =>
          parseRunConfig(
            remaining,
            DefaultSitemapsOutputPath,
            PipelineMode.Sitemaps,
            maxFiles = maxFiles
          )
        case "local-sitemaps" :: remaining =>
          parseLocalRunConfig(
            remaining,
            maxFiles,
            DefaultSitemapsOutputPath,
            PipelineMode.LocalSitemaps,
            defaultInputPath = DefaultOutputPath,
            defaultMaster = DefaultLocalSitemapsMaster
          )
        case "filter-sitemaps" :: remaining =>
          parseLocalRunConfig(
            remaining,
            maxFiles,
            DefaultFilteredSitemapsOutputPath,
            PipelineMode.FilterSitemaps,
            defaultInputPath = DefaultSitemapsOutputPath,
            defaultMaster = DefaultLocalSitemapsMaster
          )
        case "download-sitemaps" :: remaining =>
          parseLocalRunConfig(
            remaining,
            maxFiles,
            DefaultDownloadedSitemapLinksOutputPath,
            PipelineMode.DownloadSitemaps,
            defaultInputPath = DefaultFilteredSitemapsOutputPath,
            defaultMaster = DefaultLocalSitemapsMaster
          )
        case pathsUrl :: outputPath :: Nil =>
          Right(
            CliCommand.Run(
              JobConfig(
                pathsUrl,
                outputPath,
                DefaultMaster,
                PipelineMode.Robots,
                maxFiles
              )
            )
          )
        case pathsUrl :: outputPath :: master :: Nil =>
          Right(
            CliCommand.Run(
              JobConfig(
                pathsUrl,
                outputPath,
                master,
                PipelineMode.Robots,
                maxFiles
              )
            )
          )
        case _ =>
          Left(usage)
      }
    }

  private def parseRunConfig(
      args: List[String],
      defaultOutputPath: String,
      pipeline: PipelineMode,
      defaultInputPath: String = DefaultPathsUrl,
      defaultMaster: String = DefaultMaster,
      maxFiles: Option[Int] = None
  ): Either[String, CliCommand] =
    args match {
      case Nil =>
        Right(
          CliCommand.Run(
            JobConfig(
              defaultInputPath,
              defaultOutputPath,
              defaultMaster,
              pipeline,
              maxFiles
            )
          )
        )
      case inputPath :: Nil =>
        Right(
          CliCommand.Run(
            JobConfig(
              inputPath,
              defaultOutputPath,
              defaultMaster,
              pipeline,
              maxFiles
            )
          )
        )
      case pathsUrl :: outputPath :: Nil =>
        Right(
          CliCommand.Run(
            JobConfig(pathsUrl, outputPath, defaultMaster, pipeline, maxFiles)
          )
        )
      case pathsUrl :: outputPath :: master :: Nil =>
        Right(
          CliCommand.Run(
            JobConfig(pathsUrl, outputPath, master, pipeline, maxFiles)
          )
        )
      case _ =>
        Left(usage)
    }

  private def parseLocalRunConfig(
      args: List[String],
      maxFiles: Option[Int],
      defaultOutputPath: String,
      pipeline: PipelineMode,
      defaultInputPath: String,
      defaultMaster: String
  ): Either[String, CliCommand] =
    maxFiles match {
      case Some(_) =>
        Left(
          s"--max-files is only supported for robots and sitemaps runs.\n\n$usage"
        )
      case None =>
        parseRunConfig(
          args,
          defaultOutputPath,
          pipeline,
          defaultInputPath,
          defaultMaster
        )
    }

  private def splitMaxFiles(
      args: List[String]
  ): Either[String, (List[String], Option[Int])] = {
    def parseLimit(value: String): Either[String, Int] =
      Try(value.toInt).toOption.filter(_ > 0) match {
        case Some(maxFiles) => Right(maxFiles)
        case None           =>
          Left(s"--max-files must be a positive integer.\n\n$usage")
      }

    def loop(
        remaining: List[String],
        positionArgs: Vector[String],
        maxFiles: Option[Int]
    ): Either[String, (List[String], Option[Int])] =
      remaining match {
        case Nil =>
          Right((positionArgs.toList, maxFiles))
        case "--max-files" :: value :: tail =>
          parseLimit(value).flatMap(parsed =>
            loop(tail, positionArgs, Some(parsed))
          )
        case "--max-files" :: Nil =>
          Left(s"--max-files requires a positive integer value.\n\n$usage")
        case option :: tail if option.startsWith("--max-files=") =>
          parseLimit(option.stripPrefix("--max-files=")).flatMap(parsed =>
            loop(tail, positionArgs, Some(parsed))
          )
        case arg :: tail =>
          loop(tail, positionArgs :+ arg, maxFiles)
      }

    loop(args, Vector.empty, None)
  }

  def usage: String =
    s"""Usage:
       |  sbt "run [paths_gz_url] [output_dir] [spark_master] [--max-files N]"
       |  sbt "run robots [paths_gz_url] [output_dir] [spark_master] [--max-files N]"
       |  sbt "run sitemaps [paths_gz_url] [output_dir] [spark_master] [--max-files N]"
       |  sbt "run local-sitemaps [robots_dir] [output_dir] [spark_master]"
       |  sbt "run filter-sitemaps [sitemaps_dir] [output_dir] [spark_master]"
       |  sbt "run download-sitemaps [sitemaps_dir] [output_dir] [spark_master]"
       |
       |Defaults:
       |  paths_gz_url          $DefaultPathsUrl
       |  robots_dir            $DefaultOutputPath
       |  sitemaps_dir          $DefaultSitemapsOutputPath
       |  robots output_dir     $DefaultOutputPath
       |  sitemaps output_dir   $DefaultSitemapsOutputPath
       |  filtered output_dir   $DefaultFilteredSitemapsOutputPath
       |  sitemap links output_dir $DefaultDownloadedSitemapLinksOutputPath
       |  spark_master          $DefaultMaster
       |  local-sitemaps master $DefaultLocalSitemapsMaster
       |  filter-sitemaps master $DefaultLocalSitemapsMaster
       |  download-sitemaps master $DefaultLocalSitemapsMaster
       |
       |Options:
       |  --max-files N        limit robotstxt archive files read from the manifest
       |
       |Pass a Common Crawl robotstxt.paths.gz URL. A sibling wat.paths.gz URL is
       |also accepted and is resolved to robotstxt.paths.gz before downloading.
       |""".stripMargin
}
