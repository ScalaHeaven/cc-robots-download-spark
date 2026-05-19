import java.util.Locale

import scala.util.Try

final case class DownloadTimeoutConfig(
    connectTimeoutSeconds: Int,
    readTimeoutSeconds: Int
)

final case class JobConfig(
    pathsUrl: String,
    outputPath: String,
    master: String,
    pipeline: PipelineMode,
    maxFiles: Option[Int] = None,
    downloadTimeouts: DownloadTimeoutConfig = Cli.DefaultDownloadTimeouts,
    countryFilter: Option[String] = None
)

enum PipelineMode:
  case Robots
  case Sitemaps
  case LocalSitemaps
  case FilterSitemaps
  case CountrySitemaps
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
  val DefaultDownloadTimeouts = DownloadTimeoutConfig(
    connectTimeoutSeconds = 10,
    readTimeoutSeconds = 30
  )

  def parseArgs(args: Array[String]): Either[String, CliCommand] =
    splitOptions(args.toList).flatMap {
      case (positionArgs, maxFiles, downloadTimeouts) =>
        positionArgs match {
          case Nil =>
            Right(
              CliCommand.Run(
                JobConfig(
                  DefaultPathsUrl,
                  DefaultOutputPath,
                  DefaultMaster,
                  PipelineMode.Robots,
                  maxFiles,
                  downloadTimeouts
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
              maxFiles = maxFiles,
              downloadTimeouts = downloadTimeouts
            )
          case "sitemaps" :: remaining =>
            parseRunConfig(
              remaining,
              DefaultSitemapsOutputPath,
              PipelineMode.Sitemaps,
              maxFiles = maxFiles,
              downloadTimeouts = downloadTimeouts
            )
          case "local-sitemaps" :: remaining =>
            parseLocalRunConfig(
              remaining,
              maxFiles,
              DefaultSitemapsOutputPath,
              PipelineMode.LocalSitemaps,
              defaultInputPath = DefaultOutputPath,
              defaultMaster = DefaultLocalSitemapsMaster,
              downloadTimeouts = downloadTimeouts
            )
          case "filter-sitemaps" :: remaining =>
            parseLocalRunConfig(
              remaining,
              maxFiles,
              DefaultFilteredSitemapsOutputPath,
              PipelineMode.FilterSitemaps,
              defaultInputPath = DefaultSitemapsOutputPath,
              defaultMaster = DefaultLocalSitemapsMaster,
              downloadTimeouts = downloadTimeouts
            )
          case "country-sitemaps" :: remaining =>
            parseCountrySitemapsConfig(remaining, maxFiles, downloadTimeouts)
          case "download-sitemaps" :: remaining =>
            parseLocalRunConfig(
              remaining,
              maxFiles,
              DefaultDownloadedSitemapLinksOutputPath,
              PipelineMode.DownloadSitemaps,
              defaultInputPath = DefaultFilteredSitemapsOutputPath,
              defaultMaster = DefaultLocalSitemapsMaster,
              downloadTimeouts = downloadTimeouts
            )
          case pathsUrl :: outputPath :: Nil =>
            Right(
              CliCommand.Run(
                JobConfig(
                  pathsUrl,
                  outputPath,
                  DefaultMaster,
                  PipelineMode.Robots,
                  maxFiles,
                  downloadTimeouts
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
                  maxFiles,
                  downloadTimeouts
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
      maxFiles: Option[Int] = None,
      downloadTimeouts: DownloadTimeoutConfig = DefaultDownloadTimeouts,
      countryFilter: Option[String] = None
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
              maxFiles,
              downloadTimeouts,
              countryFilter
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
              maxFiles,
              downloadTimeouts,
              countryFilter
            )
          )
        )
      case pathsUrl :: outputPath :: Nil =>
        Right(
          CliCommand.Run(
            JobConfig(
              pathsUrl,
              outputPath,
              defaultMaster,
              pipeline,
              maxFiles,
              downloadTimeouts,
              countryFilter
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
              pipeline,
              maxFiles,
              downloadTimeouts,
              countryFilter
            )
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
      defaultMaster: String,
      downloadTimeouts: DownloadTimeoutConfig,
      countryFilter: Option[String] = None
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
          defaultMaster,
          downloadTimeouts = downloadTimeouts,
          countryFilter = countryFilter
        )
    }

  private def parseCountrySitemapsConfig(
      args: List[String],
      maxFiles: Option[Int],
      downloadTimeouts: DownloadTimeoutConfig
  ): Either[String, CliCommand] =
    args match {
      case country :: remaining if country.trim.nonEmpty =>
        val defaultOutputPath =
          s"$DefaultFilteredSitemapsOutputPath/country/${countryPathKey(country)}"

        parseLocalRunConfig(
          remaining,
          maxFiles,
          defaultOutputPath,
          PipelineMode.CountrySitemaps,
          defaultInputPath = DefaultSitemapsOutputPath,
          defaultMaster = DefaultLocalSitemapsMaster,
          downloadTimeouts = downloadTimeouts,
          countryFilter = Some(country)
        )
      case _ =>
        Left(s"country-sitemaps requires a country key.\n\n$usage")
    }

  private def splitOptions(
      args: List[String]
  ): Either[String, (List[String], Option[Int], DownloadTimeoutConfig)] = {
    def parseLimit(value: String): Either[String, Int] =
      Try(value.toInt).toOption.filter(_ > 0) match {
        case Some(maxFiles) => Right(maxFiles)
        case None           =>
          Left(s"--max-files must be a positive integer.\n\n$usage")
      }

    def parseTimeout(optionName: String, value: String): Either[String, Int] =
      Try(value.toInt).toOption.filter(_ > 0) match {
        case Some(seconds) => Right(seconds)
        case None          =>
          Left(s"$optionName must be a positive integer.\n\n$usage")
      }

    def loop(
        remaining: List[String],
        positionArgs: Vector[String],
        maxFiles: Option[Int],
        downloadTimeouts: DownloadTimeoutConfig
    ): Either[String, (List[String], Option[Int], DownloadTimeoutConfig)] =
      remaining match {
        case Nil =>
          Right((positionArgs.toList, maxFiles, downloadTimeouts))
        case "--max-files" :: value :: tail =>
          parseLimit(value).flatMap(parsed =>
            loop(tail, positionArgs, Some(parsed), downloadTimeouts)
          )
        case "--max-files" :: Nil =>
          Left(s"--max-files requires a positive integer value.\n\n$usage")
        case option :: tail if option.startsWith("--max-files=") =>
          parseLimit(option.stripPrefix("--max-files=")).flatMap(parsed =>
            loop(tail, positionArgs, Some(parsed), downloadTimeouts)
          )
        case "--download-connect-timeout-seconds" :: value :: tail =>
          parseTimeout("--download-connect-timeout-seconds", value).flatMap {
            parsed =>
              loop(
                tail,
                positionArgs,
                maxFiles,
                downloadTimeouts.copy(connectTimeoutSeconds = parsed)
              )
          }
        case "--download-connect-timeout-seconds" :: Nil =>
          Left(
            "--download-connect-timeout-seconds requires a positive integer value.\n\n" +
              usage
          )
        case option :: tail
            if option.startsWith("--download-connect-timeout-seconds=") =>
          parseTimeout(
            "--download-connect-timeout-seconds",
            option.stripPrefix("--download-connect-timeout-seconds=")
          ).flatMap(parsed =>
            loop(
              tail,
              positionArgs,
              maxFiles,
              downloadTimeouts.copy(connectTimeoutSeconds = parsed)
            )
          )
        case "--download-read-timeout-seconds" :: value :: tail =>
          parseTimeout("--download-read-timeout-seconds", value).flatMap {
            parsed =>
              loop(
                tail,
                positionArgs,
                maxFiles,
                downloadTimeouts.copy(readTimeoutSeconds = parsed)
              )
          }
        case "--download-read-timeout-seconds" :: Nil =>
          Left(
            "--download-read-timeout-seconds requires a positive integer value.\n\n" +
              usage
          )
        case option :: tail
            if option.startsWith("--download-read-timeout-seconds=") =>
          parseTimeout(
            "--download-read-timeout-seconds",
            option.stripPrefix("--download-read-timeout-seconds=")
          ).flatMap(parsed =>
            loop(
              tail,
              positionArgs,
              maxFiles,
              downloadTimeouts.copy(readTimeoutSeconds = parsed)
            )
          )
        case arg :: tail =>
          loop(tail, positionArgs :+ arg, maxFiles, downloadTimeouts)
      }

    loop(args, Vector.empty, None, DefaultDownloadTimeouts)
  }

  private def countryPathKey(country: String): String = {
    val normalized = country.trim.toLowerCase(Locale.ROOT)
    val key = normalized
      .replaceAll("[^a-z0-9]+", "-")
      .stripPrefix("-")
      .stripSuffix("-")

    if (key.nonEmpty) key else "country"
  }

  def usage: String =
    s"""Usage:
       |  sbt "run [paths_gz_url] [output_dir] [spark_master] [--max-files N]"
       |  sbt "run robots [paths_gz_url] [output_dir] [spark_master] [--max-files N]"
       |  sbt "run sitemaps [paths_gz_url] [output_dir] [spark_master] [--max-files N]"
       |  sbt "run local-sitemaps [robots_dir] [output_dir] [spark_master]"
       |  sbt "run filter-sitemaps [sitemaps_dir] [output_dir] [spark_master]"
       |  sbt "run country-sitemaps <country> [sitemaps_dir] [output_dir] [spark_master]"
       |  sbt "run download-sitemaps [sitemaps_dir] [output_dir] [spark_master] [timeout options]"
       |
       |Defaults:
       |  paths_gz_url          $DefaultPathsUrl
       |  robots_dir            $DefaultOutputPath
       |  sitemaps_dir          $DefaultSitemapsOutputPath
       |  robots output_dir     $DefaultOutputPath
       |  sitemaps output_dir   $DefaultSitemapsOutputPath
       |  filtered output_dir   $DefaultFilteredSitemapsOutputPath
       |  country output_dir    $DefaultFilteredSitemapsOutputPath/country/<country>
       |  sitemap links output_dir $DefaultDownloadedSitemapLinksOutputPath
       |  spark_master          $DefaultMaster
       |  local-sitemaps master $DefaultLocalSitemapsMaster
       |  filter-sitemaps master $DefaultLocalSitemapsMaster
       |  country-sitemaps master $DefaultLocalSitemapsMaster
       |  download-sitemaps master $DefaultLocalSitemapsMaster
       |  download connect timeout ${DefaultDownloadTimeouts.connectTimeoutSeconds}s
       |  download read timeout ${DefaultDownloadTimeouts.readTimeoutSeconds}s
       |
       |Options:
       |  --max-files N        limit robotstxt archive files read from the manifest
       |  --download-connect-timeout-seconds N
       |                       HTTP connect timeout for downloads
       |  --download-read-timeout-seconds N
       |                       HTTP read timeout for downloads
       |
       |Pass a Common Crawl robotstxt.paths.gz URL. A sibling wat.paths.gz URL is
       |also accepted and is resolved to robotstxt.paths.gz before downloading.
       |""".stripMargin
}
