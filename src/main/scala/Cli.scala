import java.util.Locale

import scala.util.Try

final case class DownloadPolicyConfig(
    connectTimeoutSeconds: Int,
    readTimeoutSeconds: Int,
    delaySeconds: Int
)

final case class JobConfig(
    pathsUrl: String,
    outputPath: String,
    master: String,
    pipeline: PipelineMode,
    maxFiles: Option[Int] = None,
    downloadPolicy: DownloadPolicyConfig = Cli.DefaultDownloadPolicy,
    countryFilter: Option[String] = None,
    skipFiles: Int = 0
)

enum PipelineMode:
  case Robots
  case Sitemaps
  case LocalSitemaps
  case FilterSitemaps
  case LocalCountrySitemaps
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
  val DefaultDownloadPolicy = DownloadPolicyConfig(
    connectTimeoutSeconds = 10,
    readTimeoutSeconds = 30,
    delaySeconds = 1
  )

  def parseArgs(args: Array[String]): Either[String, CliCommand] =
    splitOptions(args.toList).flatMap {
      case (positionArgs, maxFiles, skipFiles, downloadPolicy) =>
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
                  downloadPolicy,
                  skipFiles = skipFiles
                )
              )
            )
          case "--help" :: Nil =>
            if (maxFiles.isEmpty && skipFiles == 0) {
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
              skipFiles = skipFiles,
              downloadPolicy = downloadPolicy
            )
          case "sitemaps" :: remaining =>
            parseRunConfig(
              remaining,
              DefaultSitemapsOutputPath,
              PipelineMode.Sitemaps,
              maxFiles = maxFiles,
              skipFiles = skipFiles,
              downloadPolicy = downloadPolicy
            )
          case "local-sitemaps" :: remaining =>
            parseLocalRunConfig(
              remaining,
              maxFiles,
              skipFiles,
              DefaultSitemapsOutputPath,
              PipelineMode.LocalSitemaps,
              defaultInputPath = DefaultOutputPath,
              defaultMaster = DefaultLocalSitemapsMaster,
              downloadPolicy = downloadPolicy
            )
          case "filter-sitemaps" :: remaining =>
            parseLocalRunConfig(
              remaining,
              maxFiles,
              skipFiles,
              DefaultFilteredSitemapsOutputPath,
              PipelineMode.FilterSitemaps,
              defaultInputPath = DefaultSitemapsOutputPath,
              defaultMaster = DefaultLocalSitemapsMaster,
              downloadPolicy = downloadPolicy
            )
          case "local-country-sitemaps" :: remaining =>
            parseCountrySitemapsConfig(
              remaining,
              maxFiles,
              skipFiles,
              downloadPolicy,
              pipeline = PipelineMode.LocalCountrySitemaps,
              defaultInputPath = DefaultSitemapsOutputPath,
              defaultMaster = DefaultLocalSitemapsMaster,
              allowMaxFiles = false,
              commandName = "local-country-sitemaps"
            )
          case "country-sitemaps" :: remaining =>
            parseCountrySitemapsConfig(
              remaining,
              maxFiles,
              skipFiles,
              downloadPolicy,
              pipeline = PipelineMode.CountrySitemaps,
              defaultInputPath = DefaultPathsUrl,
              defaultMaster = DefaultMaster,
              allowMaxFiles = true,
              commandName = "country-sitemaps"
            )
          case "download-sitemaps" :: remaining =>
            parseLocalRunConfig(
              remaining,
              maxFiles,
              skipFiles,
              DefaultDownloadedSitemapLinksOutputPath,
              PipelineMode.DownloadSitemaps,
              defaultInputPath = DefaultFilteredSitemapsOutputPath,
              defaultMaster = DefaultLocalSitemapsMaster,
              downloadPolicy = downloadPolicy
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
                  downloadPolicy,
                  skipFiles = skipFiles
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
                  downloadPolicy,
                  skipFiles = skipFiles
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
      skipFiles: Int = 0,
      downloadPolicy: DownloadPolicyConfig = DefaultDownloadPolicy,
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
              downloadPolicy,
              countryFilter,
              skipFiles
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
              downloadPolicy,
              countryFilter,
              skipFiles
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
              downloadPolicy,
              countryFilter,
              skipFiles
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
              downloadPolicy,
              countryFilter,
              skipFiles
            )
          )
        )
      case _ =>
        Left(usage)
    }

  private def parseLocalRunConfig(
      args: List[String],
      maxFiles: Option[Int],
      skipFiles: Int,
      defaultOutputPath: String,
      pipeline: PipelineMode,
      defaultInputPath: String,
      defaultMaster: String,
      downloadPolicy: DownloadPolicyConfig,
      countryFilter: Option[String] = None
  ): Either[String, CliCommand] =
    (maxFiles, skipFiles) match {
      case (Some(_), _) =>
        Left(
          s"--max-files is only supported for robots, sitemaps, and country-sitemaps runs.\n\n$usage"
        )
      case (None, skipped) if skipped > 0 =>
        Left(
          s"--skip-files is only supported for robots, sitemaps, and country-sitemaps runs.\n\n$usage"
        )
      case (None, _) =>
        parseRunConfig(
          args,
          defaultOutputPath,
          pipeline,
          defaultInputPath,
          defaultMaster,
          skipFiles = skipFiles,
          downloadPolicy = downloadPolicy,
          countryFilter = countryFilter
        )
    }

  private def parseCountrySitemapsConfig(
      args: List[String],
      maxFiles: Option[Int],
      skipFiles: Int,
      downloadPolicy: DownloadPolicyConfig,
      pipeline: PipelineMode,
      defaultInputPath: String,
      defaultMaster: String,
      allowMaxFiles: Boolean,
      commandName: String
  ): Either[String, CliCommand] =
    args match {
      case country :: remaining if country.trim.nonEmpty =>
        val defaultOutputPath =
          s"$DefaultFilteredSitemapsOutputPath/country/${countryPathKey(country)}"

        if (allowMaxFiles) {
          parseRunConfig(
            remaining,
            defaultOutputPath,
            pipeline,
            defaultInputPath = defaultInputPath,
            defaultMaster = defaultMaster,
            maxFiles = maxFiles,
            skipFiles = skipFiles,
            downloadPolicy = downloadPolicy,
            countryFilter = Some(country)
          )
        } else {
          parseLocalRunConfig(
            remaining,
            maxFiles,
            skipFiles,
            defaultOutputPath,
            pipeline,
            defaultInputPath = defaultInputPath,
            defaultMaster = defaultMaster,
            downloadPolicy = downloadPolicy,
            countryFilter = Some(country)
          )
        }
      case _ =>
        Left(s"$commandName requires a country key.\n\n$usage")
    }

  private def splitOptions(
      args: List[String]
  ): Either[String, (List[String], Option[Int], Int, DownloadPolicyConfig)] = {
    def parseLimit(value: String): Either[String, Int] =
      Try(value.toInt).toOption.filter(_ > 0) match {
        case Some(maxFiles) => Right(maxFiles)
        case None           =>
          Left(s"--max-files must be a positive integer.\n\n$usage")
      }

    def parseSkip(value: String): Either[String, Int] =
      Try(value.toInt).toOption.filter(_ >= 0) match {
        case Some(skipFiles) => Right(skipFiles)
        case None            =>
          Left(s"--skip-files must be a non-negative integer.\n\n$usage")
      }

    def parseTimeout(optionName: String, value: String): Either[String, Int] =
      Try(value.toInt).toOption.filter(_ > 0) match {
        case Some(seconds) => Right(seconds)
        case None          =>
          Left(s"$optionName must be a positive integer.\n\n$usage")
      }

    def parseDelay(value: String): Either[String, Int] =
      Try(value.toInt).toOption.filter(_ >= 0) match {
        case Some(seconds) => Right(seconds)
        case None          =>
          Left(
            s"--download-delay-seconds must be a non-negative integer.\n\n$usage"
          )
      }

    def loop(
        remaining: List[String],
        positionArgs: Vector[String],
        maxFiles: Option[Int],
        skipFiles: Int,
        downloadPolicy: DownloadPolicyConfig
    ): Either[String, (List[String], Option[Int], Int, DownloadPolicyConfig)] =
      remaining match {
        case Nil =>
          Right((positionArgs.toList, maxFiles, skipFiles, downloadPolicy))
        case "--max-files" :: value :: tail =>
          parseLimit(value).flatMap(parsed =>
            loop(tail, positionArgs, Some(parsed), skipFiles, downloadPolicy)
          )
        case "--max-files" :: Nil =>
          Left(s"--max-files requires a positive integer value.\n\n$usage")
        case option :: tail if option.startsWith("--max-files=") =>
          parseLimit(option.stripPrefix("--max-files=")).flatMap(parsed =>
            loop(tail, positionArgs, Some(parsed), skipFiles, downloadPolicy)
          )
        case "--skip-files" :: value :: tail =>
          parseSkip(value).flatMap(parsed =>
            loop(tail, positionArgs, maxFiles, parsed, downloadPolicy)
          )
        case "--skip-files" :: Nil =>
          Left(s"--skip-files requires a non-negative integer value.\n\n$usage")
        case option :: tail if option.startsWith("--skip-files=") =>
          parseSkip(option.stripPrefix("--skip-files=")).flatMap(parsed =>
            loop(tail, positionArgs, maxFiles, parsed, downloadPolicy)
          )
        case "--download-connect-timeout-seconds" :: value :: tail =>
          parseTimeout("--download-connect-timeout-seconds", value).flatMap {
            parsed =>
              loop(
                tail,
                positionArgs,
                maxFiles,
                skipFiles,
                downloadPolicy.copy(connectTimeoutSeconds = parsed)
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
              skipFiles,
              downloadPolicy.copy(connectTimeoutSeconds = parsed)
            )
          )
        case "--download-read-timeout-seconds" :: value :: tail =>
          parseTimeout("--download-read-timeout-seconds", value).flatMap {
            parsed =>
              loop(
                tail,
                positionArgs,
                maxFiles,
                skipFiles,
                downloadPolicy.copy(readTimeoutSeconds = parsed)
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
              skipFiles,
              downloadPolicy.copy(readTimeoutSeconds = parsed)
            )
          )
        case "--download-delay-seconds" :: value :: tail =>
          parseDelay(value).flatMap(parsed =>
            loop(
              tail,
              positionArgs,
              maxFiles,
              skipFiles,
              downloadPolicy.copy(delaySeconds = parsed)
            )
          )
        case "--download-delay-seconds" :: Nil =>
          Left(
            "--download-delay-seconds requires a non-negative integer value.\n\n" +
              usage
          )
        case option :: tail if option.startsWith("--download-delay-seconds=") =>
          parseDelay(option.stripPrefix("--download-delay-seconds=")).flatMap {
            parsed =>
              loop(
                tail,
                positionArgs,
                maxFiles,
                skipFiles,
                downloadPolicy.copy(delaySeconds = parsed)
              )
          }
        case arg :: tail =>
          loop(tail, positionArgs :+ arg, maxFiles, skipFiles, downloadPolicy)
      }

    loop(args, Vector.empty, None, 0, DefaultDownloadPolicy)
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
       |  sbt "run [paths_gz_url] [output_dir] [spark_master] [--skip-files N] [--max-files N]"
       |  sbt "run robots [paths_gz_url] [output_dir] [spark_master] [--skip-files N] [--max-files N]"
       |  sbt "run sitemaps [paths_gz_url] [output_dir] [spark_master] [--skip-files N] [--max-files N]"
       |  sbt "run local-sitemaps [robots_dir] [output_dir] [spark_master]"
       |  sbt "run filter-sitemaps [sitemaps_dir] [output_dir] [spark_master]"
       |  sbt "run country-sitemaps <country> [paths_gz_url] [output_dir] [spark_master] [--skip-files N] [--max-files N]"
       |  sbt "run local-country-sitemaps <country> [sitemaps_dir] [output_dir] [spark_master]"
       |  sbt "run download-sitemaps [sitemaps_dir] [output_dir] [spark_master] [download options]"
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
       |  country-sitemaps master $DefaultMaster
       |  local-country-sitemaps master $DefaultLocalSitemapsMaster
       |  download-sitemaps master $DefaultLocalSitemapsMaster
       |  download connect timeout ${DefaultDownloadPolicy.connectTimeoutSeconds}s
       |  download read timeout ${DefaultDownloadPolicy.readTimeoutSeconds}s
       |  download delay ${DefaultDownloadPolicy.delaySeconds}s
       |
       |Options:
       |  --max-files N        limit robotstxt archive files read from the manifest
       |  --skip-files N       skip this many robotstxt archive files before parsing
       |  --download-connect-timeout-seconds N
       |                       HTTP connect timeout for downloads
       |  --download-read-timeout-seconds N
       |                       HTTP read timeout for downloads
       |  --download-delay-seconds N
       |                       minimum delay between HTTP request starts in each JVM
       |
       |Pass a Common Crawl robotstxt.paths.gz URL. A sibling wat.paths.gz URL is
       |also accepted and is resolved to robotstxt.paths.gz before downloading.
       |""".stripMargin
}
