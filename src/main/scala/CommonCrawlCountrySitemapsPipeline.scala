import java.io.BufferedWriter
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

import org.apache.spark.sql.SparkSession
import org.netpreserve.jwarc.{WarcReader, WarcResponse}

import scala.jdk.CollectionConverters.*
import scala.util.Using

final case class CountrySitemapArchiveResult(
    archivePath: String,
    parsedFiles: Int,
    rejectedFiles: Int,
    matchedSitemapLinks: Int,
    failure: String | Null
)

object CommonCrawlCountrySitemapsPipeline {
  def run(spark: SparkSession, config: JobConfig): Unit = {
    val suffixes = SitemapCountryLocaleClassifier.loadDefaultCountrySuffixes()
    val countryQuery = config.countryFilter.getOrElse {
      throw IllegalArgumentException(
        "country-sitemaps requires a country key, country name, or suffix"
      )
    }
    val selectedCountry = SitemapCountryLocaleClassifier
      .selectCountry(countryQuery, suffixes)
      .getOrElse {
        val examples = SitemapCountryLocaleClassifier
          .supportedCountryKeys(suffixes)
          .take(12)
          .mkString(", ")
        throw IllegalArgumentException(
          s"Unknown sitemap country '$countryQuery'. Supported country keys include: $examples"
        )
      }
    val selectedSuffixes =
      SitemapCountryLocaleClassifier.countrySuffixes(selectedCountry, suffixes)

    val manifestUrl =
      CommonCrawlRobotsArchiveSupport.resolveManifestUrl(config.pathsUrl)
    val allArchivePaths =
      CommonCrawlRobotsArchiveSupport.readArchivePaths(manifestUrl)
    val archivePaths = config.maxFiles.fold(allArchivePaths) { maxFiles =>
      allArchivePaths.take(maxFiles)
    }
    val outputDir = Path.of(config.outputPath).toAbsolutePath.normalize()
    val outputDirString = outputDir.toString

    Files.createDirectories(outputDir)
    deleteArchiveOutputFiles(outputDir)

    val results = spark.sparkContext
      .parallelize(archivePaths, archivePaths.size.max(1))
      .map(archivePath =>
        downloadAndExtractCountrySitemaps(
          archivePath,
          outputDirString,
          selectedCountry,
          selectedSuffixes
        )
      )
      .collect()

    val parsedFiles = results.map(_.parsedFiles).sum
    val rejectedFiles = results.map(_.rejectedFiles).sum
    val matchedSitemapLinks = results.map(_.matchedSitemapLinks).sum
    val failures = results.filter(_.failure != null)

    println(
      s"Read ${archivePaths.size} robotstxt archive paths from $manifestUrl"
    )
    config.maxFiles.foreach { maxFiles =>
      println(
        s"Limited manifest processing to $maxFiles robotstxt archive files"
      )
    }
    println(s"Parsed $parsedFiles usable robots.txt captures")
    println(
      s"Rejected $rejectedFiles robots.txt captures without usable robots directives"
    )
    println(
      s"Saved $matchedSitemapLinks ${selectedCountry.countryName} sitemap links into $outputDir"
    )

    if (failures.nonEmpty) {
      failures.foreach { failure =>
        System.err.println(
          s"Failed ${failure.archivePath}: ${failure.failure}"
        )
      }
      throw IllegalStateException(
        s"${failures.length} robotstxt archives failed to download or extract"
      )
    }
  }

  private def deleteArchiveOutputFiles(outputDir: Path): Unit =
    Using.resource(Files.list(outputDir)) { paths =>
      paths
        .iterator()
        .asScala
        .filter(Files.isRegularFile(_))
        .filter(path =>
          path
            .getFileName()
            .toString
            .matches("archive-[a-f0-9]{16}\\.sitemaps\\.tsv")
        )
        .foreach(Files.deleteIfExists)
    }

  private def downloadAndExtractCountrySitemaps(
      archivePath: String,
      outputDir: String,
      selectedCountry: SelectedCountry,
      suffixes: Vector[CountrySuffix]
  ): CountrySitemapArchiveResult =
    try {
      val archiveUri =
        CommonCrawlRobotsArchiveSupport.archiveUriFor(archivePath)
      CommonCrawlRobotsArchiveSupport.withTempFile(
        "commoncrawl-robotstxt-",
        ".warc.gz"
      ) { tempFile =>
        CommonCrawlRobotsArchiveSupport.downloadToPath(archiveUri, tempFile)

        val outputFile = Path
          .of(outputDir)
          .resolve(sitemapLinksFileName(archivePath))
        val result =
          extractCountrySitemaps(
            tempFile,
            outputFile,
            archivePath,
            suffixes
          )

        println(
          s"Archive $archivePath: parsed ${result.parsedFiles} usable robots.txt files; " +
            s"rejected ${result.rejectedFiles} captures without usable robots directives; " +
            s"saved ${result.matchedSitemapLinks} ${selectedCountry.countryName} sitemap links"
        )

        result
      }
    } catch {
      case exception: Exception =>
        CountrySitemapArchiveResult(
          archivePath,
          0,
          0,
          0,
          s"${exception.getClass.getSimpleName}: ${exception.getMessage}"
        )
    }

  private def extractCountrySitemaps(
      archiveFile: Path,
      outputFile: Path,
      archivePath: String,
      suffixes: Vector[CountrySuffix]
  ): CountrySitemapArchiveResult = {
    var parsedFiles = 0
    var rejectedFiles = 0
    var matchedSitemapLinks = 0
    var writer = Option.empty[BufferedWriter]

    Files.deleteIfExists(outputFile)

    def writeSitemapLink(
        captureDate: String,
        robotsUrl: String,
        sitemapUrl: String,
        classification: SitemapClassification
    ): Unit = {
      val activeWriter = writer.getOrElse {
        Files.createDirectories(outputFile.getParent())
        val createdWriter =
          Files.newBufferedWriter(outputFile, StandardCharsets.UTF_8)
        writer = Some(createdWriter)
        createdWriter
      }

      activeWriter.write(
        extractedCountrySitemapTsvRow(
          archivePath,
          captureDate,
          robotsUrl,
          sitemapUrl,
          classification
        )
      )
      activeWriter.newLine()
      matchedSitemapLinks += 1
    }

    try {
      Using.resource(WarcReader(archiveFile)) { reader =>
        reader.setLenient(true)

        reader.iterator().asScala.foreach {
          case response: WarcResponse =>
            if (
              CommonCrawlRobotsArchiveSupport.isRobotsTarget(response.target())
            ) {
              val robotsUrl = response.target()
              val robotsHost = SitemapCountryLocaleClassifier
                .extractHost(robotsUrl)
                .getOrElse("unknown-host")
              val httpResponse = response.http()
              val content = Using.resource(
                CommonCrawlRobotsArchiveSupport.decodedBodyStream(
                  httpResponse.body().stream(),
                  httpResponse.headers().first("Content-Encoding").orElse("")
                )
              ) { body =>
                new String(body.readAllBytes(), StandardCharsets.UTF_8)
              }
              val robotsTxt = RobotsTxtParser.parse(content)

              if (robotsTxt.isValid) {
                parsedFiles += 1
                robotsTxt.sitemaps.distinct.foreach { sitemapUrl =>
                  SitemapCountryLocaleClassifier
                    .classify(robotsHost, sitemapUrl, suffixes)
                    .foreach { classification =>
                      writeSitemapLink(
                        response.date().toString,
                        robotsUrl,
                        sitemapUrl,
                        classification
                      )
                    }
                }
              } else {
                rejectedFiles += 1
              }
            } else {
              response.body().consume()
            }

          case record =>
            record.body().consume()
        }
      }
    } finally {
      writer.foreach(_.close())
    }

    CountrySitemapArchiveResult(
      archivePath,
      parsedFiles,
      rejectedFiles,
      matchedSitemapLinks,
      null
    )
  }

  private def sitemapLinksFileName(archivePath: String): String =
    s"archive-${CommonCrawlRobotsArchiveSupport.sha256Hex(archivePath).take(16)}.sitemaps.tsv"

  def extractedCountrySitemapTsvRow(
      archivePath: String,
      captureDate: String,
      robotsUrl: String,
      sitemapUrl: String,
      classification: SitemapClassification
  ): String =
    Vector(
      archivePath,
      captureDate,
      robotsUrl,
      sitemapUrl,
      classification.countryKey,
      classification.countryName,
      classification.matchedSuffix,
      classification.languageRegion
    ).map(tsvField).mkString("\t")

  private def tsvField(value: String): String =
    value
      .replace('\t', ' ')
      .replace('\r', ' ')
      .replace('\n', ' ')
}
