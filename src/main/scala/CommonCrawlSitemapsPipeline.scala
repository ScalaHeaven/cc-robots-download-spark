import java.io.BufferedWriter
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

import org.apache.spark.sql.SparkSession
import org.netpreserve.jwarc.{WarcReader, WarcResponse}

import scala.jdk.CollectionConverters.*
import scala.util.Using

final case class SitemapArchiveResult(
    archivePath: String,
    parsedFiles: Int,
    rejectedFiles: Int,
    savedSitemapLinks: Int,
    failure: String | Null
)

object CommonCrawlSitemapsPipeline {
  def run(spark: SparkSession, config: JobConfig): Unit = {
    val manifestUrl =
      CommonCrawlRobotsArchiveSupport.resolveManifestUrl(config.pathsUrl)
    val allArchivePaths =
      CommonCrawlRobotsArchiveSupport.readArchivePaths(
        manifestUrl,
        config.downloadPolicy
      )
    val skippedArchivePaths = allArchivePaths.drop(config.skipFiles)
    val archivePaths = config.maxFiles.fold(skippedArchivePaths) { maxFiles =>
      skippedArchivePaths.take(maxFiles)
    }
    val outputDir = Path.of(config.outputPath).toAbsolutePath.normalize()
    val outputDirString = outputDir.toString

    Files.createDirectories(outputDir)

    val results = spark.sparkContext
      .parallelize(archivePaths, archivePaths.size.max(1))
      .map(archivePath =>
        downloadAndExtractSitemaps(
          archivePath,
          outputDirString,
          config.downloadPolicy
        )
      )
      .collect()

    val parsedFiles = results.map(_.parsedFiles).sum
    val rejectedFiles = results.map(_.rejectedFiles).sum
    val savedSitemapLinks = results.map(_.savedSitemapLinks).sum
    val failures = results.filter(_.failure != null)

    println(
      s"Read ${archivePaths.size} robotstxt archive paths from $manifestUrl"
    )
    config.maxFiles.foreach { maxFiles =>
      println(
        s"Limited manifest processing to $maxFiles robotstxt archive files"
      )
    }
    if (config.skipFiles > 0) {
      println(
        s"Skipped ${config.skipFiles} robotstxt archive files before parsing"
      )
    }
    println(s"Parsed $parsedFiles usable robots.txt captures")
    println(
      s"Rejected $rejectedFiles robots.txt captures without usable robots directives"
    )
    println(
      CommonCrawlRobotsArchiveSupport.downloadPolicySummary(
        config.downloadPolicy
      )
    )
    println(s"Saved $savedSitemapLinks sitemap links into $outputDir")

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

  private def downloadAndExtractSitemaps(
      archivePath: String,
      outputDir: String,
      policy: DownloadPolicyConfig
  ): SitemapArchiveResult =
    try {
      val archiveUri =
        CommonCrawlRobotsArchiveSupport.archiveUriFor(archivePath)
      CommonCrawlRobotsArchiveSupport.withTempFile(
        "commoncrawl-robotstxt-",
        ".warc.gz"
      ) { tempFile =>
        CommonCrawlRobotsArchiveSupport.downloadToPath(
          archiveUri,
          tempFile,
          policy
        )

        val outputFile = Path
          .of(outputDir)
          .resolve(sitemapLinksFileName(archivePath))
        val result =
          extractSitemaps(tempFile, outputFile, archivePath)

        println(
          s"Archive $archivePath: parsed ${result.parsedFiles} usable robots.txt files; " +
            s"rejected ${result.rejectedFiles} captures without usable robots directives; " +
            s"saved ${result.savedSitemapLinks} sitemap links"
        )

        result
      }
    } catch {
      case exception: Exception =>
        SitemapArchiveResult(
          archivePath,
          0,
          0,
          0,
          s"${exception.getClass.getSimpleName}: ${exception.getMessage}"
        )
    }

  private def extractSitemaps(
      archiveFile: Path,
      outputFile: Path,
      archivePath: String
  ): SitemapArchiveResult = {
    var parsedFiles = 0
    var rejectedFiles = 0
    var savedSitemapLinks = 0
    var writer = Option.empty[BufferedWriter]

    Files.deleteIfExists(outputFile)

    def writeSitemapLink(
        captureDate: String,
        robotsUrl: String,
        sitemapUrl: String
    ): Unit = {
      val activeWriter = writer.getOrElse {
        Files.createDirectories(outputFile.getParent())
        val createdWriter =
          Files.newBufferedWriter(outputFile, StandardCharsets.UTF_8)
        writer = Some(createdWriter)
        createdWriter
      }

      activeWriter.write(
        Vector(
          archivePath,
          captureDate,
          robotsUrl,
          sitemapUrl
        ).map(tsvField).mkString("\t")
      )
      activeWriter.newLine()
      savedSitemapLinks += 1
    }

    try {
      Using.resource(WarcReader(archiveFile)) { reader =>
        reader.setLenient(true)

        reader.iterator().asScala.foreach {
          case response: WarcResponse =>
            if (
              CommonCrawlRobotsArchiveSupport.isRobotsTarget(response.target())
            ) {
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
                  writeSitemapLink(
                    response.date().toString,
                    response.target(),
                    sitemapUrl
                  )
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

    SitemapArchiveResult(
      archivePath,
      parsedFiles,
      rejectedFiles,
      savedSitemapLinks,
      null
    )
  }

  private def sitemapLinksFileName(archivePath: String): String =
    s"archive-${CommonCrawlRobotsArchiveSupport.sha256Hex(archivePath).take(16)}.sitemaps.tsv"

  private def tsvField(value: String): String =
    value
      .replace('\t', ' ')
      .replace('\r', ' ')
      .replace('\n', ' ')
}
