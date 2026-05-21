import java.net.URI
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.util.Locale

import org.apache.spark.sql.SparkSession
import org.netpreserve.jwarc.{WarcReader, WarcResponse}

import scala.jdk.CollectionConverters.*
import scala.util.Using

final case class ArchiveDownloadResult(
    archivePath: String,
    savedFiles: Int,
    rejectedFiles: Int,
    failure: String | Null
)

private final case class ArchiveExtractionResult(
    savedFiles: Int,
    rejectedFiles: Int
)

object CommonCrawlRobotsPipeline {
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
        downloadAndExtractArchive(
          archivePath,
          outputDirString,
          config.downloadPolicy
        )
      )
      .collect()

    val savedFiles = results.map(_.savedFiles).sum
    val rejectedFiles = results.map(_.rejectedFiles).sum
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
    println(s"Saved $savedFiles usable robots.txt captures into $outputDir")
    println(
      s"Rejected $rejectedFiles robots.txt captures without usable robots directives"
    )
    println(
      CommonCrawlRobotsArchiveSupport.downloadPolicySummary(
        config.downloadPolicy
      )
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

  private def downloadAndExtractArchive(
      archivePath: String,
      outputDir: String,
      policy: DownloadPolicyConfig
  ): ArchiveDownloadResult =
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

        val extractionResult =
          extractRobots(tempFile, Path.of(outputDir), archivePath)

        println(
          s"Archive $archivePath: saved ${extractionResult.savedFiles} usable robots.txt files; " +
            s"rejected ${extractionResult.rejectedFiles} captures without usable robots directives"
        )

        ArchiveDownloadResult(
          archivePath,
          extractionResult.savedFiles,
          extractionResult.rejectedFiles,
          null
        )
      }
    } catch {
      case exception: Exception =>
        ArchiveDownloadResult(
          archivePath,
          0,
          0,
          s"${exception.getClass.getSimpleName}: ${exception.getMessage}"
        )
    }

  private def extractRobots(
      archiveFile: Path,
      outputDir: Path,
      archivePath: String
  ): ArchiveExtractionResult = {
    var savedFiles = 0
    var rejectedFiles = 0

    Using.resource(WarcReader(archiveFile)) { reader =>
      reader.setLenient(true)

      reader.iterator().asScala.foreach {
        case response: WarcResponse =>
          if (
            CommonCrawlRobotsArchiveSupport.isRobotsTarget(response.target())
          ) {
            val targetUri = response.targetURI()
            val fileName =
              robotsFileName(targetUri, response.date().toString, archivePath)
            val destination = outputDir.resolve(fileName)

            val httpResponse = response.http()

            val content = Using.resource(
              CommonCrawlRobotsArchiveSupport.decodedBodyStream(
                httpResponse.body().stream(),
                httpResponse.headers().first("Content-Encoding").orElse("")
              )
            ) { body =>
              new String(body.readAllBytes(), StandardCharsets.UTF_8)
            }

            if (isValidRobotsTxt(content)) {
              Files.createDirectories(destination.getParent())
              Files.writeString(
                destination,
                content,
                StandardCharsets.UTF_8
              )
              savedFiles += 1
            } else {
              Files.deleteIfExists(destination)
              rejectedFiles += 1
            }
          } else {
            response.body().consume()
          }

        case record =>
          record.body().consume()
      }
    }

    ArchiveExtractionResult(savedFiles, rejectedFiles)
  }

  private def isValidRobotsTxt(content: String): Boolean = {
    RobotsTxtParser.isValid(content)
  }

  private def robotsFileName(
      targetUri: URI,
      captureDate: String,
      archivePath: String
  ): String = {
    val host = Option(targetUri.getHost())
      .filter(_.nonEmpty)
      .getOrElse("unknown-host")
      .toLowerCase(Locale.ROOT)
    val scheme = Option(targetUri.getScheme()).getOrElse("unknown")
    val digestInput = s"$targetUri\n$captureDate\n$archivePath"

    s"${CommonCrawlRobotsArchiveSupport.safePathPart(host)}/${CommonCrawlRobotsArchiveSupport
        .safePathPart(scheme)}-${CommonCrawlRobotsArchiveSupport
        .safePathPart(captureDate)}-${CommonCrawlRobotsArchiveSupport.sha256Hex(digestInput).take(16)}.txt"
  }
}
