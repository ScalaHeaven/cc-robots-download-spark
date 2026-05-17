import java.io.{BufferedInputStream, BufferedWriter, InputStream}
import java.net.URI
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.util.zip.GZIPInputStream

import org.apache.spark.TaskContext
import org.apache.spark.sql.SparkSession

import scala.jdk.CollectionConverters.*
import scala.util.{Failure, Success, Try, Using}

final case class SitemapDownloadInputRow(
    sourceFile: String,
    robotsHost: String,
    robotsScheme: String,
    seedSitemapUrl: String
)

final case class LocalSitemapDownloadResult(
    partitionId: Int,
    readRows: Int,
    malformedRows: Int,
    downloadedSitemaps: Int,
    invalidSitemaps: Int,
    failedDownloads: Int,
    savedLinks: Int,
    failure: String | Null
)

object LocalSitemapDownloadPipeline {
  private val MaxSitemapIndexDepth = 20

  def run(spark: SparkSession, config: JobConfig): Unit = {
    val inputDir = Path.of(config.pathsUrl).toAbsolutePath.normalize()
    val outputDir = Path.of(config.outputPath).toAbsolutePath.normalize()
    val outputDirString = outputDir.toString

    if (!Files.isDirectory(inputDir)) {
      throw IllegalArgumentException(
        s"Local sitemap TSV input path is not a directory: $inputDir"
      )
    }

    Files.createDirectories(outputDir)
    deletePartitionOutputFiles(outputDir)

    val inputFiles = listSitemapFiles(inputDir)
    val partitionCount =
      inputFiles.size.min(spark.sparkContext.defaultParallelism * 4).max(1)
    val results = spark.sparkContext
      .parallelize(inputFiles.map(_.toString), partitionCount)
      .mapPartitionsWithIndex { case (partitionId, files) =>
        Iterator(
          downloadPartitionSitemaps(
            partitionId,
            files.toVector,
            outputDirString
          )
        )
      }
      .collect()

    val readRows = results.map(_.readRows).sum
    val malformedRows = results.map(_.malformedRows).sum
    val downloadedSitemaps = results.map(_.downloadedSitemaps).sum
    val invalidSitemaps = results.map(_.invalidSitemaps).sum
    val failedDownloads = results.map(_.failedDownloads).sum
    val savedLinks = results.map(_.savedLinks).sum
    val failures = results.filter(_.failure != null)

    println(s"Read ${inputFiles.size} local sitemap TSV files from $inputDir")
    println(s"Read $readRows sitemap rows")
    println(s"Skipped $malformedRows malformed sitemap rows")
    println(s"Downloaded and validated $downloadedSitemaps sitemap files")
    println(s"Rejected $invalidSitemaps invalid sitemap files")
    println(s"Failed to download $failedDownloads sitemap files")
    println(s"Saved $savedLinks extracted sitemap links into $outputDir")

    if (failures.nonEmpty) {
      failures.foreach { failure =>
        System.err.println(
          s"Failed partition ${failure.partitionId}: ${failure.failure}"
        )
      }
      throw IllegalStateException(
        s"${failures.length} sitemap download partitions failed"
      )
    }
  }

  private def listSitemapFiles(inputDir: Path): Vector[Path] =
    Using.resource(Files.walk(inputDir)) { paths =>
      paths
        .iterator()
        .asScala
        .filter(Files.isRegularFile(_))
        .filter(path => path.getFileName().toString.endsWith(".sitemaps.tsv"))
        .toVector
        .sortBy(_.toString)
    }

  private def deletePartitionOutputFiles(outputDir: Path): Unit =
    Using.resource(Files.list(outputDir)) { paths =>
      paths
        .iterator()
        .asScala
        .filter(Files.isRegularFile(_))
        .filter(path =>
          path
            .getFileName()
            .toString
            .matches("part-\\d{5}\\.sitemap-links\\.tsv")
        )
        .foreach(Files.deleteIfExists)
    }

  private def downloadPartitionSitemaps(
      partitionId: Int,
      inputFilePaths: Vector[String],
      outputDir: String
  ): LocalSitemapDownloadResult =
    try {
      val taskPartitionId = Option(TaskContext.get())
        .map(_.partitionId())
        .getOrElse(partitionId)
      val outputFile = Path
        .of(outputDir)
        .resolve(f"part-$taskPartitionId%05d.sitemap-links.tsv")

      downloadSitemapsFromFiles(taskPartitionId, inputFilePaths, outputFile)
    } catch {
      case exception: Exception =>
        LocalSitemapDownloadResult(
          partitionId,
          0,
          0,
          0,
          0,
          0,
          0,
          s"${exception.getClass.getSimpleName}: ${exception.getMessage}"
        )
    }

  private def downloadSitemapsFromFiles(
      partitionId: Int,
      inputFilePaths: Vector[String],
      outputFile: Path
  ): LocalSitemapDownloadResult = {
    var readRows = 0
    var malformedRows = 0
    var downloadedSitemaps = 0
    var invalidSitemaps = 0
    var failedDownloads = 0
    var savedLinks = 0
    var writer: BufferedWriter | Null = null

    Files.deleteIfExists(outputFile)

    def writeLink(
        row: SitemapDownloadInputRow,
        fetchedSitemapUrl: String,
        pageUrl: String
    ): Unit = {
      if (writer == null) {
        Files.createDirectories(outputFile.getParent())
        writer = Files.newBufferedWriter(outputFile, StandardCharsets.UTF_8)
      }

      writer.nn.write(
        Vector(
          row.sourceFile,
          row.robotsHost,
          row.robotsScheme,
          row.seedSitemapUrl,
          fetchedSitemapUrl,
          pageUrl
        ).map(tsvField).mkString("\t")
      )
      writer.nn.newLine()
      savedLinks += 1
    }

    try {
      inputFilePaths.foreach { inputFilePath =>
        Using.resource(Files.lines(Path.of(inputFilePath))) { lines =>
          lines.iterator().asScala.foreach { line =>
            parseSitemapInputRow(inputFilePath, line) match {
              case Some(row) =>
                readRows += 1
                val counters = processSeedSitemap(row, writeLink)
                downloadedSitemaps += counters.downloadedSitemaps
                invalidSitemaps += counters.invalidSitemaps
                failedDownloads += counters.failedDownloads

              case None =>
                malformedRows += 1
            }
          }
        }
      }
    } finally {
      if (writer != null) {
        writer.nn.close()
      }
    }

    LocalSitemapDownloadResult(
      partitionId,
      readRows,
      malformedRows,
      downloadedSitemaps,
      invalidSitemaps,
      failedDownloads,
      savedLinks,
      null
    )
  }

  private final case class SitemapCounters(
      downloadedSitemaps: Int,
      invalidSitemaps: Int,
      failedDownloads: Int
  )

  private def processSeedSitemap(
      row: SitemapDownloadInputRow,
      writeLink: (SitemapDownloadInputRow, String, String) => Unit
  ): SitemapCounters = {
    var downloadedSitemaps = 0
    var invalidSitemaps = 0
    var failedDownloads = 0
    var visited = Set.empty[String]
    var queue = Vector((row.seedSitemapUrl, 0))

    while (queue.nonEmpty) {
      val (sitemapUrl, depth) = queue.head
      queue = queue.tail

      if (!visited.contains(sitemapUrl) && depth <= MaxSitemapIndexDepth) {
        visited += sitemapUrl
        downloadAndParseSitemap(sitemapUrl) match {
          case Success(document) =>
            downloadedSitemaps += 1
            val baseUri = URI.create(sitemapUrl)

            document match {
              case SitemapXmlDocument.UrlSet(urls) =>
                urls.flatMap(SitemapXmlSchema.resolveLoc(baseUri, _)).foreach {
                  pageUrl =>
                    writeLink(row, sitemapUrl, pageUrl)
                }

              case SitemapXmlDocument.SitemapIndex(sitemaps) =>
                queue ++= sitemaps
                  .flatMap(SitemapXmlSchema.resolveLoc(baseUri, _))
                  .filterNot(visited.contains)
                  .map(childSitemapUrl => (childSitemapUrl, depth + 1))
            }

          case Failure(_: SitemapValidationException) =>
            invalidSitemaps += 1

          case Failure(_) =>
            failedDownloads += 1
        }
      }
    }

    SitemapCounters(downloadedSitemaps, invalidSitemaps, failedDownloads)
  }

  private final class SitemapValidationException(message: String)
      extends RuntimeException(message)

  private def downloadAndParseSitemap(
      sitemapUrl: String
  ): Try[SitemapXmlDocument] =
    Try {
      CommonCrawlRobotsArchiveSupport.withTempFile(
        "downloaded-sitemap-",
        ".xml"
      ) { tempFile =>
        CommonCrawlRobotsArchiveSupport.downloadToPath(
          URI.create(sitemapUrl),
          tempFile
        )

        Using.resource(decodedSitemapStream(tempFile)) { input =>
          SitemapXmlSchema.parse(input) match {
            case Right(document) =>
              document
            case Left(error) =>
              throw SitemapValidationException(error.message)
          }
        }
      }
    }

  private def decodedSitemapStream(path: Path): InputStream = {
    val input = BufferedInputStream(Files.newInputStream(path))
    input.mark(2)
    val first = input.read()
    val second = input.read()
    input.reset()

    if (first == 0x1f && second == 0x8b) {
      GZIPInputStream(input)
    } else {
      input
    }
  }

  private def parseSitemapInputRow(
      sourceFile: String,
      line: String
  ): Option[SitemapDownloadInputRow] =
    line.split("\t", -1).toVector match {
      case fields if fields.length >= 4 && fields(3).nonEmpty =>
        Some(
          SitemapDownloadInputRow(
            sourceFile,
            fields(1),
            fields(2),
            fields(3)
          )
        )
      case _ =>
        None
    }

  private def tsvField(value: String): String =
    value
      .replace('\t', ' ')
      .replace('\r', ' ')
      .replace('\n', ' ')
}
