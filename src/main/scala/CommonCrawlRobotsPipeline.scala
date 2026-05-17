import java.io.{IOException, InputStream}
import java.net.URI
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, StandardCopyOption}
import java.security.MessageDigest
import java.util.Locale
import java.util.zip.{GZIPInputStream, InflaterInputStream}

import org.apache.spark.sql.SparkSession
import org.netpreserve.jwarc.{WarcReader, WarcResponse}
import sttp.client3.*
import sttp.client3.httpclient.HttpClientSyncBackend

import scala.io.Source
import scala.jdk.CollectionConverters.*
import scala.util.Try
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
  private val CommonCrawlBaseUri = URI.create("https://data.commoncrawl.org/")
  private val DownloadMaxAttempts = 4
  private val DownloadInitialBackoffMillis = 500L
  private val UserAgent = "spark-scala3-commoncrawl-robots/0.1"

  def run(spark: SparkSession, config: JobConfig): Unit = {
    val manifestUrl = resolveManifestUrl(config.pathsUrl)
    val archivePaths = readArchivePaths(manifestUrl)
    val outputDir = Path.of(config.outputPath).toAbsolutePath.normalize()
    val outputDirString = outputDir.toString

    Files.createDirectories(outputDir)

    val results = spark.sparkContext
      .parallelize(archivePaths, archivePaths.size.max(1))
      .map(archivePath =>
        downloadAndExtractArchive(archivePath, outputDirString)
      )
      .collect()

    val savedFiles = results.map(_.savedFiles).sum
    val rejectedFiles = results.map(_.rejectedFiles).sum
    val failures = results.filter(_.failure != null)

    println(
      s"Read ${archivePaths.size} robotstxt archive paths from $manifestUrl"
    )
    println(s"Saved $savedFiles valid robots.txt captures into $outputDir")
    println(s"Rejected $rejectedFiles invalid robots.txt captures")

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

  def resolveManifestUrl(pathsUrl: String): URI = {
    val uri = URI.create(pathsUrl)
    val path = Option(uri.getPath()).getOrElse("")

    if (path.endsWith("/wat.paths.gz")) {
      URI.create(pathsUrl.stripSuffix("/wat.paths.gz") + "/robotstxt.paths.gz")
    } else {
      uri
    }
  }

  def readArchivePaths(manifestUrl: URI): Vector[String] =
    withTempFile("commoncrawl-robotstxt-paths-", ".gz") { manifestFile =>
      downloadToPath(manifestUrl, manifestFile)

      Using.Manager { use =>
        val responseBody = use(Files.newInputStream(manifestFile))
        val gzipped = use(GZIPInputStream(responseBody))
        val source =
          use(Source.fromInputStream(gzipped, StandardCharsets.UTF_8.name()))

        source
          .getLines()
          .map(_.trim)
          .filter(line => line.nonEmpty && !line.startsWith("#"))
          .filter(line =>
            line.contains("/robotstxt/") && line.endsWith(".warc.gz")
          )
          .toVector
      }.get
    }

  private def downloadAndExtractArchive(
      archivePath: String,
      outputDir: String
  ): ArchiveDownloadResult =
    try {
      val archiveUri = archiveUriFor(archivePath)
      withTempFile("commoncrawl-robotstxt-", ".warc.gz") { tempFile =>
        downloadToPath(archiveUri, tempFile)

        val extractionResult =
          extractRobots(tempFile, Path.of(outputDir), archivePath)

        println(
          s"Archive $archivePath: saved ${extractionResult.savedFiles} valid robots.txt files; rejected ${extractionResult.rejectedFiles} invalid robots.txt files"
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
          if (isRobotsTarget(response.target())) {
            val targetUri = response.targetURI()
            val fileName =
              robotsFileName(targetUri, response.date().toString, archivePath)
            val destination = outputDir.resolve(fileName)

            val httpResponse = response.http()

            val content = Using.resource(
              decodedBodyStream(
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

  private def archiveUriFor(archivePath: String): URI = {
    val uri = URI.create(archivePath)

    if (uri.isAbsolute) {
      uri
    } else {
      CommonCrawlBaseUri.resolve(archivePath)
    }
  }

  private def isRobotsTarget(target: String): Boolean =
    Try {
      URI
        .create(target)
        .getPath()
        .toLowerCase(Locale.ROOT)
        .endsWith("/robots.txt")
    }.getOrElse(false)

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

    s"${safePathPart(host)}/${safePathPart(scheme)}-${safePathPart(captureDate)}-${sha256Hex(digestInput).take(16)}.txt"
  }

  private def safePathPart(value: String): String =
    value.replaceAll("[^A-Za-z0-9._-]", "_")

  private def decodedBodyStream(
      body: InputStream,
      contentEncoding: String
  ): InputStream =
    contentEncoding
      .split(",")
      .map(_.trim.toLowerCase(Locale.ROOT))
      .filter(_.nonEmpty)
      .reverse
      .foldLeft(body) {
        case (stream, "gzip" | "x-gzip") => GZIPInputStream(stream)
        case (stream, "deflate")         => InflaterInputStream(stream)
        case (stream, _)                 => stream
      }

  private def sha256Hex(value: String): String = {
    val digest = MessageDigest.getInstance("SHA-256")
    digest
      .digest(value.getBytes(StandardCharsets.UTF_8))
      .map(byte => f"${byte & 0xff}%02x")
      .mkString
  }

  private def downloadToPath(uri: URI, destination: Path): Path =
    Option(uri.getScheme()).map(_.toLowerCase(Locale.ROOT)) match {
      case Some("file") =>
        Files.copy(
          Path.of(uri),
          destination,
          StandardCopyOption.REPLACE_EXISTING
        )
        destination
      case Some("http" | "https") =>
        downloadHttpToPathWithRetries(uri, destination)
      case Some(scheme) =>
        throw IOException(s"Unsupported URI scheme: $scheme")
      case None =>
        Files.copy(
          Path.of(uri.toString),
          destination,
          StandardCopyOption.REPLACE_EXISTING
        )
        destination
    }

  private def downloadHttpToPathWithRetries(
      uri: URI,
      destination: Path
  ): Path = {
    var attempt = 1
    var backoffMillis = DownloadInitialBackoffMillis
    var lastFailure: Throwable | Null = null

    while (attempt <= DownloadMaxAttempts) {
      try {
        return downloadHttpToPath(uri, destination)
      } catch {
        case exception: Exception =>
          lastFailure = exception
          Files.deleteIfExists(destination)

          if (attempt == DownloadMaxAttempts) {
            throw IOException(
              s"GET $uri failed after $DownloadMaxAttempts attempts",
              exception
            )
          }

          Thread.sleep(backoffMillis)
          backoffMillis *= 2
          attempt += 1
      }
    }

    throw IOException(
      s"GET $uri failed after $DownloadMaxAttempts attempts",
      lastFailure
    )
  }

  private def downloadHttpToPath(uri: URI, destination: Path): Path = {
    val backend = HttpClientSyncBackend()

    try {
      basicRequest
        .get(sttp.model.Uri.unsafeParse(uri.toString))
        .header("User-Agent", UserAgent)
        .response(asPath(destination).getRight)
        .send(backend)
        .body
    } finally {
      backend.close()
    }
  }

  private def withTempFile[A](
      prefix: String,
      suffix: String
  )(use: Path => A): A = {
    val tempFile = Files.createTempFile(prefix, suffix)

    try {
      use(tempFile)
    } finally {
      Files.deleteIfExists(tempFile)
    }
  }
}
