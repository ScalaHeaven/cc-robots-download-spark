import java.io.{IOException, InputStream}
import java.net.URI
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, StandardCopyOption}
import java.security.MessageDigest
import java.util.Locale
import java.util.zip.{GZIPInputStream, InflaterInputStream}

import sttp.client3.*
import sttp.client3.httpclient.HttpClientSyncBackend

import scala.concurrent.duration.*
import scala.io.Source
import scala.annotation.tailrec
import scala.util.Try
import scala.util.Using

object CommonCrawlRobotsArchiveSupport {
  private val CommonCrawlBaseUri = URI.create("https://data.commoncrawl.org/")
  private val DownloadMaxAttempts = 4
  private val DownloadInitialBackoffMillis = 500L
  private val UserAgent = "spark-scala3-commoncrawl-robots/0.1"

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

  def archiveUriFor(archivePath: String): URI = {
    val uri = URI.create(archivePath)

    if (uri.isAbsolute) {
      uri
    } else {
      CommonCrawlBaseUri.resolve(archivePath)
    }
  }

  def isRobotsTarget(target: String): Boolean =
    Try {
      URI
        .create(target)
        .getPath()
        .toLowerCase(Locale.ROOT)
        .endsWith("/robots.txt")
    }.getOrElse(false)

  def decodedBodyStream(
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

  def downloadToPath(
      uri: URI,
      destination: Path,
      timeouts: DownloadTimeoutConfig = Cli.DefaultDownloadTimeouts
  ): Path =
    Option(uri.getScheme()).map(_.toLowerCase(Locale.ROOT)) match {
      case Some("file") =>
        Files.copy(
          Path.of(uri),
          destination,
          StandardCopyOption.REPLACE_EXISTING
        )
        destination
      case Some("http" | "https") =>
        downloadHttpToPathWithRetries(uri, destination, timeouts)
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

  def withTempFile[A](
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

  def safePathPart(value: String): String =
    value.replaceAll("[^A-Za-z0-9._-]", "_")

  def sha256Hex(value: String): String = {
    val digest = MessageDigest.getInstance("SHA-256")
    digest
      .digest(value.getBytes(StandardCharsets.UTF_8))
      .map(byte => f"${byte & 0xff}%02x")
      .mkString
  }

  private def downloadHttpToPathWithRetries(
      uri: URI,
      destination: Path,
      timeouts: DownloadTimeoutConfig
  ): Path =
    @tailrec
    def attempt(attemptNumber: Int, backoffMillis: Long): Path =
      try {
        downloadHttpToPath(uri, destination, timeouts)
      } catch {
        case exception: Exception =>
          Files.deleteIfExists(destination)

          if (attemptNumber == DownloadMaxAttempts) {
            throw IOException(
              s"GET $uri failed after $DownloadMaxAttempts attempts",
              exception
            )
          }

          Thread.sleep(backoffMillis)
          attempt(attemptNumber + 1, backoffMillis * 2)
      }

    attempt(attemptNumber = 1, DownloadInitialBackoffMillis)

  private def downloadHttpToPath(
      uri: URI,
      destination: Path,
      timeouts: DownloadTimeoutConfig
  ): Path = {
    val backend = HttpClientSyncBackend(
      options = SttpBackendOptions.connectionTimeout(
        timeouts.connectTimeoutSeconds.seconds
      )
    )

    try {
      basicRequest
        .get(sttp.model.Uri.unsafeParse(uri.toString))
        .header("User-Agent", UserAgent)
        .readTimeout(timeouts.readTimeoutSeconds.seconds)
        .response(asPath(destination).getRight)
        .send(backend)
        .body
    } finally {
      backend.close()
    }
  }
}
