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
  private val DefaultDownloadMaxAttempts = 4
  private val CommonCrawlDownloadMaxAttempts = 1000
  private val DownloadRetryDelayMillis = 1000L
  private val DownloadMaxBackoffMillis = 30_000L
  private val CommonCrawlDownloadHosts =
    Set(
      "data.commoncrawl.org",
      "ds5q9oxwqwsfj.cloudfront.net",
      "commoncrawl.s3.amazonaws.com"
    )
  private val TransientHttpStatusCodes =
    Set(408, 425, 429, 500, 502, 503, 504)
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

  def readArchivePaths(
      manifestUrl: URI,
      policy: DownloadPolicyConfig = Cli.DefaultDownloadPolicy
  ): Vector[String] =
    withTempFile("commoncrawl-robotstxt-paths-", ".gz") { manifestFile =>
      downloadToPath(manifestUrl, manifestFile, policy)

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
      policy: DownloadPolicyConfig = Cli.DefaultDownloadPolicy
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
        downloadHttpToPathWithRetries(uri, destination, policy)
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

  def downloadPolicySummary(policy: DownloadPolicyConfig): String =
    "Used HTTP download policy: " +
      s"connect=${policy.connectTimeoutSeconds}s, " +
      s"read=${policy.readTimeoutSeconds}s, " +
      s"delay=${policy.delaySeconds}s"

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
      policy: DownloadPolicyConfig
  ): Path = {
    val maxAttempts = downloadMaxAttemptsFor(uri)

    @tailrec
    def attempt(attemptNumber: Int, backoffMillis: Long): Path =
      try {
        downloadHttpToPath(uri, destination, policy)
      } catch {
        case exception: HttpDownloadStatusException if !exception.retryable =>
          Files.deleteIfExists(destination)
          throw exception

        case exception: Exception =>
          Files.deleteIfExists(destination)

          if (attemptNumber == maxAttempts) {
            throw IOException(
              s"GET $uri failed after $maxAttempts attempts",
              exception
            )
          }

          Thread.sleep(backoffMillis)
          attempt(
            attemptNumber + 1,
            nextBackoffMillis(uri, backoffMillis)
          )
      }

    attempt(attemptNumber = 1, DownloadRetryDelayMillis)
  }

  private def downloadHttpToPath(
      uri: URI,
      destination: Path,
      policy: DownloadPolicyConfig
  ): Path = {
    val backend = HttpClientSyncBackend(
      options = SttpBackendOptions.connectionTimeout(
        policy.connectTimeoutSeconds.seconds
      )
    )

    try {
      HttpDownloadDelay.waitBeforeRequest(policy)

      val response = basicRequest
        .get(sttp.model.Uri.unsafeParse(uri.toString))
        .header("User-Agent", UserAgent)
        .readTimeout(policy.readTimeoutSeconds.seconds)
        .response(asPath(destination))
        .send(backend)

      response.body match {
        case Right(downloadedPath) =>
          downloadedPath
        case Left(errorBody) =>
          Files.deleteIfExists(destination)
          throw HttpDownloadStatusException(
            uri,
            response.code.code,
            response.statusText,
            errorBody,
            retryableStatusCode(uri, response.code.code)
          )
      }
    } finally {
      backend.close()
    }
  }

  private def downloadMaxAttemptsFor(uri: URI): Int =
    if (isCommonCrawlDownloadUri(uri)) {
      CommonCrawlDownloadMaxAttempts
    } else {
      DefaultDownloadMaxAttempts
    }

  private def nextBackoffMillis(uri: URI, currentBackoffMillis: Long): Long =
    if (isCommonCrawlDownloadUri(uri)) {
      DownloadRetryDelayMillis
    } else {
      (currentBackoffMillis * 2).min(DownloadMaxBackoffMillis)
    }

  private def retryableStatusCode(uri: URI, statusCode: Int): Boolean =
    TransientHttpStatusCodes.contains(statusCode) ||
      (statusCode == 403 && isCommonCrawlDownloadUri(uri))

  private def isCommonCrawlDownloadUri(uri: URI): Boolean =
    Option(uri.getHost())
      .map(_.toLowerCase(Locale.ROOT))
      .exists(CommonCrawlDownloadHosts.contains)

  private final case class HttpDownloadStatusException(
      uri: URI,
      statusCode: Int,
      statusText: String,
      body: String,
      retryable: Boolean
  ) extends IOException(
        httpStatusMessage(uri, statusCode, statusText, body, retryable)
      )

  private def httpStatusMessage(
      uri: URI,
      statusCode: Int,
      statusText: String,
      body: String,
      retryable: Boolean
  ): String = {
    val details = body.linesIterator.take(3).mkString(" ").take(240)
    val retryText =
      if (retryable) "retryable HTTP response"
      else "non-retryable HTTP response"
    val detailText = if (details.nonEmpty) s": $details" else ""

    s"GET $uri returned $statusCode $statusText ($retryText)$detailText"
  }
}

private object HttpDownloadDelay {
  private var nextRequestStartMillis = 0L

  def waitBeforeRequest(policy: DownloadPolicyConfig): Unit = {
    val delayMillis = policy.delaySeconds.toLong * 1000L

    if (delayMillis > 0) {
      val waitMillis = synchronized {
        val now = System.currentTimeMillis()
        val scheduledStart = nextRequestStartMillis.max(now)
        nextRequestStartMillis = scheduledStart + delayMillis

        scheduledStart - now
      }

      if (waitMillis > 0) {
        Thread.sleep(waitMillis)
      }
    }
  }
}
