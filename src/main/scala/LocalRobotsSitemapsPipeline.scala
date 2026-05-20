import java.io.BufferedWriter
import java.nio.charset.StandardCharsets
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{FileVisitResult, Files, Path, SimpleFileVisitor}
import java.util.concurrent.{Callable, Executors, LinkedBlockingQueue, TimeUnit}
import java.util.Locale

import scala.jdk.CollectionConverters.*
import scala.util.Try
import scala.util.Using

final case class LocalSitemapPartitionResult(
    partitionId: Int,
    parsedFiles: Long,
    rejectedFiles: Long,
    savedSitemapLinks: Long,
    failure: String | Null
)

object LocalRobotsSitemapsPipeline {
  private val DefaultQueueSize = 50000
  private val DefaultProgressInterval = 100000

  def run(config: JobConfig): Unit = {
    val robotsDir = Path.of(config.pathsUrl).toAbsolutePath.normalize()
    val outputDir = Path.of(config.outputPath).toAbsolutePath.normalize()
    val workerCount = configuredWorkerCount(config.master)
    val queueSize = configuredQueueSize()
    val progressInterval = configuredProgressInterval()

    if (!Files.isDirectory(robotsDir)) {
      throw IllegalArgumentException(
        s"Local robots.txt input path is not a directory: $robotsDir"
      )
    }

    Files.createDirectories(outputDir)
    deletePartitionOutputFiles(outputDir)

    val queue = LinkedBlockingQueue[Option[Path]](queueSize)
    val executor = Executors.newFixedThreadPool(workerCount)
    val workerResults = (0 until workerCount).map { workerId =>
      executor.submit(new Callable[LocalSitemapPartitionResult] {
        override def call(): LocalSitemapPartitionResult =
          extractQueuedSitemaps(
            workerId,
            queue,
            outputDir.resolve(f"part-$workerId%05d.sitemaps.tsv")
          )
      })
    }

    println(
      s"Streaming local robots.txt tree $robotsDir with $workerCount workers and queue size $queueSize"
    )

    var visitedDirectories = 0L
    var visitedFiles = 0L
    var matchedRobotsFiles = 0L

    def printWalkProgress(): Unit =
      println(
        s"Walked $visitedDirectories directories and $visitedFiles files; queued $matchedRobotsFiles robots.txt files"
      )

    try {
      Files.walkFileTree(
        robotsDir,
        new SimpleFileVisitor[Path] {
          override def preVisitDirectory(
              dir: Path,
              attrs: BasicFileAttributes
          ): FileVisitResult = {
            visitedDirectories += 1

            if (visitedDirectories % progressInterval == 0) {
              printWalkProgress()
            }

            FileVisitResult.CONTINUE
          }

          override def visitFile(
              file: Path,
              attrs: BasicFileAttributes
          ): FileVisitResult = {
            visitedFiles += 1

            if (attrs.isRegularFile && file.getFileName().toString.endsWith(
                ".txt"
              )) {
              matchedRobotsFiles += 1
              queue.put(Some(file))

              if (matchedRobotsFiles % progressInterval == 0) {
                printWalkProgress()
              }
            } else if (visitedFiles % progressInterval == 0) {
              printWalkProgress()
            }

            FileVisitResult.CONTINUE
          }
        }
      )
    } finally {
      (0 until workerCount).foreach(_ => queue.put(None))
      executor.shutdown()
      executor.awaitTermination(Long.MaxValue, TimeUnit.NANOSECONDS)
    }

    val results = workerResults.map(_.get())
    val parsedFiles = results.map(_.parsedFiles).sum
    val rejectedFiles = results.map(_.rejectedFiles).sum
    val savedSitemapLinks = results.map(_.savedSitemapLinks).sum
    val failures = results.filter(_.failure != null)

    println(
      s"Read $matchedRobotsFiles local robots.txt files from $robotsDir"
    )
    println(s"Parsed $parsedFiles usable robots.txt files")
    println(
      s"Rejected $rejectedFiles robots.txt files without usable robots directives"
    )
    println(s"Saved $savedSitemapLinks sitemap links into $outputDir")

    if (failures.nonEmpty) {
      failures.foreach { failure =>
        System.err.println(
          s"Failed worker ${failure.partitionId}: ${failure.failure}"
        )
      }
      throw IllegalStateException(
        s"${failures.length} local robots.txt workers failed to parse"
      )
    }
  }

  private def configuredQueueSize(): Int =
    sys.props
      .get("localSitemaps.queueSize")
      .orElse(sys.props.get("localSitemaps.batchSize"))
      .flatMap(value => Try(value.toInt).toOption)
      .filter(_ > 0)
      .getOrElse(DefaultQueueSize)

  private def configuredProgressInterval(): Int =
    sys.props
      .get("localSitemaps.progressInterval")
      .flatMap(value => Try(value.toInt).toOption)
      .filter(_ > 0)
      .getOrElse(DefaultProgressInterval)

  private def configuredWorkerCount(master: String): Int =
    sys.props
      .get("localSitemaps.workers")
      .flatMap(value => Try(value.toInt).toOption)
      .filter(_ > 0)
      .getOrElse(masterWorkerCount(master))

  private def masterWorkerCount(master: String): Int = {
    val localFixed = "^local\\[(\\d+)\\]$".r

    master.trim.toLowerCase(Locale.ROOT) match {
      case "local" | "local[*]" =>
        Runtime.getRuntime.availableProcessors().max(1)
      case localFixed(workerCount) =>
        Option(workerCount).flatMap(_.toIntOption).filter(_ > 0).getOrElse(1)
      case _ =>
        Runtime.getRuntime.availableProcessors().max(1)
    }
  }

  private def deletePartitionOutputFiles(outputDir: Path): Unit =
    Using.resource(Files.list(outputDir)) { paths =>
      paths
        .iterator()
        .asScala
        .filter(Files.isRegularFile(_))
        .filter(path =>
          path.getFileName().toString.matches(
            "part-\\d{5}(?:-\\d{5})?\\.sitemaps\\.tsv"
          )
        )
        .foreach(Files.deleteIfExists)
    }

  private def extractQueuedSitemaps(
      partitionId: Int,
      queue: LinkedBlockingQueue[Option[Path]],
      outputFile: Path
  ): LocalSitemapPartitionResult =
    try {
      extractSitemapsFromQueue(partitionId, queue, outputFile)
    } catch {
      case exception: Exception =>
        LocalSitemapPartitionResult(
          partitionId,
          0,
          0,
          0,
          s"${exception.getClass.getSimpleName}: ${exception.getMessage}"
        )
    }

  private def extractSitemapsFromQueue(
      partitionId: Int,
      queue: LinkedBlockingQueue[Option[Path]],
      outputFile: Path
  ): LocalSitemapPartitionResult = {
    var parsedFiles = 0L
    var rejectedFiles = 0L
    var savedSitemapLinks = 0L
    var writer = Option.empty[BufferedWriter]

    Files.deleteIfExists(outputFile)

    def writeSitemapLink(
        robotFile: Path,
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
          robotFile.toString,
          robotsHost(robotFile),
          robotsScheme(robotFile),
          sitemapUrl
        ).map(tsvField).mkString("\t")
      )
      activeWriter.newLine()
      savedSitemapLinks += 1
    }

    try {
      var keepRunning = true
      while (keepRunning) {
        queue.take() match {
          case Some(robotFile) =>
            Try(Files.readAllBytes(robotFile)).map(RobotsTxtParser.parse) match {
              case scala.util.Success(robotsTxt) if robotsTxt.isValid =>
                parsedFiles += 1
                robotsTxt.sitemaps.distinct.foreach { sitemapUrl =>
                  writeSitemapLink(robotFile, sitemapUrl)
                }

              case _ =>
                rejectedFiles += 1
            }

          case None =>
            keepRunning = false
        }
      }
    } finally {
      writer.foreach(_.close())
    }

    LocalSitemapPartitionResult(
      partitionId,
      parsedFiles,
      rejectedFiles,
      savedSitemapLinks,
      null
    )
  }

  private def robotsHost(robotFile: Path): String =
    Option(robotFile.getParent())
      .flatMap(parent => Option(parent.getFileName()))
      .map(_.toString)
      .getOrElse("unknown-host")

  private def robotsScheme(robotFile: Path): String =
    robotFile.getFileName().toString.split("-", 2).headOption match {
      case Some(scheme) if scheme.nonEmpty =>
        scheme.toLowerCase(Locale.ROOT)
      case _ =>
        "unknown"
    }

  private def tsvField(value: String): String =
    value
      .replace('\t', ' ')
      .replace('\r', ' ')
      .replace('\n', ' ')
}
