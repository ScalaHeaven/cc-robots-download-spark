import java.io.BufferedWriter
import java.nio.charset.StandardCharsets
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{FileVisitResult, Files, Path, SimpleFileVisitor}
import java.util.Locale

import org.apache.spark.TaskContext
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.jdk.CollectionConverters.*
import scala.util.Try
import scala.util.Using

final case class LocalSitemapPartitionResult(
    partitionId: Int,
    parsedFiles: Int,
    rejectedFiles: Int,
    savedSitemapLinks: Int,
    failure: String | Null
)

object LocalRobotsSitemapsPipeline {
  private val DefaultFileBatchSize = 10000
  private val DefaultProgressInterval = 100000

  def run(spark: SparkSession, config: JobConfig): Unit = {
    val robotsDir = Path.of(config.pathsUrl).toAbsolutePath.normalize()
    val outputDir = Path.of(config.outputPath).toAbsolutePath.normalize()
    val outputDirString = outputDir.toString
    val fileBatchSize = configuredFileBatchSize()
    val progressInterval = configuredProgressInterval()

    if (!Files.isDirectory(robotsDir)) {
      throw IllegalArgumentException(
        s"Local robots.txt input path is not a directory: $robotsDir"
      )
    }

    Files.createDirectories(outputDir)
    deletePartitionOutputFiles(outputDir)

    var batchId = 0
    var totalFiles = 0L
    var parsedFiles = 0L
    var rejectedFiles = 0L
    var savedSitemapLinks = 0L
    var failures = Vector.empty[LocalSitemapPartitionResult]
    val currentBatch = mutable.ArrayBuffer.empty[String]

    println(
      s"Walking local robots.txt tree $robotsDir in batches of $fileBatchSize files"
    )

    def processCurrentBatch(): Unit =
      if (currentBatch.nonEmpty) {
        val batchFiles = currentBatch.toVector
        currentBatch.clear()
        totalFiles += batchFiles.size

        val partitionCount =
          batchFiles.size.min(spark.sparkContext.defaultParallelism * 4).max(1)

        println(
          s"Processing local robots.txt batch $batchId with ${batchFiles.size} files"
        )

        val results = spark.sparkContext
          .parallelize(batchFiles, partitionCount)
          .mapPartitionsWithIndex { case (partitionId, files) =>
            Iterator(
              extractPartitionSitemaps(
                batchId,
                partitionId,
                files.toVector,
                outputDirString
              )
            )
          }
          .collect()

        parsedFiles += results.map(_.parsedFiles.toLong).sum
        rejectedFiles += results.map(_.rejectedFiles.toLong).sum
        savedSitemapLinks += results.map(_.savedSitemapLinks.toLong).sum
        failures ++= results.filter(_.failure != null)
        batchId += 1
      }

    var visitedDirectories = 0L
    var visitedFiles = 0L
    var matchedRobotsFiles = 0L

    def printWalkProgress(): Unit =
      println(
        s"Walked $visitedDirectories directories and $visitedFiles files; found $matchedRobotsFiles robots.txt files"
      )

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
            currentBatch += file.toString

            if (currentBatch.size >= fileBatchSize) {
              processCurrentBatch()
            }
          } else if (visitedFiles % progressInterval == 0) {
            printWalkProgress()
          }

          FileVisitResult.CONTINUE
        }
      }
    )
    processCurrentBatch()

    println(s"Read $totalFiles local robots.txt files from $robotsDir")
    println(s"Parsed $parsedFiles usable robots.txt files")
    println(
      s"Rejected $rejectedFiles robots.txt files without usable robots directives"
    )
    println(s"Saved $savedSitemapLinks sitemap links into $outputDir")

    if (failures.nonEmpty) {
      failures.foreach { failure =>
        System.err.println(
          s"Failed partition ${failure.partitionId}: ${failure.failure}"
        )
      }
      throw IllegalStateException(
        s"${failures.length} local robots.txt partitions failed to parse"
      )
    }
  }

  private def configuredFileBatchSize(): Int =
    sys.props
      .get("localSitemaps.batchSize")
      .flatMap(value => Try(value.toInt).toOption)
      .filter(_ > 0)
      .getOrElse(DefaultFileBatchSize)

  private def configuredProgressInterval(): Int =
    sys.props
      .get("localSitemaps.progressInterval")
      .flatMap(value => Try(value.toInt).toOption)
      .filter(_ > 0)
      .getOrElse(DefaultProgressInterval)

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

  private def extractPartitionSitemaps(
      batchId: Int,
      partitionId: Int,
      robotFilePaths: Vector[String],
      outputDir: String
  ): LocalSitemapPartitionResult =
    try {
      val taskPartitionId = Option(TaskContext.get())
        .map(_.partitionId())
        .getOrElse(partitionId)
      val outputFile = Path
        .of(outputDir)
        .resolve(f"part-$batchId%05d-$taskPartitionId%05d.sitemaps.tsv")

      extractSitemapsFromFiles(taskPartitionId, robotFilePaths, outputFile)
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

  private def extractSitemapsFromFiles(
      partitionId: Int,
      robotFilePaths: Vector[String],
      outputFile: Path
  ): LocalSitemapPartitionResult = {
    var parsedFiles = 0
    var rejectedFiles = 0
    var savedSitemapLinks = 0
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
      robotFilePaths.foreach { robotFilePath =>
        val robotFile = Path.of(robotFilePath)

        Try(Files.readAllBytes(robotFile)).map(RobotsTxtParser.parse) match {
          case scala.util.Success(robotsTxt) if robotsTxt.isValid =>
            parsedFiles += 1
            robotsTxt.sitemaps.distinct.foreach { sitemapUrl =>
              writeSitemapLink(robotFile, sitemapUrl)
            }

          case _ =>
            rejectedFiles += 1
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
