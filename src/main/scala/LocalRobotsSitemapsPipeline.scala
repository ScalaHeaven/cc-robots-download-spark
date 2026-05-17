import java.io.BufferedWriter
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.util.Locale

import org.apache.spark.TaskContext
import org.apache.spark.sql.SparkSession

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
  def run(spark: SparkSession, config: JobConfig): Unit = {
    val robotsDir = Path.of(config.pathsUrl).toAbsolutePath.normalize()
    val outputDir = Path.of(config.outputPath).toAbsolutePath.normalize()
    val outputDirString = outputDir.toString

    if (!Files.isDirectory(robotsDir)) {
      throw IllegalArgumentException(
        s"Local robots.txt input path is not a directory: $robotsDir"
      )
    }

    Files.createDirectories(outputDir)
    deletePartitionOutputFiles(outputDir)

    val robotsFiles = listRobotsFiles(robotsDir)
    val partitionCount =
      robotsFiles.size.min(spark.sparkContext.defaultParallelism * 4).max(1)
    val results = spark.sparkContext
      .parallelize(robotsFiles.map(_.toString), partitionCount)
      .mapPartitionsWithIndex { case (partitionId, files) =>
        Iterator(
          extractPartitionSitemaps(
            partitionId,
            files.toVector,
            outputDirString
          )
        )
      }
      .collect()

    val parsedFiles = results.map(_.parsedFiles).sum
    val rejectedFiles = results.map(_.rejectedFiles).sum
    val savedSitemapLinks = results.map(_.savedSitemapLinks).sum
    val failures = results.filter(_.failure != null)

    println(s"Read ${robotsFiles.size} local robots.txt files from $robotsDir")
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

  private def listRobotsFiles(robotsDir: Path): Vector[Path] =
    Using.resource(Files.walk(robotsDir)) { paths =>
      paths
        .iterator()
        .asScala
        .filter(Files.isRegularFile(_))
        .filter(path => path.getFileName().toString.endsWith(".txt"))
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
          path.getFileName().toString.matches("part-\\d{5}\\.sitemaps\\.tsv")
        )
        .foreach(Files.deleteIfExists)
    }

  private def extractPartitionSitemaps(
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
        .resolve(f"part-$taskPartitionId%05d.sitemaps.tsv")

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
    var writer: BufferedWriter | Null = null

    Files.deleteIfExists(outputFile)

    def writeSitemapLink(
        robotFile: Path,
        sitemapUrl: String
    ): Unit = {
      if (writer == null) {
        Files.createDirectories(outputFile.getParent())
        writer = Files.newBufferedWriter(outputFile, StandardCharsets.UTF_8)
      }

      writer.nn.write(
        Vector(
          robotFile.toString,
          robotsHost(robotFile),
          robotsScheme(robotFile),
          sitemapUrl
        ).map(tsvField).mkString("\t")
      )
      writer.nn.newLine()
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
      if (writer != null) {
        writer.nn.close()
      }
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
