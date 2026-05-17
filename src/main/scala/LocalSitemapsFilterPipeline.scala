import java.io.BufferedWriter
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

import org.apache.spark.TaskContext
import org.apache.spark.sql.SparkSession

import scala.jdk.CollectionConverters.*
import scala.util.Using

final case class LocalSitemapFilterResult(
    partitionId: Int,
    readRows: Int,
    malformedRows: Int,
    keptRows: Int,
    failure: String | Null
)

final case class LocalSitemapRow(
    robotsFilePath: String,
    robotsHost: String,
    robotsScheme: String,
    sitemapUrl: String
) {
  def fields: Vector[String] =
    Vector(robotsFilePath, robotsHost, robotsScheme, sitemapUrl)
}

final case class ClassifiedLocalSitemapRow(
    row: LocalSitemapRow,
    classification: SitemapClassification
) {
  def fields: Vector[String] =
    row.fields ++ Vector(
      classification.countryKey,
      classification.countryName,
      classification.matchedSuffix,
      classification.languageRegion
    )
}

object LocalSitemapsFilterPipeline {
  def run(spark: SparkSession, config: JobConfig): Unit = {
    val inputDir = Path.of(config.pathsUrl).toAbsolutePath.normalize()
    val outputDir = Path.of(config.outputPath).toAbsolutePath.normalize()
    val outputDirString = outputDir.toString
    val suffixes = SitemapCountryLocaleClassifier.loadDefaultCountrySuffixes()

    if (!Files.isDirectory(inputDir)) {
      throw IllegalArgumentException(
        s"Local sitemap TSV input path is not a directory: $inputDir"
      )
    }

    Files.createDirectories(outputDir)
    deleteFilterOutputFiles(outputDir)

    val inputFiles = listSitemapFiles(inputDir)
    val partitionCount =
      inputFiles.size.min(spark.sparkContext.defaultParallelism * 4).max(1)
    val results = spark.sparkContext
      .parallelize(inputFiles.map(_.toString), partitionCount)
      .mapPartitionsWithIndex { case (partitionId, files) =>
        Iterator(
          filterPartitionSitemaps(
            partitionId,
            files.toVector,
            outputDirString,
            suffixes
          )
        )
      }
      .collect()

    val readRows = results.map(_.readRows).sum
    val malformedRows = results.map(_.malformedRows).sum
    val keptRows = results.map(_.keptRows).sum
    val failures = results.filter(_.failure != null)

    println(s"Read ${inputFiles.size} local sitemap TSV files from $inputDir")
    println(s"Read $readRows sitemap rows")
    println(s"Skipped $malformedRows malformed sitemap rows")
    println(s"Saved $keptRows filtered sitemap rows into $outputDir")

    if (failures.nonEmpty) {
      failures.foreach { failure =>
        System.err.println(
          s"Failed partition ${failure.partitionId}: ${failure.failure}"
        )
      }
      throw IllegalStateException(
        s"${failures.length} local sitemap TSV partitions failed to filter"
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

  private def deleteFilterOutputFiles(outputDir: Path): Unit = {
    val outputGroups = Vector("country", "language-region")

    outputGroups.foreach { group =>
      val groupDir = outputDir.resolve(group)
      if (Files.isDirectory(groupDir)) {
        Using.resource(Files.walk(groupDir)) { paths =>
          paths
            .iterator()
            .asScala
            .filter(Files.isRegularFile(_))
            .filter(path =>
              path
                .getFileName()
                .toString
                .matches("part-\\d{5}\\.sitemaps\\.tsv")
            )
            .foreach(Files.deleteIfExists)
        }
      }
    }
  }

  private def filterPartitionSitemaps(
      partitionId: Int,
      inputFilePaths: Vector[String],
      outputDir: String,
      suffixes: Vector[CountrySuffix]
  ): LocalSitemapFilterResult =
    try {
      val taskPartitionId = Option(TaskContext.get())
        .map(_.partitionId())
        .getOrElse(partitionId)

      filterSitemapsFromFiles(
        taskPartitionId,
        inputFilePaths,
        Path.of(outputDir),
        suffixes
      )
    } catch {
      case exception: Exception =>
        LocalSitemapFilterResult(
          partitionId,
          0,
          0,
          0,
          s"${exception.getClass.getSimpleName}: ${exception.getMessage}"
        )
    }

  private def filterSitemapsFromFiles(
      partitionId: Int,
      inputFilePaths: Vector[String],
      outputDir: Path,
      suffixes: Vector[CountrySuffix]
  ): LocalSitemapFilterResult = {
    var readRows = 0
    var malformedRows = 0
    var keptRows = 0
    var writers = Map.empty[Path, BufferedWriter]

    def writerFor(outputFile: Path): BufferedWriter = {
      writers.get(outputFile) match {
        case Some(writer) =>
          writer
        case None =>
          Files.createDirectories(outputFile.getParent())
          val writer =
            Files.newBufferedWriter(outputFile, StandardCharsets.UTF_8)
          writers += outputFile -> writer
          writer
      }
    }

    def writeRow(
        group: String,
        key: String,
        row: ClassifiedLocalSitemapRow
    ): Unit = {
      val outputFile = outputDir
        .resolve(group)
        .resolve(key)
        .resolve(f"part-$partitionId%05d.sitemaps.tsv")
      val writer = writerFor(outputFile)

      writer.write(row.fields.map(tsvField).mkString("\t"))
      writer.newLine()
    }

    try {
      inputFilePaths.foreach { inputFilePath =>
        Using.resource(Files.lines(Path.of(inputFilePath))) { lines =>
          lines.iterator().asScala.foreach { line =>
            parseLocalSitemapRow(line) match {
              case Some(row) =>
                readRows += 1
                SitemapCountryLocaleClassifier
                  .classify(row.robotsHost, row.sitemapUrl, suffixes)
                  .foreach { classification =>
                    val classifiedRow =
                      ClassifiedLocalSitemapRow(row, classification)

                    writeRow(
                      "country",
                      classification.countryKey,
                      classifiedRow
                    )
                    writeRow(
                      "language-region",
                      classification.languageRegion,
                      classifiedRow
                    )
                    keptRows += 1
                  }

              case None =>
                malformedRows += 1
            }
          }
        }
      }
    } finally {
      writers.values.foreach(_.close())
    }

    LocalSitemapFilterResult(
      partitionId,
      readRows,
      malformedRows,
      keptRows,
      null
    )
  }

  private def parseLocalSitemapRow(line: String): Option[LocalSitemapRow] =
    line.split("\t", -1).toVector match {
      case Vector(robotsFilePath, robotsHost, robotsScheme, sitemapUrl)
          if robotsHost.nonEmpty && sitemapUrl.nonEmpty =>
        Some(
          LocalSitemapRow(
            robotsFilePath,
            robotsHost,
            robotsScheme,
            sitemapUrl
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
