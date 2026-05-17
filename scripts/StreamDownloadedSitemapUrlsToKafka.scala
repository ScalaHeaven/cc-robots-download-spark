//> using scala "3.8.3"
//> using dep "org.apache.kafka:kafka-clients:4.2.0"

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.util.Properties
import java.util.concurrent.Future

import org.apache.kafka.clients.producer.{
  KafkaProducer,
  ProducerRecord,
  RecordMetadata
}
import org.apache.kafka.common.serialization.StringSerializer

import scala.jdk.CollectionConverters.*
import scala.util.Using

final case class StreamConfig(
    inputDir: Path,
    topic: String,
    bootstrapServers: String,
    batchSize: Int,
    producerBatchSizeBytes: Int,
    lingerMs: Int,
    compressionType: String,
    acks: String,
    clientId: String,
    fileSuffix: String
)

final case class StreamCounters(
    files: Long,
    rows: Long,
    malformedRows: Long,
    sentUrls: Long
)

@main def streamDownloadedSitemapUrlsToKafka(args: String*): Unit =
  if args.toList == List("--help") || args.toList == List("-h") then
    println(usage)
  else
    parseArgs(args.toList) match
      case Left(message) =>
        System.err.println(message)
        System.err.println(usage)
        sys.exit(2)

      case Right(config) =>
        val counters = streamUrls(config)
        println(
          s"Read ${counters.files} sitemap-link TSV files from ${config.inputDir}"
        )
        println(s"Read ${counters.rows} rows")
        println(s"Skipped ${counters.malformedRows} malformed rows")
        println(
          s"Sent ${counters.sentUrls} URLs to Kafka topic ${config.topic}"
        )

private val DefaultBatchSize = 1000
private val DefaultProducerBatchSizeBytes = 131072
private val DefaultLingerMs = 50
private val DefaultCompressionType = "lz4"
private val DefaultAcks = "all"
private val DefaultClientId = "cc-sitemap-url-loader"
private val DefaultFileSuffix = ".sitemap-links.tsv"

private def streamUrls(config: StreamConfig): StreamCounters =
  if !Files.isDirectory(config.inputDir) then
    throw IllegalArgumentException(
      s"Input path is not a directory: ${config.inputDir}"
    )

  val producer = KafkaProducer[String, String](producerProperties(config))

  try
    var files = 0L
    var rows = 0L
    var malformedRows = 0L
    var sentUrls = 0L
    var batch = Vector.empty[String]

    def sendActiveBatch(): Unit =
      if batch.nonEmpty then
        sendBatch(producer, config.topic, batch)
        sentUrls += batch.size
        batch = Vector.empty

    listInputFiles(config.inputDir, config.fileSuffix).foreach { inputFile =>
      files += 1
      Using.resource(Files.lines(inputFile, StandardCharsets.UTF_8)) { lines =>
        lines.iterator().asScala.foreach { line =>
          rows += 1
          parsePageUrl(line) match
            case Some(pageUrl) =>
              batch :+= pageUrl
              if batch.size >= config.batchSize then sendActiveBatch()

            case None =>
              malformedRows += 1
        }
      }
    }

    sendActiveBatch()
    producer.flush()

    StreamCounters(files, rows, malformedRows, sentUrls)
  finally producer.close()

private def listInputFiles(inputDir: Path, fileSuffix: String): Vector[Path] =
  Using.resource(Files.walk(inputDir)) { paths =>
    paths
      .iterator()
      .asScala
      .filter(Files.isRegularFile(_))
      .filter(path => path.getFileName().toString.endsWith(fileSuffix))
      .toVector
      .sortBy(_.toString)
  }

private def parsePageUrl(line: String): Option[String] =
  line.split("\t", -1).toVector match
    case fields if fields.length >= 3 && fields(2).nonEmpty =>
      Some(fields(2))
    case _ =>
      None

private def sendBatch(
    producer: KafkaProducer[String, String],
    topic: String,
    urls: Vector[String]
): Unit =
  val sends: Vector[Future[RecordMetadata]] =
    urls.map(url =>
      producer.send(ProducerRecord[String, String](topic, jsonUrlValue(url)))
    )

  sends.foreach(_.get())

private def jsonUrlValue(url: String): String =
  s"""{"url":"${jsonString(url)}"}"""

private def jsonString(value: String): String =
  value.flatMap {
    case '"'                          => "\\\""
    case '\\'                         => "\\\\"
    case '\b'                         => "\\b"
    case '\f'                         => "\\f"
    case '\n'                         => "\\n"
    case '\r'                         => "\\r"
    case '\t'                         => "\\t"
    case character if character < ' ' =>
      "\\u%04x".format(character.toInt)
    case character =>
      character.toString
  }

private def producerProperties(config: StreamConfig): Properties =
  val properties = Properties()
  properties.put("bootstrap.servers", config.bootstrapServers)
  properties.put("key.serializer", classOf[StringSerializer].getName)
  properties.put("value.serializer", classOf[StringSerializer].getName)
  properties.put("acks", config.acks)
  properties.put("client.id", config.clientId)
  properties.put("batch.size", config.producerBatchSizeBytes.toString)
  properties.put("linger.ms", config.lingerMs.toString)
  properties.put("compression.type", config.compressionType)
  properties

private def parseArgs(args: List[String]): Either[String, StreamConfig] =
  args match
    case inputDir :: topic :: bootstrapServers :: remaining =>
      parseOptions(remaining).map { options =>
        StreamConfig(
          inputDir = Path.of(inputDir).toAbsolutePath.normalize(),
          topic = topic,
          bootstrapServers = bootstrapServers,
          batchSize = options.batchSize,
          producerBatchSizeBytes = options.producerBatchSizeBytes,
          lingerMs = options.lingerMs,
          compressionType = options.compressionType,
          acks = options.acks,
          clientId = options.clientId,
          fileSuffix = options.fileSuffix
        )
      }

    case _ =>
      Left("Missing required arguments.")

private final case class ParsedOptions(
    batchSize: Int = DefaultBatchSize,
    producerBatchSizeBytes: Int = DefaultProducerBatchSizeBytes,
    lingerMs: Int = DefaultLingerMs,
    compressionType: String = DefaultCompressionType,
    acks: String = DefaultAcks,
    clientId: String = DefaultClientId,
    fileSuffix: String = DefaultFileSuffix
)

private def parseOptions(args: List[String]): Either[String, ParsedOptions] =
  def loop(
      remaining: List[String],
      options: ParsedOptions
  ): Either[String, ParsedOptions] =
    remaining match
      case Nil =>
        Right(options)

      case "--batch-size" :: value :: tail =>
        positiveInt("--batch-size", value).flatMap(batchSize =>
          loop(tail, options.copy(batchSize = batchSize))
        )

      case "--producer-batch-size-bytes" :: value :: tail =>
        positiveInt("--producer-batch-size-bytes", value).flatMap(batchSize =>
          loop(tail, options.copy(producerBatchSizeBytes = batchSize))
        )

      case "--linger-ms" :: value :: tail =>
        nonNegativeInt("--linger-ms", value).flatMap(lingerMs =>
          loop(tail, options.copy(lingerMs = lingerMs))
        )

      case "--compression-type" :: value :: tail =>
        loop(tail, options.copy(compressionType = value))

      case "--acks" :: value :: tail =>
        loop(tail, options.copy(acks = value))

      case "--client-id" :: value :: tail =>
        loop(tail, options.copy(clientId = value))

      case "--file-suffix" :: value :: tail =>
        loop(tail, options.copy(fileSuffix = value))

      case option :: _ if option.startsWith("--") =>
        Left(s"Unknown option: $option")

      case value :: _ =>
        Left(s"Unexpected positional argument: $value")

  loop(args, ParsedOptions())

private def positiveInt(option: String, value: String): Either[String, Int] =
  nonNegativeInt(option, value).flatMap { parsed =>
    if parsed > 0 then Right(parsed)
    else Left(s"$option must be greater than zero")
  }

private def nonNegativeInt(option: String, value: String): Either[String, Int] =
  value.toIntOption match
    case Some(parsed) if parsed >= 0 =>
      Right(parsed)
    case _ =>
      Left(s"$option must be a non-negative integer")

private val usage =
  s"""Usage:
     |  scala-cli run scripts/StreamDownloadedSitemapUrlsToKafka.scala -- <input_dir> <topic> <bootstrap_servers> [options]
     |
     |Arguments:
     |  input_dir           Folder containing *.sitemap-links.tsv files from LocalSitemapDownloadPipeline
     |  topic               Kafka topic that receives one page URL per message
     |  bootstrap_servers   Kafka bootstrap servers, for example localhost:9092
     |
     |Options:
     |  --batch-size N                    URLs to enqueue before waiting for Kafka acknowledgements (default: $DefaultBatchSize)
     |  --producer-batch-size-bytes N     Kafka producer batch.size setting (default: $DefaultProducerBatchSizeBytes)
     |  --linger-ms N                     Kafka producer linger.ms setting (default: $DefaultLingerMs)
     |  --compression-type TYPE           Kafka compression.type setting (default: $DefaultCompressionType)
     |  --acks VALUE                      Kafka acks setting (default: $DefaultAcks)
     |  --client-id VALUE                 Kafka client.id setting (default: $DefaultClientId)
     |  --file-suffix VALUE               Input file suffix (default: $DefaultFileSuffix)
     |""".stripMargin
