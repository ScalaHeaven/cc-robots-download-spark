import java.nio.charset.StandardCharsets
import java.util.Locale

final case class RobotsTxt(
    groups: Vector[RobotsGroup],
    sitemaps: Vector[String],
    warnings: Vector[RobotsParseWarning]
) {
  def isValid: Boolean =
    groups.nonEmpty || sitemaps.nonEmpty

  def groupsFor(userAgent: String): Vector[RobotsGroup] = {
    val normalizedUserAgent = userAgent.toLowerCase(Locale.ROOT)

    groups.filter(_.appliesTo(normalizedUserAgent))
  }
}

final case class RobotsGroup(
    userAgents: Vector[String],
    rules: Vector[RobotsRule],
    crawlDelay: Option[Double],
    requestRate: Option[RobotsRequestRate]
) {
  def appliesTo(userAgent: String): Boolean = {
    val normalizedUserAgent = userAgent.toLowerCase(Locale.ROOT)

    userAgents.exists { candidate =>
      candidate == "*" || normalizedUserAgent.contains(candidate)
    }
  }
}

sealed trait RobotsRule {
  def path: String
}

object RobotsRule {
  final case class Allow(path: String) extends RobotsRule
  final case class Disallow(path: String) extends RobotsRule
}

final case class RobotsRequestRate(requests: Int, seconds: Int)

final case class RobotsParseWarning(lineNumber: Int, message: String)

object RobotsTxtParser {
  def parse(content: Array[Byte]): RobotsTxt =
    parse(new String(content, StandardCharsets.UTF_8))

  def parse(content: String): RobotsTxt = {
    val builder = ParserBuilder()

    content.linesIterator.zipWithIndex.foreach { case (line, index) =>
      parseLine(line, index + 1, builder)
    }

    builder.result()
  }

  def isValid(content: String): Boolean =
    parse(content).isValid

  def isValid(content: Array[Byte]): Boolean =
    parse(content).isValid

  private def parseLine(
      rawLine: String,
      lineNumber: Int,
      builder: ParserBuilder
  ): Unit = {
    val line = stripComment(rawLine).trim

    if (line.nonEmpty) {
      line.split(":", 2).toList match {
        case fieldName :: value :: Nil =>
          applyField(
            normalizeFieldName(fieldName),
            value.trim,
            lineNumber,
            builder
          )

        case _ =>
          builder.warn(lineNumber, s"Expected 'field: value', found '$line'")
      }
    }
  }

  private def applyField(
      fieldName: String,
      value: String,
      lineNumber: Int,
      builder: ParserBuilder
  ): Unit =
    fieldName match {
      case "user-agent" =>
        if (value.nonEmpty) {
          builder.addUserAgent(value)
        } else {
          builder.warn(lineNumber, "Missing user-agent value")
        }

      case "allow" =>
        builder.addRule(RobotsRule.Allow(value), lineNumber)

      case "disallow" =>
        builder.addRule(RobotsRule.Disallow(value), lineNumber)

      case "crawl-delay" =>
        parseCrawlDelay(value) match {
          case Some(delay) => builder.setCrawlDelay(delay, lineNumber)
          case None        =>
            builder.warn(
              lineNumber,
              s"Invalid crawl-delay value '$value'"
            )
        }

      case "request-rate" =>
        parseRequestRate(value) match {
          case Some(requestRate) =>
            builder.setRequestRate(requestRate, lineNumber)
          case None =>
            builder.warn(
              lineNumber,
              s"Invalid request-rate value '$value'"
            )
        }

      case "sitemap" =>
        if (value.nonEmpty) {
          builder.addSitemap(value)
        } else {
          builder.warn(lineNumber, "Missing sitemap value")
        }

      case _ =>
        ()
    }

  private def stripComment(line: String): String =
    line.takeWhile(_ != '#')

  private def normalizeFieldName(fieldName: String): String =
    fieldName.trim.stripPrefix("\uFEFF").toLowerCase(Locale.ROOT)

  private def parseCrawlDelay(value: String): Option[Double] =
    value.toDoubleOption.filter(_ >= 0)

  private def parseRequestRate(value: String): Option[RobotsRequestRate] =
    value.split("/", 2).toList match {
      case requests :: seconds :: Nil =>
        for {
          requestCount <- requests.trim.toIntOption.filter(_ > 0)
          secondCount <- seconds.trim.toIntOption.filter(_ > 0)
        } yield RobotsRequestRate(requestCount, secondCount)

      case _ =>
        None
    }

  private final class ParserBuilder {
    private var completedGroups = Vector.empty[RobotsGroup]
    private var currentUserAgents = Vector.empty[String]
    private var currentRules = Vector.empty[RobotsRule]
    private var currentCrawlDelay = Option.empty[Double]
    private var currentRequestRate = Option.empty[RobotsRequestRate]
    private var parsedSitemaps = Vector.empty[String]
    private var parsedWarnings = Vector.empty[RobotsParseWarning]

    def addUserAgent(userAgent: String): Unit = {
      if (
        currentRules.nonEmpty || currentCrawlDelay.nonEmpty || currentRequestRate.nonEmpty
      ) {
        flushGroup()
      }

      currentUserAgents :+= userAgent.toLowerCase(Locale.ROOT)
    }

    def addRule(rule: RobotsRule, lineNumber: Int): Unit =
      if (currentUserAgents.nonEmpty) {
        currentRules :+= rule
      } else {
        warn(lineNumber, "Ignored rule before user-agent")
      }

    def setCrawlDelay(crawlDelay: Double, lineNumber: Int): Unit =
      if (currentUserAgents.nonEmpty) {
        currentCrawlDelay = Some(crawlDelay)
      } else {
        warn(lineNumber, "Ignored crawl-delay before user-agent")
      }

    def setRequestRate(
        requestRate: RobotsRequestRate,
        lineNumber: Int
    ): Unit =
      if (currentUserAgents.nonEmpty) {
        currentRequestRate = Some(requestRate)
      } else {
        warn(lineNumber, "Ignored request-rate before user-agent")
      }

    def addSitemap(sitemap: String): Unit =
      parsedSitemaps :+= sitemap

    def warn(lineNumber: Int, message: String): Unit =
      parsedWarnings :+= RobotsParseWarning(lineNumber, message)

    def result(): RobotsTxt = {
      flushGroup()
      RobotsTxt(completedGroups, parsedSitemaps, parsedWarnings)
    }

    private def flushGroup(): Unit = {
      if (currentUserAgents.nonEmpty) {
        completedGroups :+= RobotsGroup(
          userAgents = currentUserAgents,
          rules = currentRules,
          crawlDelay = currentCrawlDelay,
          requestRate = currentRequestRate
        )
      }

      currentUserAgents = Vector.empty
      currentRules = Vector.empty
      currentCrawlDelay = None
      currentRequestRate = None
    }
  }

  private object ParserBuilder {
    def apply(): ParserBuilder =
      new ParserBuilder()
  }
}
