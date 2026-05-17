import java.io.{ByteArrayInputStream, InputStream}
import java.net.URI
import java.nio.charset.StandardCharsets

import javax.xml.XMLConstants
import javax.xml.stream.{XMLInputFactory, XMLStreamConstants}

sealed trait SitemapXmlDocument {
  def locs: Vector[String]
}

object SitemapXmlDocument {
  final case class UrlSet(urls: Vector[String]) extends SitemapXmlDocument {
    override def locs: Vector[String] = urls
  }

  final case class SitemapIndex(sitemaps: Vector[String])
      extends SitemapXmlDocument {
    override def locs: Vector[String] = sitemaps
  }
}

final case class SitemapXmlValidationError(message: String)

object SitemapXmlSchema {
  private val SitemapNamespace = "http://www.sitemaps.org/schemas/sitemap/0.9"
  private val AtomNamespace = "http://www.w3.org/2005/Atom"

  def parse(
      input: InputStream
  ): Either[SitemapXmlValidationError, SitemapXmlDocument] = {
    val bytes = input.readAllBytes()

    if (looksLikeXml(bytes)) {
      parseXml(ByteArrayInputStream(bytes))
    } else {
      parseTextSitemap(bytes)
    }
  }

  private def parseXml(
      input: InputStream
  ): Either[SitemapXmlValidationError, SitemapXmlDocument] = {
    val factory = XMLInputFactory.newFactory()
    configureSecureXmlInput(factory)

    val reader = factory.createXMLStreamReader(input)
    var rootName: String | Null = null
    var rootNamespace: String | Null = null
    var currentEntry: String | Null = null
    var elementStack = Vector.empty[String]
    var insideLoc = false
    var currentLoc = StringBuilder()
    var locs = Vector.empty[String]

    try {
      while (reader.hasNext()) {
        reader.next() match {
          case XMLStreamConstants.START_ELEMENT =>
            val localName = reader.getLocalName()
            elementStack :+= localName

            if (rootName == null) {
              rootName = localName
              rootNamespace = reader.getNamespaceURI()
              validateRoot(
                rootName,
                Option(rootNamespace).getOrElse("")
              ) match {
                case Left(error) => return Left(error)
                case Right(_)    =>
              }
            } else if (isEntryElement(rootName, localName)) {
              currentEntry = localName
            } else if (
              rootName == "feed" && currentEntry == "entry" &&
              localName == "link"
            ) {
              atomLinkHref(
                reader.getAttributeValue(null, "rel"),
                reader.getAttributeValue(null, "href")
              ).foreach { href =>
                locs :+= href
              }
            } else if (
              isTextLocElement(rootName, localName) && currentEntry != null &&
              elementStack.dropRight(1).lastOption.contains(currentEntry)
            ) {
              insideLoc = true
              currentLoc = StringBuilder()
            }

          case XMLStreamConstants.CHARACTERS | XMLStreamConstants.CDATA =>
            if (insideLoc) {
              currentLoc.append(reader.getText())
            }

          case XMLStreamConstants.END_ELEMENT =>
            val localName = reader.getLocalName()

            if (isTextLocElement(rootName, localName) && currentEntry != null) {
              val loc = currentLoc.toString().trim
              if (loc.nonEmpty) {
                locs :+= loc
              }
              insideLoc = false
              currentLoc = StringBuilder()
            } else if (currentEntry != null && localName == currentEntry) {
              currentEntry = null
            }
            if (elementStack.nonEmpty) {
              elementStack = elementStack.dropRight(1)
            }

          case _ =>
        }
      }

      Option(rootName) match {
        case Some("urlset") if locs.nonEmpty =>
          Right(SitemapXmlDocument.UrlSet(locs.distinct))
        case Some("sitemapindex") if locs.nonEmpty =>
          Right(SitemapXmlDocument.SitemapIndex(locs.distinct))
        case Some("rss" | "feed") if locs.nonEmpty =>
          Right(SitemapXmlDocument.UrlSet(locs.distinct))
        case Some(name) =>
          Left(
            SitemapXmlValidationError(
              s"Sitemap $name document does not contain any loc entries"
            )
          )
        case None =>
          Left(SitemapXmlValidationError("Sitemap XML document is empty"))
      }
    } catch {
      case exception: Exception =>
        Left(
          SitemapXmlValidationError(
            s"Invalid sitemap XML: ${exception.getMessage}"
          )
        )
    } finally {
      reader.close()
    }
  }

  def resolveLoc(baseUri: URI, loc: String): Option[String] =
    try {
      val resolved = baseUri.resolve(loc).normalize()

      if (isValidExtractedUrl(resolved)) {
        Some(resolved.toString)
      } else {
        None
      }
    } catch {
      case _: IllegalArgumentException =>
        None
    }

  def isValidExtractedUrl(uri: URI): Boolean =
    Option(uri.getScheme()).exists { scheme =>
      val normalizedScheme = scheme.toLowerCase(java.util.Locale.ROOT)
      normalizedScheme == "http" || normalizedScheme == "https"
    } && Option(uri.getRawAuthority()).exists(_.nonEmpty) &&
      !uri.isOpaque && !containsWhitespaceOrControl(uri.toString)

  private def configureSecureXmlInput(factory: XMLInputFactory): Unit = {
    setXmlProperty(factory, XMLInputFactory.SUPPORT_DTD, false)
    setXmlProperty(factory, XMLConstants.ACCESS_EXTERNAL_DTD, "")
    setXmlProperty(factory, XMLConstants.ACCESS_EXTERNAL_SCHEMA, "")
    setXmlProperty(
      factory,
      "javax.xml.stream.isSupportingExternalEntities",
      false
    )
  }

  private def validateRoot(
      rootName: String,
      rootNamespace: String
  ): Either[SitemapXmlValidationError, Unit] =
    if (
      rootName != "urlset" && rootName != "sitemapindex" &&
      rootName != "rss" && rootName != "feed"
    ) {
      Left(
        SitemapXmlValidationError(
          s"Unsupported sitemap root element: $rootName"
        )
      )
    } else if (rootNamespace.nonEmpty && rootNamespace != SitemapNamespace) {
      rootName match {
        case "feed" if rootNamespace == AtomNamespace =>
          Right(())
        case "rss" =>
          Right(())
        case _ =>
          Left(
            SitemapXmlValidationError(
              s"Unsupported sitemap namespace: $rootNamespace"
            )
          )
      }
    } else {
      Right(())
    }

  private def parseTextSitemap(
      bytes: Array[Byte]
  ): Either[SitemapXmlValidationError, SitemapXmlDocument] = {
    val lines = new String(bytes, StandardCharsets.UTF_8)
      .stripPrefix("\uFEFF")
      .linesIterator
      .map(_.trim)
      .filter(_.nonEmpty)
      .toVector

    if (lines.isEmpty) {
      Left(SitemapXmlValidationError("Text sitemap document is empty"))
    } else {
      val invalidLine = lines.find { line =>
        !resolveLoc(URI.create("https://example.invalid/"), line).contains(line)
      }

      invalidLine match {
        case Some(line) =>
          Left(
            SitemapXmlValidationError(
              s"Invalid text sitemap URL: $line"
            )
          )
        case None =>
          Right(SitemapXmlDocument.UrlSet(lines.distinct))
      }
    }
  }

  private def looksLikeXml(bytes: Array[Byte]): Boolean = {
    val text = new String(bytes.take(256), StandardCharsets.UTF_8)
      .stripPrefix("\uFEFF")
      .dropWhile(_.isWhitespace)

    text.startsWith("<")
  }

  private def atomLinkHref(
      rel: String | Null,
      href: String | Null
  ): Option[String] =
    val normalizedRel =
      Option(rel).map(_.trim.toLowerCase(java.util.Locale.ROOT))
    Option(href).map(_.trim).filter { value =>
      value.nonEmpty && normalizedRel.forall(_ == "alternate")
    }

  private def isTextLocElement(
      rootName: String | Null,
      localName: String
  ): Boolean =
    rootName match {
      case "urlset" | "sitemapindex" => localName == "loc"
      case "rss"                     => localName == "link"
      case _                         => false
    }

  private def isEntryElement(
      rootName: String | Null,
      localName: String
  ): Boolean =
    (rootName == "urlset" && localName == "url") ||
      (rootName == "sitemapindex" && localName == "sitemap") ||
      (rootName == "rss" && localName == "item") ||
      (rootName == "feed" && localName == "entry")

  private def setXmlProperty(
      factory: XMLInputFactory,
      propertyName: String,
      value: String | Boolean
  ): Unit =
    try {
      factory.setProperty(propertyName, value)
    } catch {
      case _: IllegalArgumentException =>
    }

  private def containsWhitespaceOrControl(value: String): Boolean =
    value.exists(character => character.isWhitespace || character.isControl)
}
