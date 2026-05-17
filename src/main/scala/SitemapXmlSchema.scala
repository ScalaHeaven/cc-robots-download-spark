import java.io.InputStream
import java.net.URI

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

  def parse(
      input: InputStream
  ): Either[SitemapXmlValidationError, SitemapXmlDocument] =
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
              localName == "loc" && currentEntry != null &&
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

            if (localName == "loc" && currentEntry != null) {
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
    if (rootName != "urlset" && rootName != "sitemapindex") {
      Left(
        SitemapXmlValidationError(
          s"Unsupported sitemap root element: $rootName"
        )
      )
    } else if (rootNamespace.nonEmpty && rootNamespace != SitemapNamespace) {
      Left(
        SitemapXmlValidationError(
          s"Unsupported sitemap namespace: $rootNamespace"
        )
      )
    } else {
      Right(())
    }

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

  private def isEntryElement(rootName: String, localName: String): Boolean =
    (rootName == "urlset" && localName == "url") ||
      (rootName == "sitemapindex" && localName == "sitemap")

  private def containsWhitespaceOrControl(value: String): Boolean =
    value.exists(character => character.isWhitespace || character.isControl)
}
