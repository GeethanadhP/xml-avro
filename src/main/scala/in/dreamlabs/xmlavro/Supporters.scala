package in.dreamlabs.xmlavro

import java.util.{Calendar, TimeZone}
import javax.xml.bind.DatatypeConverter

import in.dreamlabs.xmlavro.RichAvro.{ignoreMissing, suppressWarnings}
import in.dreamlabs.xmlavro.Utils._
import org.apache.avro.Schema.Type
import AvroPath.countsMap
import org.apache.avro.Schema.Type._
import org.apache.xerces.xni.XNIException
import org.apache.xerces.xni.parser.{XMLErrorHandler, XMLParseException}
import org.apache.xerces.xs.XSObject
import org.w3c.dom.{DOMError, DOMErrorHandler}
import org.xml.sax.{ErrorHandler, SAXParseException}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by Royce on 20/01/2017.
  */
case class ConversionException(message: String = null, cause: Throwable = null)
  extends RuntimeException(message, cause) {
  def this(cause: Throwable) = this(null, cause)
}

class XSDErrorHandler extends XMLErrorHandler with DOMErrorHandler {
  private var exception: Option[XMLParseException] = None
  private var error: Option[DOMError] = None

  @throws[XNIException]
  def warning(domain: String,
              key: String,
              exception: XMLParseException): Unit =
    if (this.exception isEmpty) this.exception = Option(exception)

  @throws[XNIException]
  def error(domain: String, key: String, exception: XMLParseException): Unit =
    if (this.exception isEmpty) this.exception = Option(exception)

  @throws[XNIException]
  def fatalError(domain: String,
                 key: String,
                 exception: XMLParseException): Unit =
    if (this.exception isEmpty) this.exception = Option(exception)

  def handleError(error: DOMError): Boolean = {
    if (this.error isEmpty) this.error = Option(error)
    false
  }

  def check(): Unit = {
    if (exception isDefined) throw new ConversionException(exception.get)
    if (error isDefined) {
      error.get.getRelatedException match {
        case cause: Throwable => throw new ConversionException(cause)
        case _ =>
      }
      val locator = error.get.getLocation
      val location = "at:" + locator.getUri + ", line:" + locator.getLineNumber + ", char:" + locator.getColumnNumber
      throw ConversionException(location + " " + error.get.getMessage)
    }
  }
}

class ValidationErrorHandler(var xml: XMLDocument) extends ErrorHandler {
  def warning(exception: SAXParseException): Unit = {
    handle(exception)
  }

  def error(exception: SAXParseException): Unit = {
    handle(exception)
  }

  def fatalError(exception: SAXParseException): Unit = {
    handle(exception)
  }

  private def handle(exception: SAXParseException): Unit = xml.fail(exception)
}

case class XNode(name: String,
                 nsURI: String,
                 nsName: String,
                 attribute: Boolean) {
  var parentNS: String = _
  val element: Boolean = !attribute

  def sourceMatches(sourceTag: String,
                    caseSensitive: Boolean,
                    ignoreList: List[String]): Boolean = {
    val matches =
      if (caseSensitive)
        if (ignoreList contains sourceTag.toLowerCase)
          source.equalsIgnoreCase(sourceTag) || parentNSSource
            .equalsIgnoreCase(sourceTag)
        else
          source == sourceTag || parentNSSource == sourceTag
      else
        source.equalsIgnoreCase(sourceTag) || parentNSSource.equalsIgnoreCase(
          sourceTag)
    matches
  }

  def source: String =
    (if (attribute) "attribute" else "element") + s" ${fullName()}"

  def parentNSSource: String =
    (if (attribute) "attribute" else "element") + s" ${fullName(other = true)}"

  def fullName(other: Boolean = false): String =
    if (other)
      s"${if (option(parentNS) isDefined) parentNS + ":" else ""}$name"
    else
      s"${if (option(nsURI) isDefined) nsURI + ":" else ""}$name"

  override def toString: String =
    s"${if (option(nsName) isDefined) nsName + ":" else ""}$name"
}

object XNode {
  val SOURCE = "source"
  val DOCUMENT = "document"
  val WILDCARD = "others"
  val TEXT_VALUE = "text_value"
  var namespaces = true

  def apply(ele: XSObject, attribute: Boolean = false): XNode =
    new XNode(ele.getName, ele.getNamespace, null, attribute)

  def apply(parentNode: XNode,
            name: String,
            nsURI: String,
            nsName: String,
            attribute: Boolean): XNode = {
    val node = new XNode(name, nsURI, nsName, attribute)
    if (option(nsURI) isEmpty)
      if (option(parentNode.nsURI) isDefined) node.parentNS = parentNode.nsURI
      else node.parentNS = parentNode.parentNS
    node
  }

  def textNode: XNode = new XNode(TEXT_VALUE, null, null, attribute = false)

  def wildNode(attribute: Boolean): XNode =
    new XNode(WILDCARD, null, null, attribute)
}

class AvroPath(val name: String,
               val pathType: Type,
               currentPath: ListBuffer[AvroPath],
               val virtual: Boolean = false) {

  private val innerName = {
    val builder = StringBuilder.newBuilder
    builder append s"$name"
    currentPath.foreach(path =>
      builder append path.toString)
    builder.mkString
  }

  val index: Int =
    if (countsMap contains innerName) {
      var currentIndex = countsMap(innerName)
      currentIndex += 1
      countsMap += (innerName -> currentIndex)
      currentIndex
    } else {
      countsMap += (innerName -> 0)
      0
    }

  def destroy(): Unit = {
    var currentIndex = countsMap(innerName)
    currentIndex -= 1
    countsMap += (innerName -> currentIndex)
  }

  override def toString: String =
    if (pathType == ARRAY) s"$name[$index]" else name
}

object AvroPath {
  val countsMap: mutable.Map[String, Int] = mutable.Map[String, Int]()
  val missingNodes: ListBuffer[String] = ListBuffer[String]()

  def apply(name: String,
            pathType: Type,
            currentPath: ListBuffer[AvroPath],
            virtual: Boolean = false) =
    new AvroPath(name, pathType, currentPath, virtual)

  def reset(): Unit = countsMap.clear()

  def missing(eleStack: ListBuffer[XNode], node: XNode = null): Unit = {
    val builder = StringBuilder.newBuilder
    var missingStack = eleStack
    var missingNode = node
    if (Option(node) isEmpty) {
      missingStack = eleStack.tail
      missingNode = eleStack.head
    }
    missingStack.reverse.foreach(ele => builder append s"$ele/")
    builder.append(s"${if (missingNode attribute) "@" else ""}${missingNode name}")
    val fullNode = builder.mkString
    if (!missingNodes.contains(fullNode)) {
      missingNodes += fullNode
      val message = s"$fullNode is not found in Schema (even as a wildcard)"
      if (ignoreMissing && !suppressWarnings)
        warn(message)
      else if (!ignoreMissing)
        throw ConversionException(message)
    }
  }
}


object AvroUtils {
  private val TIMESTAMP_PATTERN =
    "^(\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.*\\d*)Z?$"

  var timeZone: TimeZone = TimeZone.getTimeZone("UTC-0")

  def createValue(nodeType: Type, content: String): AnyRef = {
    val result = nodeType match {
      case BOOLEAN => content.toLowerCase == "true" || content == "1"
      case INT => content.toInt
      case LONG =>
        if (content contains "T") parseDateFrom(content trim)
        else content.toLong
      case FLOAT => content.toFloat
      case DOUBLE => content.toDouble
      case STRING => content
      case other => throw ConversionException(s"Unsupported type $other")
    }
    result.asInstanceOf[AnyRef]
  }

  private def parseDateFrom(text: String): Long = {
    var cal = DatatypeConverter.parseDateTime(text)
    if (text matches TIMESTAMP_PATTERN)
      cal.setTimeZone(timeZone)
    cal.getTimeInMillis
    //Local
    val tsp =
      if (!text.matches(TIMESTAMP_PATTERN)) text.substring(0, 19)
      else text
    cal = DatatypeConverter.parseDateTime(tsp)
    cal.setTimeZone(timeZone)
    cal.getTimeInMillis
  }
}

object Utils {
  var debugEnabled = false

  def option(text: String): Option[String] = {
    if (Option(text) isDefined)
      if (text.trim == "") None else Option(text)
    else None
  }

  def debug(text: String): Unit = if (debugEnabled) log("DEBUG", text)

  def info(text: String): Unit = log("INFO", text)

  def warn(text: String): Unit = log("WARNING", text)

  def log(level: String, text: String): Unit = System.err.println(s"${Calendar.getInstance().getTime} ${level.toUpperCase}: $text")

  def profile(tag: String)(op: => Unit): Unit = {
    val start = Calendar.getInstance().getTimeInMillis
    op
    val end = Calendar.getInstance().getTimeInMillis
    info(s"$tag took: ${(end - start) / 1000.0} seconds")
  }
}
