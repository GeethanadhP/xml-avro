package in.dreamlabs.xmlavro

import in.dreamlabs.xmlavro.Utils.option
import org.apache.xerces.xni.XNIException
import org.apache.xerces.xni.parser.{XMLErrorHandler, XMLParseException}
import org.apache.xerces.xs.XSObject
import org.w3c.dom.{DOMError, DOMErrorHandler}

/**
  * Created by Royce on 20/01/2017.
  */
case class ConversionError(message: String = null, cause: Throwable = null)
  extends RuntimeException(message, cause) {
  def this(cause: Throwable) = this(null, cause)
}

class ErrorHandler extends XMLErrorHandler with DOMErrorHandler {
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
    if (exception isDefined) throw new ConversionError(exception.get)
    if (error isDefined) {
      error.get.getRelatedException match {
        case cause: Throwable => throw new ConversionError(cause)
        case _ =>
      }
      val locator = error.get.getLocation
      val location = "at:" + locator.getUri + ", line:" + locator.getLineNumber + ", char:" + locator.getColumnNumber
      throw ConversionError(location + " " + error.get.getMessage)
    }
  }
}

case class XNode(name: String,
                 nsURI: String,
                 nsName: String,
                 attribute: Boolean) {
  val element: Boolean = !attribute

  def source: String =
    (if (attribute) "attribute" else "element") + s" $fullName"

  def fullName: String =
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

  def textNode: XNode = new XNode(TEXT_VALUE, null, null, attribute = false)

  def wildNode(attribute: Boolean): XNode = new XNode(WILDCARD, null, null, attribute)
}
