package in.dreamlabs.xmlavro

import java.util.Objects

import org.apache.xerces.xni.XNIException
import org.apache.xerces.xni.parser.{XMLErrorHandler, XMLParseException}
import org.w3c.dom.{DOMError, DOMErrorHandler}

/**
  * Created by Royce on 20/01/2017.
  */
case class XSDException(message: String = null, cause: Throwable = null)
  extends RuntimeException(message, cause) {
  def this(cause: Throwable) = this(null, cause)
}

case class XMLException(message: String = null, cause: Throwable = null)
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
    if (exception isDefined) throw new XSDException(exception.get)
    if (error isDefined) {
      error.get.getRelatedException match {
        case cause: Throwable => throw new XSDException(cause)
        case _ =>
      }
      val locator = error.get.getLocation
      val location = "at:" + locator.getUri + ", line:" + locator.getLineNumber + ", char:" + locator.getColumnNumber
      throw XSDException(location + " " + error.get.getMessage)
    }
  }
}

object Source {
  def apply(name: String, attribute: Boolean = false) = new Source(name, attribute)

  val SOURCE = "source"
  val DOCUMENT = "document"
  val WILDCARD = "others"
}

class Source(val name: String, val attribute: Boolean) {

  def isElement: Boolean = !isAttribute

  def isAttribute: Boolean = attribute

  override def hashCode: Int = Objects.hash(name, attribute.asInstanceOf[Object])

  override def toString: String =
    (if (attribute) "attribute" else "element") + " " + name

  override def equals(obj: Any): Boolean = {
    if (!obj.isInstanceOf[Source]) return false
    val source = obj.asInstanceOf[Source]
    name == source.name && attribute == source.attribute
  }
}
