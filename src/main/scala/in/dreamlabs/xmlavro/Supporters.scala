package in.dreamlabs.xmlavro

import java.io.{IOException, PipedReader, PipedWriter}
import javax.xml.XMLConstants
import javax.xml.stream.events.XMLEvent
import javax.xml.stream.{XMLEventFactory, XMLEventWriter, XMLOutputFactory}
import javax.xml.transform.stream.StreamSource
import javax.xml.validation.{Schema, SchemaFactory}

import in.dreamlabs.xmlavro.Utils._
import in.dreamlabs.xmlavro.config.XMLConfig
import org.apache.xerces.xni.XNIException
import org.apache.xerces.xni.parser.{XMLErrorHandler, XMLParseException}
import org.apache.xerces.xs.XSObject
import org.w3c.dom.{DOMError, DOMErrorHandler}
import org.xml.sax.{ErrorHandler, SAXParseException}

import scala.collection.mutable
import scala.compat.Platform.EOL
import scala.reflect.io.Path

/**
  * Created by Royce on 20/01/2017.
  */
case class ConversionError(message: String = null, cause: Throwable = null)
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

class XMLDocument(val id: Int, config: XMLConfig) {
  private val events = mutable.ListBuffer[XMLEvent]()
  var error = false
  private var exception: Exception = _
  private var pipeIn: PipedReader = _
  private var pipeOut: PipedWriter = _
  private var eventOut: XMLEventWriter = _
  info(s"Processing document #$id")

  private var validationThread = if (config.validationXSD isDefined) {
    pipeIn = new PipedReader()
    pipeOut = new PipedWriter(pipeIn)
    eventOut = XMLOutputFactory.newInstance().createXMLEventWriter(pipeOut)
    Option(new Thread {
      override def run(): Unit = {
        val validator = XMLDocument.schema.newValidator()
        //        validator.setErrorHandler(new ValidationErrorHandler(XMLDocument.this))
        try validator.validate(new StreamSource(pipeIn))
        catch {
          case e: SAXParseException => fail(e)
          case e: Exception => warn("Exception in thread: " + e.getMessage)
            fail(e)
        } finally {
          pipeIn.close()
          info(s"Finished xsd validation on document #$id")
        }
      }
    })
  } else None

  if (validationThread isDefined) validationThread.get.start()

  def add(event: XMLEvent): Unit = this.synchronized {
    if (config.errorFile isDefined) events += event
    if (validationThread.isDefined && !error) eventOut.add(event)
  }

  def fail(exception: Exception): Unit = this.synchronized {
    if (!error) {
      error = true
      this.exception = exception
      if (validationThread isDefined) validationThread = None
    }
  }

  def close(): Unit = this.synchronized {
    if (error) {
      log(
        config.docErrorLevel,
        s"Failed processing document #$id with reason \'${exception.getMessage}\'")
      debug(exception.getStackTrace.mkString("", EOL, EOL))
      if (config.errorFile.isDefined) {
        info(s"Saving the failed document #$id in ${config.errorFile.get}")
        val out = XMLOutputFactory
          .newInstance()
          .createXMLEventWriter(
            config.errorFile.get.toFile.bufferedWriter(append = true))
        events += XMLEventFactory.newInstance().createSpace("\n")
        events.foreach(out.add)
        out.flush()
        out.close()
      }
    }
    if (validationThread isDefined) {
      eventOut.flush()
      pipeOut.flush()
      eventOut.close()
      pipeOut.close()
      validationThread.get.join()
    }
    info(s"Closed document #$id")
  }
}

object XMLDocument {
  private var schema: Schema = _
  private var count: Int = 0
  var config: XMLConfig = _

  def apply(): XMLDocument = {
    count += 1
    if (Option(schema).isEmpty && config.validationXSD.isDefined)
      schema = SchemaFactory
        .newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI)
        .newSchema(config.validationXSD.get.jfile)
    new XMLDocument(count, config)
  }

  def closeAll(): Unit = {
    if (config.qaDir.isDefined) {
      val qaDir = config.qaDir.get
      if (!qaDir.exists)
        qaDir.jfile.mkdir()
      try {
        val docCountOut = Path("DOCUMENT_COUNT").toAbsoluteWithRoot(qaDir).toFile.bufferedWriter()
        docCountOut.write(count + "")
        docCountOut.close()
      } catch {
        case e: IOException => warn("Problem occurred while writing DOCUMENT_COUNT to QA DIR :" + e.getMessage)
      }
    }
  }
}
