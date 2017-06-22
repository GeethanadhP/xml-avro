package in.dreamlabs.xmlavro

import java.io.{IOException, PipedReader, PipedWriter, PrintWriter}
import javax.xml.XMLConstants
import javax.xml.stream.events.XMLEvent
import javax.xml.stream.{XMLEventFactory, XMLEventWriter, XMLOutputFactory}
import javax.xml.transform.stream.StreamSource
import javax.xml.validation.{Schema, SchemaFactory}

import in.dreamlabs.xmlavro.Utils.{info, log, warn}
import in.dreamlabs.xmlavro.config.XMLConfig
import org.xml.sax.SAXParseException

import scala.collection.mutable
import scala.reflect.io.{File, Path}

/**
  * Created by Royce on 06/03/2017.
  */
class XMLDocument(val id: Int, val uniqueKey: Option[String], config: XMLConfig) {
  private val events = mutable.ListBuffer[XMLEvent]()
  @volatile var error = false
  private var exceptionList: mutable.ListBuffer[Exception] =
    mutable.ListBuffer()
  private var pipeIn: PipedReader = _
  private var pipeOut: PipedWriter = _
  private var eventOut: XMLEventWriter = _
  private var errorDataFile, errorMetaFile: File = _
  private val locker: AnyRef = new AnyRef
  val docText = s"document #$id${
    if (uniqueKey.isDefined)
      s" with Unique ID: \'${uniqueKey.get.toString}\'"
    else ""
  }"

  info("Processing " + docText)

  if (config.errorFile isDefined) {
    val filePath = config.errorFile.get
    val fileName = filePath.stripExtension
    val fileSuffix = if (uniqueKey isDefined) s"${id}__${uniqueKey.get}" else s"$id"
    val parent = filePath.parent
    errorDataFile = Path(s"${fileName}__$fileSuffix")
      .toAbsoluteWithRoot(parent)
      .addExtension("xml")
      .toFile
    errorMetaFile = Path(s"${fileName}__$fileSuffix")
      .toAbsoluteWithRoot(parent)
      .addExtension("MD")
      .toFile
  }

  private var validationThread = if (config.validationXSD isDefined) {
    pipeIn = new PipedReader()
    pipeOut = new PipedWriter(pipeIn)
    eventOut = XMLOutputFactory.newInstance().createXMLEventWriter(pipeOut)
    Option(new Thread {
      override def run(): Unit = {
        val validator = XMLDocument.schema.newValidator()
        try validator.validate(new StreamSource(pipeIn))
        catch {
          case e: SAXParseException =>
            val message =  s"XSD validation failed - Line: ${e.getLineNumber}, Column: ${e.getColumnNumber}, Message: ${e.getMessage}"
            fail(ConversionException(message))
          case e: Exception =>
            warn("Exception in thread: " + e.getMessage)
            fail(e)
        } finally {
          pipeIn.close()
          info(s"Finished xsd validation on " + docText)
        }
      }
    })
  } else None

  if (validationThread isDefined) validationThread.get.start()

  def add(event: XMLEvent): Unit = locker.synchronized {
    if (config.errorFile isDefined) events += event
    if (validationThread.isDefined && !error) eventOut.add(event)
  }

  def fail(exception: Exception, wait: Boolean = false): Unit = {
    if (wait) {
      var thread: Thread = null
      validationThread.synchronized {
        if (validationThread.isDefined)
          thread = validationThread.get
      }
      if (Option(thread) isDefined)
        thread.join(2000)
    }
    locker.synchronized {
      error = true
      exceptionList += exception
      validationThread.synchronized {
        if (validationThread isDefined) validationThread = None
      }
    }
  }

  def close(): Unit = this.synchronized {
    if (error) {
      val reasons = {
        val builder = StringBuilder.newBuilder
        exceptionList.foreach(exc =>
          builder.append(exc.getMessage).append(", "))
        builder.mkString.stripSuffix(", ")
      }
      log(config.docErrorLevel,
        s"Failed processing $docText with reason '$reasons'")
      if (config.errorFile.isDefined) {
        info(
          s"Saving the failed $docText in '$errorDataFile' with message in '$errorMetaFile'")
        val dataOut = XMLOutputFactory
          .newInstance()
          .createXMLEventWriter(errorDataFile.bufferedWriter())
        events += XMLEventFactory.newInstance().createSpace("\n")
        events.foreach(dataOut.add)
        dataOut.flush()
        dataOut.close()
        val metaOut = new PrintWriter(errorMetaFile.bufferedWriter())
        metaOut.write(reasons)
        metaOut.flush()
        metaOut.close()
      }
    }

    var thread: Thread = null
    validationThread.synchronized {
      if (validationThread isDefined) thread = validationThread.get
    }
    if (Option(thread) isDefined) {
      try {
        eventOut.flush()
        pipeOut.flush()
        eventOut.close()
        pipeOut.close()
        info(s"Waiting for xsd validation of $docText to finish")
        thread.join(5000)
        if (thread.isAlive) {
          warn(
            s"Schema validation timed out for $docText, ignoring and proceeding further")
          pipeIn.close()
        }
      } catch {
        case e: Exception =>
          warn(
            s"Failed to close pipes for $docText with message '${e.getMessage}', ignoring and proceeding further")
      }
    }
    info(s"Closed document #$id")
  }
}

object XMLDocument {
  private var schema: Schema = _
  private var count: Int = 0
  var config: XMLConfig = _

  def apply(uniqueKey: Option[String]): XMLDocument = {
    if (count == 0 && config.errorFile.isDefined) {
      config.errorFile.get.delete()
    }
    count += 1
    if (Option(schema).isEmpty && config.validationXSD.isDefined)
      schema = SchemaFactory
        .newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI)
        .newSchema(config.validationXSD.get.jfile)
    new XMLDocument(count, uniqueKey, config)
  }

  def closeAll(): Unit = {
    if (config.qaDir.isDefined) {
      val qaDir = config.qaDir.get
      if (!qaDir.exists)
        qaDir.jfile.mkdir()
      try {
        val docCountOut = Path("DOCUMENT_COUNT")
          .toAbsoluteWithRoot(qaDir)
          .toFile
          .bufferedWriter()
        docCountOut.write(count + "")
        docCountOut.close()
      } catch {
        case e: IOException =>
          warn("Problem occurred while writing DOCUMENT_COUNT to QA DIR :" + e.getMessage)
      }
    }
  }
}
