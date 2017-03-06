package in.dreamlabs.xmlavro

import java.io.{IOException, PipedReader, PipedWriter}
import javax.xml.XMLConstants
import javax.xml.stream.{XMLEventFactory, XMLEventWriter, XMLOutputFactory}
import javax.xml.stream.events.XMLEvent
import javax.xml.transform.stream.StreamSource
import javax.xml.validation.{Schema, SchemaFactory}

import in.dreamlabs.xmlavro.Utils.{debug, info, log, warn}
import in.dreamlabs.xmlavro.config.XMLConfig
import org.xml.sax.SAXParseException

import scala.collection.mutable
import scala.compat.Platform.EOL
import scala.reflect.io.Path

/**
  * Created by Royce on 06/03/2017.
  */
class XMLDocument(val id: Int, config: XMLConfig) {
  private val events = mutable.ListBuffer[XMLEvent]()
  var error = false
  private var exception: Exception = _
  private var pipeIn: PipedReader = _
  private var pipeOut: PipedWriter = _
  private var eventOut: XMLEventWriter = _
  info(s"Processing document #$id")
  if (id==42)
    println()

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
    if (count == 0 && config.errorFile.isDefined){
      config.errorFile.get.delete()
    }
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

