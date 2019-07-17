package in.dreamlabs.xmlavro

import java.io._
import java.nio.ByteBuffer
import java.util
import javax.xml.stream.XMLInputFactory
import javax.xml.stream.XMLStreamConstants._
import javax.xml.stream.events.{Attribute, EndElement, StartElement, XMLEvent}

import in.dreamlabs.xmlavro.AvroBuilder.unknown
import in.dreamlabs.xmlavro.RichAvro._
import in.dreamlabs.xmlavro.XMLEvents.{addElement, eleStack, removeElement}
import in.dreamlabs.xmlavro.config.XMLConfig
import org.apache.avro.Schema
import org.apache.avro.file.{CodecFactory, DataFileStream, DataFileWriter}
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.specific.SpecificDatumWriter
import in.dreamlabs.xmlavro.Utils.info
import scala.collection.JavaConverters._
import scala.collection.mutable
import org.apache.commons.io.input.CountingInputStream

/**
  * Created by Royce on 25/01/2017.
  */
class AvroBuilder(config: XMLConfig) {
  Utils.debugEnabled = config.debug
  RichAvro.caseSensitive = config.caseSensitive
  RichAvro.ignoreCaseFor =
    config.ignoreCaseFor.asScala.toList.map(element => element.toLowerCase)
  RichAvro.ignoreMissing = config.ignoreMissing
  RichAvro.suppressWarnings = config.suppressWarnings
  XNode.namespaces = config.namespaces
  XMLDocument.config = config

  private val writers = mutable.Map[String, DataFileWriter[Record]]()
  private val schemas = mutable.Map[String, Schema]()
  private val streams = mutable.ListBuffer[OutputStream]()

  def createDatums(): Unit = {
    config.split.forEach { split =>
      val schema = new Schema.Parser().parse(split.avscFile.jfile)
      val datumWriter = new SpecificDatumWriter[Record](schema)
      val fileWriter = new DataFileWriter[Record](datumWriter)
      fileWriter setCodec (CodecFactory snappyCodec)
      val avroOut =
        if (split stream) new BufferedOutputStream(System.out)
        else split.avroFile.toFile.bufferedOutput()
      fileWriter create(schema, avroOut)
      streams += avroOut
      writers += split.by -> fileWriter
      schemas += split.by -> schema
    }

    val sourceInput =
      if (config.streamingInput) new BufferedInputStream(System.in)
      else config.xmlFile.toFile.bufferedInput()

    if (config.useAvroInput) {
      val avroReader = new DataFileStream[GenericRecord](
        sourceInput,
        new GenericDatumReader[GenericRecord]())
      var avroCount = 0
      avroReader.forEach { record =>
        val xmlIn = new BufferedInputStream(
          new ByteArrayInputStream(
            record.get(config.inputAvroKey).asInstanceOf[ByteBuffer].array()))
        var uniqueKey = if (config.inputAvroUniqueKey isDefined) {
          val keys = config.inputAvroUniqueKey.get.split('.')
          val valueMap =
            record.get(keys(0)).asInstanceOf[util.HashMap[AnyRef, AnyRef]]
          var found: Option[String] = None
          valueMap.forEach {
            case (key, value) =>
              if (key.toString.equals(keys(1))) {
                found = Some(value.toString)
              }
          }
          found
        } else None
        avroCount += 1
        info(s"Loading avro record #$avroCount for Unique ID: ${uniqueKey}")
        createFromXML(xmlIn, Some(record), uniqueKey)
        info(s"Finished avro record #$avroCount for Unique ID: ${uniqueKey}")
      }
      avroReader.close()
      sourceInput.close()
    } else {
      createFromXML(sourceInput)
    }

    XMLDocument.closeAll()

    writers.values.foreach { writer =>
      writer.flush()
      writer.close()
    }

    streams.foreach(_.close())
  }

  def createFromXML(xmlIn: InputStream,
                    sourceAvro: Option[GenericRecord] = None,
                    uniqueKey: Option[String] = None): Unit = {
    val countingStream = new CountingInputStream(xmlIn)
    val reader = XMLInputFactory.newInstance.createXMLEventReader(countingStream)
    var splitRecord: Record = null
    var splitFound, documentFound: Boolean = false
    var proceed: Boolean = false
    var parentEle: String = ""
    var currentDoc: Option[XMLDocument] = None
    var prevEvent: XMLEvent = null
    var lastPrintMB: Long = 0

    while (reader.hasNext) {
      var event: XMLEvent = null
      try {
        event = reader.nextEvent
        if (Utils.debugEnabled){
          val currentMB = countingStream.getByteCount/1024/1024
          if (currentMB > lastPrintMB){
            Utils.debug(s"Processed ${currentMB} Mb")
            lastPrintMB = currentMB
          }
        }
      } catch {
        case e: Exception =>
          currentDoc match {
            case None =>
              Utils.log(config.docErrorLevel,
                s"No XML data received, ${e.getMessage} ")
              return
            case Some(doc) =>
              doc.fail(
                ConversionException(s"Invalid XML received, ${e.getMessage} ",
                  e),
                wait = true)
              documentFound = false
              currentDoc.get close()
              currentDoc = None
              return
          }
      }
      if (Option(event) isDefined) {
        try {
          if (currentDoc isDefined)
            currentDoc.get add event
          event getEventType match {
            case START_DOCUMENT | END_DOCUMENT => //Ignore
            case START_ELEMENT =>
              if (writers contains "") {
                writers += event.name -> writers("")
                schemas += event.name -> schemas("")
                writers remove ""
                schemas remove ""
              }
              if (config.documentRootTag == event.name) {
                documentFound = true
                proceed = true
                splitFound = false
                currentDoc = Some(XMLDocument(uniqueKey))
                currentDoc.get add event
              }

              if (currentDoc.isDefined && !currentDoc.get.error) {
                if (writers.contains(event.name)) {
                  if (splitFound)
                    ConversionException(
                      "Splits cannot be inside each other, they should be completely separated tags")
                  splitFound = true
                  splitRecord = schemas(event name).newRecord
                  XMLEvents.setSchema(schemas(event name), splitRecord)
                  AvroPath.reset()
                  proceed = true
                }

                if (splitFound && proceed) {
                  proceed = event push()
                  parentEle = event.fullName

                  if (event.hasAttributes && proceed) {
                    val record = splitRecord.at(event path)
                    event.attributes foreach {
                      case (xEle, value) =>
                        record.add(xEle, value)
                    }
                  }
                }
              }
            case CHARACTERS =>
              if (splitFound && proceed && currentDoc.isDefined && !currentDoc.get.error && event.hasText) {
                val record = splitRecord.at(event path)
                record.add(event element, event text)
              }
            case END_ELEMENT =>
              if (splitFound && proceed && currentDoc.isDefined && !currentDoc.get.error && prevEvent.isStartElement) {
                if (event.path.nonEmpty) {
                  val path = event.path.last.name
                  if (path != event.name) {
                    val record = splitRecord.at(event path)
                    record.add(event element, "")
                  }
                }
              }
              if (currentDoc.isDefined && !currentDoc.get.error) {
                if (splitFound && (proceed || event.fullName == parentEle)) {
                  proceed = true
                  event pop()
                  if (writers.contains(event.name)) {
                    if (sourceAvro isDefined) {
                      config.inputAvroMappings.foreach {
                        case (source, target) =>
                          if ((source != config.inputAvroKey) && !config.inputAvroUniqueKey
                            .contains(source)) {
                            splitRecord.put(target, sourceAvro.get.get(source))
                          }
                      }
                    }
                    val writer = writers(event name)
                    writer append splitRecord
                    Utils.info(
                      s"Writing avro record for ${currentDoc.get.docText} split at ${event.name}")
                    splitFound = false
                  }
                }
              }
            case COMMENT => // Do nothing
            case other => unknown(other.toString, event)
          }
        } catch {
          case e: Exception =>
            currentDoc match {
              case None => throw new ConversionException(e)
              case Some(doc) =>
                var innerMessage =
                  s"'${event.toString}' after ${prevEvent.toString} at Line: ${event.getLocation.getLineNumber}, Column: ${event.getLocation.getColumnNumber}"
                val message =
                  s"${e.toString}${if (config.debug) "\n" + e.getStackTrace.mkString("\n")} occurred while processing $innerMessage"
                doc.fail(ConversionException(message), wait = true)
            }
            proceed = false
        } finally {
          if (event.isEndElement && config.documentRootTag == event.name) {
            documentFound = false
            currentDoc.get close()
            currentDoc = None
          }
          prevEvent = event
        }
      }
    }
    xmlIn.close()
  }

  implicit class RichXMLEvent(event: XMLEvent) {

    private val startEle: Option[StartElement] =
      if (event isStartElement)
        Some(event.asStartElement())
      else
        None

    private val endEle: Option[EndElement] =
      if (event isEndElement)
        Some(event.asEndElement())
      else
        None

    val attributes: mutable.LinkedHashMap[XNode, String] = {
      val attrMap = mutable.LinkedHashMap.empty[XNode, String]
      if (startEle isDefined) {
        val attrs = startEle.get.getAttributes
        while (attrs.hasNext) {
          val attr = attrs.next().asInstanceOf[Attribute]
          val name = attr.getName
          if (name.getLocalPart.toLowerCase() != "schemalocation")
            attrMap += XNode(name.getLocalPart,
              name.getNamespaceURI,
              name.getPrefix,
              attribute = true) -> attr.getValue
        }
      }
      attrMap
    }

    def path: List[AvroPath] = XMLEvents.schemaPath.toList

    def hasAttributes: Boolean = attributes nonEmpty

    def push(): Boolean = {
      if (eleStack.isEmpty)
        addElement(XNode(name, nsURI, nsName, attribute = false))
      else addElement(XNode(element, name, nsURI, nsName, attribute = false))
    }

    private def nsURI: String =
      if (startEle isDefined) startEle.get.getName.getNamespaceURI
      else if (endEle isDefined) endEle.get.getName.getNamespaceURI
      else element.nsURI

    private def nsName: String =
      if (startEle isDefined) startEle.get.getName.getPrefix
      else if (endEle isDefined) endEle.get.getName.getPrefix
      else element.nsName

    def element: XNode = eleStack.head

    def name: String =
      if (startEle isDefined) startEle.get.getName.getLocalPart
      else if (endEle isDefined) endEle.get.getName.getLocalPart
      else element.name

    def fullName: String = {
      XNode(name, nsURI, nsName, attribute = false).fullName()
    }

    def pop(): Unit =
      removeElement(XNode(name, nsURI, nsName, attribute = false))

    def text: String = event.asCharacters().getData

    def hasText: Boolean = text.trim() != "" || text.matches(" +")
  }

}

object AvroBuilder {
  private def unknown(message: String, event: XMLEvent) =
    Utils.warn(s"WARNING: Unknown $message: $event")
}
