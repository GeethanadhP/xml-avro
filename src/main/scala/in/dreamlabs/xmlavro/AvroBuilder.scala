package in.dreamlabs.xmlavro

import java.io.OutputStream
import javax.xml.XMLConstants
import javax.xml.stream.XMLStreamConstants._
import javax.xml.stream.events.{Attribute, EndElement, StartElement, XMLEvent}
import javax.xml.stream.util.EventReaderDelegate
import javax.xml.stream.{XMLEventReader, XMLInputFactory}
import javax.xml.transform.stax.StAXSource
import javax.xml.validation.SchemaFactory

import in.dreamlabs.xmlavro.AvroBuilder.unknown
import in.dreamlabs.xmlavro.RichAvro._
import in.dreamlabs.xmlavro.XMLEvents.{addElement, eleStack, removeElement}
import in.dreamlabs.xmlavro.config.XMLConfig
import org.apache.avro.Schema
import org.apache.avro.file.{CodecFactory, DataFileWriter}
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.specific.SpecificDatumWriter

import scala.collection.mutable

/**
  * Created by Royce on 25/01/2017.
  */
class AvroBuilder(config: XMLConfig) {
  private val splits = config.split
  private val validationXSD = config.validationXSD
  RichAvro.caseSensitive = config.caseSensitive
  RichAvro.ignoreMissing = config.ignoreMissing
  XNode.namespaces = config.namespaces

  def createDatums(): Unit = {
    val xmlIn =
      if (config.streamingInput) System.in
      else config.xmlFile.toFile.bufferedInput()
    val reader = new EventReaderDelegate(XMLInputFactory.newInstance.createXMLEventReader(xmlIn))

    val thread = new Thread {
      override def run() {
        Utils.startTimer("XSD Validation")
        if (validationXSD isDefined) {
          val factory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI)
          val xsdSchema = factory.newSchema(validationXSD.get.jfile)
          val validator = xsdSchema.newValidator()
          validator.validate(new StAXSource(reader))
        }
        Utils.endTimer("XSD Validation")
      }
    }
    thread.start()

    val writers = mutable.Map[String, DataFileWriter[Record]]()
    val schemas = mutable.Map[String, Schema]()
    val streams = mutable.ListBuffer[OutputStream]()
    splits.forEach { split =>
      val schema = new Schema.Parser().parse(split.avscFile.jfile)
      val datumWriter = new SpecificDatumWriter[Record](schema)
      val fileWriter = new DataFileWriter[Record](datumWriter)

      fileWriter setCodec (CodecFactory snappyCodec)
      val avroOut = split.avroFile.toFile.bufferedOutput()
      fileWriter create(schema, avroOut)
      streams += avroOut
      writers += split.by -> fileWriter
      schemas += split.by -> schema
    }

    var splitRecord: Record = null
    var splitFound: Boolean = false
    var proceed: Boolean = false
    var parentEle: String = ""

    reader.dropWhile(!_.isStartElement).foreach { event =>
      event getEventType match {
        case START_DOCUMENT => unknown("Document Start", event)
        case END_DOCUMENT => unknown("Document End", event)
        case START_ELEMENT =>
          if (writers contains "") {
            writers += event.name -> writers("")
            schemas += event.name -> schemas("")
            writers remove ""
            schemas remove ""
          }
          if (writers contains event.name) {
            if (splitFound)
              ConversionError("Splits cannot be inside each other, they should be completely separated tags")
            splitFound = true
            splitRecord = schemas(event name).newRecord
            XMLEvents.setSchema(schemas(event name), splitRecord)
            proceed = true
          }
          if (splitFound && proceed) {
            proceed = event push()
            parentEle = event.name
            println(s"Processed $parentEle")
            if (event.hasAttributes && proceed) {
              val record = splitRecord.at(event path)
              event.attributes foreach {
                case (xEle, value) =>
                  record.add(xEle, value)
              }
            }
          }
        case CHARACTERS =>
          if (splitFound && proceed && event.hasText) {
            val record = splitRecord.at(event path)
            record.add(event element, event text)
          }
        case END_ELEMENT =>
          if (splitFound) {
            if (proceed || event.name == parentEle) {
              proceed = true
              event pop()
              if (writers contains event.name) {
                val writer = writers(event name)
                writer append splitRecord
                writer flush()
                splitFound = false
              }
            }
          }
        case ATTRIBUTE => unknown("Attribute", event)
        case PROCESSING_INSTRUCTION => unknown("Processing Instruction", event)
        case COMMENT => // Ignore Comments
        case ENTITY_REFERENCE => unknown("Entity Reference", event)
        case DTD => unknown("DTD", event)
        case CDATA => unknown("CDATA", event)
        case SPACE => unknown("Space", event)
        case _ => "Unknown"
      }
    }

    xmlIn.close()
    writers.values.foreach(_.close())
    streams.foreach(_.close())
  }

  implicit class RichXMLEventIterator(reader: XMLEventReader)
    extends Iterator[XMLEvent] {
    def hasNext: Boolean = reader hasNext

    def next: XMLEvent = reader nextEvent
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
      if (eleStack.isEmpty) addElement(XNode(name, nsURI, nsName, attribute = false))
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

    def pop(): Unit =
      removeElement(XNode(name, nsURI, nsName, attribute = false))

    def text: String = event.asCharacters().getData

    def hasText: Boolean = event.asCharacters().getData.trim != ""
  }

}

object AvroBuilder {
  private def unknown(message: String, event: XMLEvent) =
    System.err.println(s"WARNING: Unknown $message: $event")

}
