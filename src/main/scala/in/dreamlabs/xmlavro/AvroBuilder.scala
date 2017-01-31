package in.dreamlabs.xmlavro

import java.io.InputStream
import java.util
import javax.xml.stream.XMLStreamConstants._
import javax.xml.stream.events.XMLEvent
import javax.xml.stream.{XMLEventReader, XMLInputFactory}

import in.dreamlabs.xmlavro.AvroBuilder.unknown
import in.dreamlabs.xmlavro.Implicits._
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData.Record

import scala.reflect.io.Path

/**
  * Created by Royce on 25/01/2017.
  */
class AvroBuilder(schema: Schema,
                  splitBy: String = "",
                  skipMissing: Boolean = false,
                  xsdFile: Option[Path] = None) {

  def createDatums(xmlIn: InputStream): util.List[AnyRef] = {
    val reader: XMLEventReader =
      XMLInputFactory.newInstance.createXMLEventReader(xmlIn)
    val datums = new util.ArrayList[AnyRef]()
    datums.add(getDatums(reader))
    datums
  }

  def getDatums(reader: XMLEventReader): Record = {
    var rootRecord: Record = null
    var rootName: String = splitBy
    AvroBuilder.root = rootName
    var rootFound: Boolean = false
    var proceed: Boolean = false
    var parentEle: String = ""

    reader.dropWhile(!_.isStartElement).foreach { event =>
      event getEventType match {
        case START_DOCUMENT => unknown("Document Start", event)
        case END_DOCUMENT => unknown("Document End", event)
        case START_ELEMENT =>
          if (rootName == "") {
            rootName = event.name
            AvroBuilder.root = rootName
          }
          if (event.name == rootName) {
            rootFound = true
            rootRecord = AvroUtils.createRecord(schema)
            XMLEvents.setSchema(schema, rootRecord)
            proceed = true
          }
          if (rootFound && proceed) {
            proceed = event push()
            parentEle = event.name
            if (event.hasAttributes && proceed) {
              val record = rootRecord.at(event path)
              event.attributes foreach {
                case (name, value) =>
                  record.add(name, value, attribute = true)
              }
            }
          }
        case CHARACTERS =>
          if (rootFound && proceed && event.hasText) {
            val record = rootRecord.at(event path)
            record.add(event name, event text)
          }
        case END_ELEMENT =>
          if (rootFound) {
            if (proceed || event.name == parentEle) {
              proceed = true
              event pop()
              if (event.name == rootName) {
                rootFound = false
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
    rootRecord
  }
}

object AvroBuilder {


  var caseSensitive = true
  var root: String = ""

  private def unknown(message: String, event: XMLEvent) =
    System.err.println(s"WARNING: Unknown $message: $event")

}
