package in.dreamlabs.xmlavro

import java.io.InputStream
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

  XMLEvents setSchema schema

  def createDatums(xmlIn: InputStream) = {
    val reader: XMLEventReader =
      XMLInputFactory.newInstance.createXMLEventReader(xmlIn)
    val record = getDatums(reader)
    println(record)
  }

  def getDatums(reader: XMLEventReader): Record = {
    var rootRecord: Record = null
    var rootName: String = splitBy
    var currentRecord: Record = null
    var currentElement: String = ""
    AvroBuilder.root = rootName
    var rootFound: Boolean = false

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
          }
          if (rootFound) {
            event push()
            if (event hasAttributes) {
              val record = rootRecord.at(event path)
              event.attributes foreach {
                case (name, value) =>
                  record.add(name, value, attribute = true)
              }
            }
          }
        case CHARACTERS =>
          if (rootFound && event.hasText) {
            val record = rootRecord.at(event path)
            record.add(event name, event text)
          }
        case END_ELEMENT =>
          if (rootFound) {
            event pop()
            if (event.name == currentElement) {
            }
            if (event.name == rootName) {
              rootFound = false
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


  //  private def createNode(record: Record,
  //                         schema: Schema,
  //                         name: String,
  //                         value: Option[String] = None, wildcard: Boolean = false): Unit = {
  //    if (wildcard) {
  //      val wildField = record.get(Source.WILDCARD).asInstanceOf[util.Map[String, AnyRef]]
  //      wildField.put(name, value.get)
  //    }
  //    else
  //      schema.getType match {
  //        case UNION => createNode(record, schema.getTypes.get(1), name, value, wildcard)
  //        case ARRAY => addToArray(record, schema, name, value)
  //        case RECORD => createRecord(schema)
  //        case otherType =>
  //          if (AvroBuilder.PRIMITIVES.contains(otherType))
  //            record.put(name, createValue(otherType, value.get))
  //          else println("Not Ready")
  //      }
  //  }
  //
  //  private def addToArray(record: GenericData.Record,
  //                         schema: Schema,
  //                         name: String,
  //                         value: Option[String]): Unit = {
  //    val elementType = schema.getElementType
  //    val array = record.get(name).asInstanceOf[util.ArrayList[AnyRef]]
  //
  //    elementType getType match {
  //      case RECORD =>
  //        val arrayRecord = createRecord(elementType)
  //        if (value isEmpty)
  //          array.add(arrayRecord)
  //        else {
  //          findAndCreateNode(arrayRecord, name, value)
  //          array.add(arrayRecord)
  //        }
  //      case other => println("Not Ready")
  //    }
  //  }
  //
  //  private def createValue(nodeType: Type, content: String): AnyRef = {
  //    val result = nodeType match {
  //      case BOOLEAN => content.toLowerCase == "true" || content == "1"
  //      case INT => content.toInt
  //      case LONG =>
  //        if (content contains "T") parseDateFrom(content trim)
  //        else content.toLong
  //      case FLOAT => content.toFloat
  //      case DOUBLE => content.toDouble
  //      case STRING => content
  //      case other => throw XMLException(s"Unsupported type $other")
  //    }
  //    result.asInstanceOf[AnyRef]
  //  }
  //
  //  private def parseDateFrom(text: String): Long = {
  //    var cal = DatatypeConverter.parseDateTime(text)
  //    if (text matches AvroBuilder.TIMESTAMP_PATTERN)
  //      cal.setTimeZone(timeZone)
  //    cal.getTimeInMillis
  //    //Local
  //    val tsp =
  //      if (!text.matches(AvroBuilder TIMESTAMP_PATTERN)) text.substring(0, 19)
  //      else text
  //    cal = DatatypeConverter.parseDateTime(text)
  //    cal.setTimeZone(timeZone)
  //    cal.getTimeInMillis
  //  }
  //
  //  private def findAndCreateNode(record: Record,
  //                                name: String,
  //                                value: Option[String] = None,
  //                                attribute: Boolean = false): (String, Record) = {
  //    var currentSchema = record.getSchema
  //    var currentRecord = record
  //    var currentElement = name
  //    var lastElement: String = ""
  //    eleStack.reverseIterator.drop(1).foreach { element =>
  //      lastElement = element
  //      var field = getField(currentSchema, Source(element) toString)
  //      if (field isEmpty)
  //        field = getNestedField(currentSchema, Source(element) toString)
  //      if (field isDefined) {
  //        currentSchema = field.get.schema()
  //        currentSchema getType match {
  //          case UNION => println("find and create Not Ready")
  //          case ARRAY =>
  //            var array =
  //              currentRecord.get(element).asInstanceOf[util.ArrayList[AnyRef]]
  //            if ((array.size() == 0 || element == name) && value.isEmpty)
  //              createNode(currentRecord, currentSchema, name)
  //            array =
  //              currentRecord.get(element).asInstanceOf[util.ArrayList[AnyRef]]
  //            currentRecord = array.get(array.size() - 1).asInstanceOf[Record]
  //            currentSchema = currentRecord getSchema()
  //            currentElement = element
  //          case RECORD => println("find and create Not Ready")
  //          case otherType => println("find and create Not Ready")
  //        }
  //      }
  //    }
  //
  //    var nodeName = name
  //    if (value isDefined) {
  //      if (!attribute && lastElement == name) {
  //        val temp = currentRecord.getSchema.getField(SchemaFixer.TEXT_VALUE)
  //        if (temp != null)
  //          nodeName = SchemaFixer.TEXT_VALUE
  //      }
  //
  //      val (wildcard, field) = findField(currentRecord, nodeName, attribute)
  //      if (field isDefined)
  //        createNode(currentRecord, field.get.schema(), nodeName, value, wildcard)
  //      else
  //        System.err.println(s"${if (attribute) "Attribute" else "Element"} $name not found in Schema (even as a wildcard)")
  //    }
  //    (currentElement, currentRecord)
  //  }
  //
  //  private def findField(record: Record, name: String, attribute: Boolean): (Boolean, Option[Field]) = {
  //    var wildcard = false
  //    val nameString = Source(name, attribute).toString
  //    var field =
  //      getField(record getSchema, nameString)
  //    if (field isEmpty) {
  //      field = getNestedField(record getSchema, nameString)
  //      if (field isEmpty) {
  //        if (name != Source.WILDCARD) {
  //          val temp = record.getSchema.getField(Source.WILDCARD)
  //          if (temp != null) {
  //            field = Option(temp)
  //            wildcard = true
  //          }
  //        }
  //      }
  //    }
  //    (wildcard, field)
  //  }
  //
  //  //  def findFieldPath(fields: util.List[Field],
  //  //                    name: String): (ListBuffer[String], ListBuffer[Type], Option[Field]) = {
  //  //    val path = ListBuffer[String]()
  //  //    val types = ListBuffer[Type]()
  //  //    fields.asScala.foreach { field =>
  //  //      val (innerPath, innerTypes, innerField) = findFieldPath(field schema(), name)
  //  //      if (innerField isDefined) {
  //  //        path += field.name()
  //  //        types += field.schema().getType
  //  //        path ++= innerPath
  //  //        types ++= innerTypes
  //  //        return (path, types, innerField)
  //  //      }
  //  //    }
  //  //    (path, types, None)
  //  //  }
  //  //
  //  //  def findFieldPath(schema: Schema,
  //  //                    name: String): (ListBuffer[String], ListBuffer[Type], Option[Field]) = {
  //  //    val path = ListBuffer[String]()
  //  //    val types = ListBuffer[Type]()
  //  //    var field = getField(schema, name)
  //  //    if (field isEmpty) {
  //  //      field = getNestedField(schema, name)
  //  //      if (field isEmpty) {
  //  //        val (innerPath, innerTypes, innerField) = findFieldPath(schema getFields, name)
  //  //        if (innerField isDefined) {
  //  //          types ++= innerTypes
  //  //          path ++= innerPath
  //  //          return (path, types, innerField)
  //  //        }
  //  //      }
  //  //    }
  //  //    (path, types, field)
  //  //  }
  //  //
  //  //  def findField(schema: Schema, sourceTag: String): (Schema, Option[Field]) = {
  //  //    val fieldSchema =
  //  //      if (schema.getType == UNION) schema.getTypes.get(1)
  //  //      else schema
  //  //
  //  //    fieldSchema getType match {
  //  //      case ARRAY => return findField(fieldSchema getElementType, sourceTag)
  //  //      case RECORD =>
  //  //        for (field <- fieldSchema.getFields.asScala) {
  //  //          val fieldSource = field.getProp(Source.SOURCE)
  //  //          if (caseSensitive && sourceTag == fieldSource)
  //  //            return (fieldSchema, Option(field))
  //  //          if (!caseSensitive && sourceTag.toLowerCase == fieldSource.toLowerCase)
  //  //            return (fieldSchema, Option(field))
  //  //        }
  //  //      case _ =>
  //  //    }
  //  //    (fieldSchema, None)
  //  //  }
  //  //
  //  //  private def findNestedField(schema: Schema,
  //  //                              sourceTag: String): (Schema, Option[Field]) = {
  //  //    var resultField: Option[Field] = None
  //  //    var resultSchema = schema
  //  //    if (schema.getType == RECORD)
  //  //      for (field <- schema.getFields.asScala) {
  //  //        val outerSchema = field.schema
  //  //        if (outerSchema.getType == ARRAY) {
  //  //          val innerSchema = outerSchema.getElementType
  //  //          if (!AvroBuilder.PRIMITIVES.contains(innerSchema getType)) {
  //  //            val fieldSource =
  //  //              try {
  //  //                val tempSource = Some(field.getProp("source"))
  //  //                if (tempSource isEmpty) tempSource
  //  //                else if (tempSource.get == "None") None
  //  //                else tempSource
  //  //              } catch {
  //  //                // TODO This exception never comes
  //  //                case e: Exception =>
  //  //                  System.err.print("WARNING: ")
  //  //                  e.printStackTrace(System.err)
  //  //                  None
  //  //              }
  //  //
  //  //            if (fieldSource isEmpty) {
  //  //              val (tempSchema, tempField) =
  //  //                findField(outerSchema getElementType, sourceTag)
  //  //              if (tempField isDefined) {
  //  //                resultField = Some(field)
  //  //                resultSchema = tempSchema
  //  //              }
  //  //            }
  //  //          }
  //  //        }
  //  //        if (resultField isDefined)
  //  //          return (resultSchema, resultField)
  //  //      }
  //  //    (resultSchema, resultField)
  //  //  }
  //  //
  //  private def getField(schema: Schema, sourceTag: String): Option[Field] = {
  //    val fieldSchema =
  //      if (schema.getType == UNION) schema.getTypes.get(1)
  //      else schema
  //
  //    fieldSchema getType match {
  //      case ARRAY =>
  //        return getField(fieldSchema getElementType, sourceTag)
  //      case RECORD =>
  //        for (field <- fieldSchema.getFields.asScala) {
  //          val fieldSource = field.getProp(Source.SOURCE)
  //          if (caseSensitive && sourceTag == fieldSource)
  //            return Option(field)
  //          if (!caseSensitive && sourceTag.toLowerCase == fieldSource.toLowerCase)
  //            return Option(field)
  //        }
  //      case _ =>
  //    }
  //    None
  //  }
  //
  //  private def getNestedField(schema: Schema,
  //                             sourceTag: String): Option[Field] = {
  //    var resultField: Option[Field] = None
  //    if (schema.getType == RECORD)
  //      for (field <- schema.getFields.asScala) {
  //        val outerSchema = field.schema
  //        if (outerSchema.getType == ARRAY) {
  //          val innerSchema = outerSchema.getElementType
  //          if (!AvroBuilder.PRIMITIVES.contains(innerSchema getType)) {
  //            val fieldSource =
  //              try {
  //                val tempSource = Some(field.getProp("source"))
  //                if (tempSource isEmpty) tempSource
  //                else if (tempSource.get == "None") None
  //                else tempSource
  //              } catch {
  //                // TODO This exception never comes
  //                case e: Exception =>
  //                  System.err.print("WARNING: ")
  //                  e.printStackTrace(System.err)
  //                  None
  //              }
  //
  //            if (fieldSource isEmpty) {
  //              val tempField =
  //                getField(outerSchema getElementType, sourceTag)
  //              if (tempField isDefined) {
  //                resultField = Some(field)
  //              }
  //            }
  //          }
  //        }
  //        if (resultField isDefined)
  //          return resultField
  //      }
  //    resultField
  //  }
  //
  //  def createRecord(schema: Schema): GenericData.Record = {
  //    val record = new GenericData.Record(schema)
  //    for (field <- record.getSchema.getFields.asScala) {
  //      if (field.schema.getType == ARRAY)
  //        record.put(field.name, new util.ArrayList[AnyRef]())
  //      if (field.name == Source.WILDCARD)
  //        record.put(field.name, new util.HashMap[String, AnyRef]())
  //    }
  //    record
  //  }

  private def unknown(message: String, event: XMLEvent) =
    System.err.println(s"WARNING: Unknown $message: $event")

  def main(args: Array[String]) {
    val schema = new SchemaFixer(
      Path("C:/Users/p3008264/git/xml-avro/src/test/resources/books.xsd"),
      Option(
        Path("C:/Users/p3008264/git/xml-avro/xml-avro/src/test/resources")))
    val input = Path(
      "C:/Users/p3008264/git/xml-avro/src/test/resources/books.xml")
    //    val input = Path("C:/Users/p3008264/git/xml-avro/xsd/sales/sales_big.xml")
    //    new AvroBuilder(schema.schema, input.toFile.bufferedReader())

  }
}
