package in.dreamlabs.xmlavro

import java.io.{InputStream, _}
import java.util
import java.util.TimeZone
import javax.xml.XMLConstants
import javax.xml.bind.DatatypeConverter
import javax.xml.parsers.{DocumentBuilderFactory, ParserConfigurationException}
import javax.xml.transform.dom.DOMSource
import javax.xml.transform.stream.{StreamResult, StreamSource}
import javax.xml.transform.{OutputKeys, TransformerException, TransformerFactory, dom}
import javax.xml.validation.SchemaFactory

import org.apache.avro.Schema
import org.apache.avro.Schema.Type._
import org.apache.avro.Schema.{Field, Type}
import org.apache.avro.generic.GenericData
import org.w3c.dom.Node.{ATTRIBUTE_NODE, ELEMENT_NODE, TEXT_NODE}
import org.w3c.dom._
import org.xml.sax
import org.xml.sax.{InputSource, SAXException}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.reflect.io.Path
import scala.util.control.Breaks._

/**
  * Created by Royce on 23/01/2017.
  */
class DatumFixer(schema: Schema,
                 xmlIn: InputStream,
                 splitBy: Option[String] = None,
                 skipMissing: Boolean = false,
                 xsdFile: Option[Path] = None) {
  var caseSensitive = true
  var timeZone: TimeZone = TimeZone.getTimeZone("UTC-0")

  val datums: ListBuffer[Any] = {
    val rootEle = parse(new sax.InputSource(xmlIn))
    val eleList: List[Node] =
      if (splitBy.isDefined && splitBy.get != "") searchElement(rootEle, splitBy.get)
      else List(rootEle)
    val tempDatums = mutable.ListBuffer.empty[Any]
    for (element <- eleList) tempDatums += createNode(schema, element)
    tempDatums
  }


  private def parse(source: InputSource): Element =
    try {
      val builderFactory = DocumentBuilderFactory.newInstance
      builderFactory setNamespaceAware true
      val doc = builderFactory.newDocumentBuilder parse source
      if (xsdFile isDefined) {
        val factory =
          SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI)
        // load a WXS schema, represented by a Schema instance
        val schemaFile = new StreamSource(xsdFile.get.jfile)
        val schema = factory.newSchema(schemaFile)
        val validator = schema.newValidator
        // validate the DOM tree
        validator.validate(new DOMSource(doc))
      }
      doc.getDocumentElement
    } catch {
      case e@(_: ParserConfigurationException | _: SAXException |
              _: IOException) =>
        throw new XMLException(e)
    }

  private def searchElement(ele: Node, searchKey: String): List[Node] = {
    val eleList = mutable.ListBuffer.empty[Node]

    if (ele.getNodeType == Node.ELEMENT_NODE) {
      if (ele.getLocalName == searchKey) eleList += ele
      else {
        var elements =
          ele.asInstanceOf[Element].getElementsByTagNameNS("*", searchKey)
        var len = elements.getLength
        if (len == 0) {
          elements = ele.getChildNodes
          len = elements.getLength
          var i = 0
          breakable {
            while (i < len) {
              eleList ++= searchElement(elements.item(i), searchKey)
              i += 1
              if (eleList.nonEmpty) break
            }
          }
        } else {
          var i = 0
          while (i < len) {
            eleList += elements.item(i)
            i += 1
          }
        }
      }
    }
    eleList.toList
  }

  private def createNode(schema: Schema,
                         source: Node,
                         setFromNode: Boolean = false): AnyRef = {
    source getNodeType match {
      case ELEMENT_NODE | ATTRIBUTE_NODE | TEXT_NODE =>
        schema getType match {
          case UNION => createUnion(schema, source)
          case RECORD =>
            createRecord(schema, source.asInstanceOf[Element], setFromNode)
          case ARRAY => createArray(schema, source.asInstanceOf[Element])
          case otherType =>
            if (DatumFixer.PRIMITIVES.contains(otherType))
              createValue(otherType, source.getTextContent)
            else throw XMLException(s"Unsupported schema type $otherType")
        }
      case other => throw XMLException(s"Unsupported node type $other")
    }
  }

  private def parseDateFrom(text: String): Long = {
    var cal = DatatypeConverter.parseDateTime(text)
    if (text matches DatumFixer.TIMESTAMP_PATTERN)
      cal.setTimeZone(timeZone)
    cal.getTimeInMillis
    //Local
    val tsp = if (!text.matches(DatumFixer TIMESTAMP_PATTERN)) text.substring(0, 19) else text
    cal = DatatypeConverter.parseDateTime(text)
    cal.setTimeZone(timeZone)
    cal.getTimeInMillis
  }

  private def createValue(nodeType: Type, content: String): AnyRef = {
    val result = nodeType match {
      case BOOLEAN => content.toLowerCase == "true" || content == "1"
      case INT => content.toInt
      case LONG =>
        if (content contains "T") parseDateFrom(content trim)
        else content.toLong
      case FLOAT => content.toFloat
      case DOUBLE => content.toDouble
      case STRING => content
      case other => throw XMLException(s"Unsupported type $other")
    }
    result.asInstanceOf[AnyRef]
  }

  private def createUnion(union: Schema, source: Node): AnyRef = {
    val types = union getTypes
    val valid = types.size() == 2 && types.get(0).getType == NULL
    if (!valid)
      throw XMLException(s"Unsupported union types $types")

    createNode(types get 1, source)
  }

  private def createArray(schema: Schema, source: Element): AnyRef = {
    val childNodes = source getChildNodes
    val elementType = schema getElementType
    val count = childNodes.getLength
    val array = new GenericData.Array[AnyRef](count, schema)
    var i = 0
    while (i < count) {
      val child = childNodes item i
      if (child.getNodeType == ELEMENT_NODE)
        array.add(createNode(elementType, child, setFromNode = true))
      i += 1
    }
    array
  }

  private def createRecord(schema: Schema,
                           element: Element,
                           setFromNode: Boolean): AnyRef = {
    val record = new GenericData.Record(schema)
    for (field <- record.getSchema.getFields.asScala) {
      if (field.schema.getType == ARRAY)
        record.put(field.name, new util.ArrayList[AnyRef]())
      if (field.name == Source.WILDCARD)
        record.put(field.name, new util.HashMap[String, AnyRef]())
    }

    val rootRecord = Source.DOCUMENT == schema.getProp(Source.SOURCE)

    if (setFromNode) setFieldFromNode(schema, record, element)
    else {
      val nodes =
        if (rootRecord) element.getOwnerDocument.getChildNodes
        else element.getChildNodes
      var i = 0
      while (i < nodes.getLength) {
        setFieldFromNode(schema, record, nodes.item(i))
        i += 1
      }
    }

    if (!rootRecord) {
      val attributes = element.getAttributes
      var i = 0
      while (i < attributes.getLength) {
        val attribute = attributes.item(i).asInstanceOf[Attr]
        if (!DatumFixer.IGNORED_NS.contains(attribute.getNamespaceURI)) {
          if (!DatumFixer.IGNORED_NAMES.contains(attribute.getName))
            if (!setFromNode) {
              val field = getFieldBySource(
                schema,
                new Source(attribute.getLocalName, attribute = true).toString)
              if (field isEmpty) {
                // Handle wildcard attributes
                val anyField = Option(schema.getField(Source.WILDCARD))
                if (anyField isEmpty) {
                  val message: String = String.format(
                    "Could not find attribute %s of element %s in avro schema",
                    attribute.getName,
                    element.getTagName)
                  if (skipMissing) System.err.println("WARNING : " + message)
                  else throw XMLException(message)
                } else {
                  val map = record
                    .get(Source.WILDCARD)
                    .asInstanceOf[util.HashMap[String, String]]
                  map.put(attribute.getName, attribute.getValue)
                }
              } else {
                val datum = createNode(field.get.schema, attribute, false)
                record.put(field.get.name, datum)
              }
            }
        }
        i += 1
      }
      // Royce - Added for element value (when attributes are available)
      val eleValue = element.getTextContent
      if (eleValue != null && !eleValue.equals("")) {
        val field =
          getFieldBySource(schema, Source(Source.TEXT_VALUE).toString)
        if (field isDefined) {
          val attr = element.getFirstChild
          val datum = createNode(field.get.schema, attr, false)
          record.put(field.get.name, datum)
        }
      }
    }
    record
  }

  private def setFieldFromNode(schema: Schema,
                               record: GenericData.Record,
                               node: Node) = {
    if (node.getNodeType == ELEMENT_NODE) {
      val child = node.asInstanceOf[Element]
      var setRecordFromNode = false
      val fieldName = child.getLocalName
      var field = getFieldBySource(schema, Source(fieldName) toString)
      if (field isEmpty) {
        field = getNestedFieldBySource(schema, Source(fieldName) toString)
        setRecordFromNode = true
      }
      if (field isDefined) {
        val array = field.get.schema.getType == ARRAY
        val datum = createNode(
          if (!array) field.get.schema else field.get.schema.getElementType,
          child,
          setRecordFromNode)
        if (!array) record.put(field.get.name, datum)
        else {
          val values =
            record.get(field.get.name).asInstanceOf[util.List[Object]]
          values.add(datum)
        }
      } else {
        val anyField = Option(schema.getField(Source.WILDCARD))
        if (anyField isEmpty) {
          val message = "Could not find field " + fieldName + " in Avro Schema " + schema.getName + " , neither as specific field nor 'any' element"
          if (skipMissing) System.err.println("WARNING : " + message)
          else throw XMLException(message)
        } else {
          val map = record
            .get(Source.WILDCARD)
            .asInstanceOf[util.HashMap[String, String]]
          map.put(fieldName, getContentAsText(child))
        }
      }
    }
  }

  private def getFieldBySource(schema: Schema,
                               sourceTag: String): Option[Field] = {
    val fieldSchema =
      if (schema.getType == UNION) schema.getTypes.get(1) else schema

    for (field <- fieldSchema.getFields.asScala) {
      val fieldSource = field.getProp(Source.SOURCE)
      if (caseSensitive && sourceTag == fieldSource) return Option(field)
      if (!caseSensitive && sourceTag.toLowerCase == fieldSource.toLowerCase)
        return Option(field)
    }
    None
  }

  private def getNestedFieldBySource(schema: Schema,
                                     sourceTag: String): Option[Field] = {
    var resultField: Option[Field] = None
    if (schema.getType == RECORD)
      breakable {
        for (field <- schema.getFields.asScala) {
          val outerSchema = field.schema
          if (outerSchema.getType == ARRAY) {
            val innerSchema = outerSchema.getElementType
            if (!DatumFixer.PRIMITIVES.contains(innerSchema getType)) {
              val fieldSource =
                try {
                  val tempSource = Some(field.getProp("source"))
                  if (tempSource isEmpty) tempSource else if (tempSource.get == "None") None else tempSource
                }
                catch {
                  // TODO This exception never comes
                  case e: Exception =>
                    System.err.print("WARNING: ")
                    e.printStackTrace(System.err)
                    None
                }

              if (fieldSource isEmpty) {
                resultField =
                  getFieldBySource(outerSchema getElementType, sourceTag)
                if (resultField isDefined)
                  resultField = Some(field)
              }
            }
          }
          if (resultField isDefined) break
        }
      }
    resultField
  }

  private def getContentAsText(element: Element): String = {
    var result = ""
    if (element.getTextContent.length != 0) {
      val writer = new StringWriter()
      try {
        val transformer = TransformerFactory.newInstance.newTransformer
        transformer.setOutputProperty(OutputKeys.METHOD, "xml")
        transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes")
        transformer.transform(new dom.DOMSource(element),
          new StreamResult(writer))
      } catch {
        case impossible: TransformerException =>
          throw new XMLException(impossible)
      }
      result = "" + writer.getBuffer
      // trim element's start tag
      var startTag = result.indexOf(element.getLocalName)
      startTag = result.indexOf('>', startTag)
      result = result.substring(startTag + 1)
      // trim element's end tag
      val endTag = result.lastIndexOf("</")
      result = result.substring(0, endTag)
    }
    result
  }

}

object DatumFixer {
  private val PRIMITIVES: List[Type] =
    List(STRING, INT, LONG, FLOAT, DOUBLE, BOOLEAN, NULL)

  private val TIMESTAMP_PATTERN =
    "^(\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.*\\d*)Z?$"

  private val IGNORED_NS: List[String] = List(
    "http://www.w3.org/2000/xmlns/",
    "http://www.w3.org/2001/XMLSchema-instance")

  private val IGNORED_NAMES: List[String] = List("xml:lang")

  def main(args: Array[String]): Unit = {
    val schema = new SchemaFixer(
      Path("C:/Users/p3008264/git/xml-avro/src/test/resources/books.xsd"),
      Option(
        Path("C:/Users/p3008264/git/xml-avro/xml-avro/src/test/resources")))
    val data = new DatumFixer(
      schema.schema,
      Path("C:/Users/p3008264/git/xml-avro/src/test/resources/books.xml").toFile
        .bufferedInput())
    println(data.datums.iterator.next())
  }
}
