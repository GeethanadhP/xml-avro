package in.dreamlabs.xmlavro

import java.util
import javax.xml.stream.XMLEventReader
import javax.xml.stream.events.{Attribute, EndElement, StartElement, XMLEvent}

import in.dreamlabs.xmlavro.XMLEvents._
import org.apache.avro.Schema.Type._
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericData.Record

import scala.collection.mutable

/**
  * Created by Royce on 26/01/2017.
  */
trait Implicits {

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

    val attributes: mutable.LinkedHashMap[String, String] = {
      val attrMap = mutable.LinkedHashMap.empty[String, String]
      if (startEle isDefined) {
        val attrs = startEle.get.getAttributes
        while (attrs.hasNext) {
          val attr = attrs.next().asInstanceOf[Attribute]
          val ns = attr.getName.getPrefix
          val name = attr.getName.getLocalPart
          val attrName =
            if (option(ns) isDefined)
              name //s"$ns:$name"
            else
              name
          if (name.toLowerCase() != "schemalocation")
            attrMap += (attrName -> attr.getValue)
        }
      }
      attrMap
    }

    def path: List[AvroPath] = XMLEvents.schemaPath.toList

    def hasAttributes: Boolean = attributes nonEmpty

    def ns: Option[String] =
      if (startEle isDefined) option(startEle.get.getName.getPrefix)
      else if (endEle isDefined) option(endEle.get.getName.getPrefix)
      else {
        val tmp = element.split(":")
        if (tmp.length == 1) option("")
        else option(tmp(0))
      }

    def name: String =
      if (startEle isDefined) startEle.get.getName.getLocalPart
      else if (endEle isDefined) endEle.get.getName.getLocalPart
      else {
        val tmp = element.split(":")
        tmp(tmp.length - 1)
      }

    def push(): Boolean = addElement(name)

    //      if (ns isDefined) addElement(s"${ns get}:$name")
    //      else addElement(name)

    def pop(): Unit = removeElement(name)

    def element: String = eleStack.head

    def text: String = event.asCharacters().getData

    def hasText: Boolean = event.asCharacters().getData.trim != ""
  }

  implicit class RichRecord(record: Record) {
    def at(path: List[AvroPath]): Record = {
      var resultRecord = record
      path.foreach {
        path =>
          if (path.pathType == ARRAY) {
            var array = resultRecord.get(path name).asInstanceOf[util.List[AnyRef]]
            if (array == null || array.size() - 1 < path.index) {
              val arraySchema = AvroUtils.getSchema(resultRecord.getSchema.getField(path name)).getElementType
              if (array == null) {
                resultRecord.put(path name, new util.ArrayList[AnyRef]())
                array = resultRecord.get(path name).asInstanceOf[util.List[AnyRef]]
              }
              resultRecord = AvroUtils.createRecord(arraySchema)
              array.add(resultRecord)
            } else
              resultRecord = array.get(path index).asInstanceOf[Record]
          } else {
            val tempSchema = AvroUtils.getSchema(resultRecord.getSchema.getField(path name))
            var tempRecord = resultRecord.get(path name).asInstanceOf[Record]
            if (tempRecord == null) {
              resultRecord.put(path name, AvroUtils.createRecord(tempSchema))
              tempRecord = resultRecord.get(path name).asInstanceOf[Record]
            }
            resultRecord = tempRecord
          }
      }
      resultRecord
    }

    def add(name: String, value: String, attribute: Boolean = false): Unit = {
      var nodeName = name
      var field = record.getSchema.getField(nodeName)
      var wildcard = false
      if (field == null) {
        field = record.getSchema.getField(Source.TEXT_VALUE)
        if (field == null) {
          field = record.getSchema.getField(Source.WILDCARD)
          if (field == null)
            System.err.println(s"WARNING: ${if (attribute) "Attribute" else "Element"} $name not found in Schema (even as a wildcard)")
          else wildcard = true
        } else nodeName = Source.TEXT_VALUE
      }

      if (wildcard) {
        val wildField = record.get(Source.WILDCARD).asInstanceOf[util.Map[String, AnyRef]]
        wildField.put(nodeName, value)
      } else if (field != null) {
        var fieldSchema = AvroUtils.getSchema(field)
        var fieldType = fieldSchema getType()
        if (fieldType == ARRAY) {
          fieldType = fieldSchema.getElementType.getType
          var array = record.get(nodeName).asInstanceOf[util.List[AnyRef]]
          if (array == null) {
            record.put(nodeName, new util.ArrayList[AnyRef]())
            array = record.get(nodeName).asInstanceOf[util.List[AnyRef]]
//            array = new GenericData.Array[AnyRef](0, fieldSchema getElementType)
//            record.put(nodeName, array)
//            array = record.get(nodeName).asInstanceOf[util.List[AnyRef]]
          }
          array.add(AvroUtils.createValue(fieldType, value))
        } else
          record.put(nodeName, AvroUtils.createValue(fieldType, value))
      }
    }
  }

}

object Implicits extends Implicits
