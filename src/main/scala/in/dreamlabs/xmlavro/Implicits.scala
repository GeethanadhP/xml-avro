package in.dreamlabs.xmlavro

import javax.xml.stream.XMLEventReader
import javax.xml.stream.events.{Attribute, EndElement, StartElement, XMLEvent}

import in.dreamlabs.xmlavro.XMLEvents._

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
              s"$ns:$name"
            else
              name
          if (name.toLowerCase() != "schemalocation")
            attrMap += (attrName -> attr.getValue)
        }
      }
      attrMap
    }

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

    def push(): Unit =
      if (ns isDefined) eleStack = s"${ns get}:$name" :: eleStack
      else eleStack = name :: eleStack

    def pop(): Unit = eleStack = eleStack.tail

    def element: String = eleStack.head

    def text: String = event.asCharacters().getData

    def hasText: Boolean = event.asCharacters().getData.trim != ""
  }

}

object XMLEvents {
  var eleStack: List[String] = List[String]()

  def option(text: String): Option[String] = {
    if (text.trim == "") None else Option(text)
  }
}

object Implicits extends Implicits
