package in.dreamlabs.xmlavro

import java.util.{Calendar, TimeZone}
import javax.xml.bind.DatatypeConverter

import in.dreamlabs.xmlavro.AvroPath.countsMap
import org.apache.avro.Schema.Type
import org.apache.avro.Schema.Type._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by Royce on 28/01/2017.
  */
class AvroPath(val name: String,
               val pathType: Type,
               currentPath: ListBuffer[AvroPath],
               val virtual: Boolean = false) {

  private val innerName = {
    val builder = StringBuilder.newBuilder
    builder append s"$name"
    currentPath.foreach(path =>
      builder append path.toString)
    builder.mkString
  }

  val index: Int =
    if (countsMap contains innerName) {
      var currentIndex = countsMap(innerName)
      currentIndex += 1
      countsMap += (innerName -> currentIndex)
      currentIndex
    } else {
      countsMap += (innerName -> 0)
      0
    }

  def destroy(): Unit = {
    var currentIndex = countsMap(innerName)
    currentIndex -= 1
    countsMap += (innerName -> currentIndex)
  }

  override def toString: String =
    if (pathType == ARRAY) s"$name[$index]" else name
}

object AvroPath {
  val countsMap: mutable.Map[String, Int] = mutable.Map[String, Int]()

  def apply(name: String,
            pathType: Type,
            currentPath: ListBuffer[AvroPath],
            virtual: Boolean = false) =
    new AvroPath(name, pathType, currentPath, virtual)

  def reset(): Unit = countsMap.clear()
}


object AvroUtils {
  private val TIMESTAMP_PATTERN =
    "^(\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.*\\d*)Z?$"

  var timeZone: TimeZone = TimeZone.getTimeZone("UTC-0")

  def createValue(nodeType: Type, content: String): AnyRef = {
    val result = nodeType match {
      case BOOLEAN => content.toLowerCase == "true" || content == "1"
      case INT => content.toInt
      case LONG =>
        if (content contains "T") parseDateFrom(content trim)
        else content.toLong
      case FLOAT => content.toFloat
      case DOUBLE => content.toDouble
      case STRING => content
      case other => throw ConversionError(s"Unsupported type $other")
    }
    result.asInstanceOf[AnyRef]
  }

  private def parseDateFrom(text: String): Long = {
    var cal = DatatypeConverter.parseDateTime(text)
    if (text matches TIMESTAMP_PATTERN)
      cal.setTimeZone(timeZone)
    cal.getTimeInMillis
    //Local
    val tsp =
      if (!text.matches(TIMESTAMP_PATTERN)) text.substring(0, 19)
      else text
    cal = DatatypeConverter.parseDateTime(tsp)
    cal.setTimeZone(timeZone)
    cal.getTimeInMillis
  }
}

object Utils {
  var debugEnabled = false

  def option(text: String): Option[String] = {
    if (Option(text) isDefined)
      if (text.trim == "") None else Option(text)
    else None
  }

  def debug(text: String): Unit = if (debugEnabled) System.err.println(s"DEBUG: $text")

  def info(text: String): Unit = System.err.println(s"INFO: $text")

  def profile(tag: String)(op: => Unit): Unit = {
    val start = Calendar.getInstance().getTimeInMillis
    op
    val end = Calendar.getInstance().getTimeInMillis
    System.err.println(s"$tag took: ${(end - start) / 1000.0} seconds")
  }
}
