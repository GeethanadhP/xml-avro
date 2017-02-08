package in.dreamlabs.xmlavro

import java.util.{Calendar, TimeZone}
import javax.xml.bind.DatatypeConverter

import in.dreamlabs.xmlavro.AvroPath.countsMap
import in.dreamlabs.xmlavro.RichAvro._
import org.apache.avro.Schema
import org.apache.avro.Schema.Type._
import org.apache.avro.Schema.{Field, Type}
import org.apache.avro.generic.GenericData.Record

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks._

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
      builder append path.toString.replace(path name, ""))
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

  def reset: Unit = countsMap.clear()
}

object XMLEvents {
  val PRIMITIVES: List[Type] =
    List(STRING, INT, LONG, FLOAT, DOUBLE, BOOLEAN, NULL)
  val eleStack: ListBuffer[XNode] = ListBuffer[XNode]()
  val schemaPath: ListBuffer[AvroPath] = ListBuffer[AvroPath]()
  var rootSchema: Schema = _
  var rootRecord: Record = _
  private var lastSchema = rootSchema

  def setSchema(schema: Schema, record: Record): Unit = {
    rootSchema = schema
    rootRecord = record
    lastSchema = rootSchema
  }

  def addElement(node: XNode): Boolean = {
    eleStack.insert(0, node)
    var found = false
    if (eleStack.size != 1) {
      val (field, path, schema) = searchField(lastSchema, node)
      if (field isDefined) {
        schemaPath ++= path.reverse
        updatePath(field.get)
        found = true
      } else {
        val builder = StringBuilder.newBuilder
        eleStack.reverse.foreach(ele => builder append s"$ele/")
        System.err.println(
          s"WARNING: Element ${builder.stripSuffix("/")} is not found in Schema")
      }
    } else found = true
    found
  }

  def removeElement(node: XNode): Unit = {
    eleStack.remove(0)
    var count = schemaPath.size
    if (count != 0) {
      if (schemaPath.last.name == node.name) {
        count = destroyLastPath()
        while (count != 0 && schemaPath.last.virtual) {
          count = destroyLastPath()
        }
      } else if (schemaPath.last.name.startsWith("type"))
        while (count != 0 && schemaPath.last.virtual) {
          count = destroyLastPath()
        }
      lastSchema = rootRecord.at(schemaPath.toList).getSchema
    }
  }

  private def destroyLastPath(): Int = {
    val tempPath = schemaPath.last
    schemaPath -= tempPath
    //    tempPath destroy()
    schemaPath size
  }

  def searchField(
                   schema: Schema,
                   node: XNode): (Option[Field], ListBuffer[AvroPath], Schema) = {
    var fieldSchema = schema.simplify
    var field = schema.field(node)
    val path = ListBuffer[AvroPath]()
    if (field isEmpty)
      breakable {
        for (typeField <- fieldSchema.customTypeFields()) {
          val (resultField, resultPath, resultSchema) =
            searchField(typeField.fieldSchema, node)
          if (resultField isDefined) {
            val (tempPath, tempSchema) = getPath(typeField, virtual = true)
            resultPath ++= tempPath
            path ++= resultPath
            field = resultField
            fieldSchema = resultSchema
            break
          }
        }
      }
    (field, path, fieldSchema)
  }

  def getPath(field: Field,
              virtual: Boolean = false): (ListBuffer[AvroPath], Schema) = {
    val path = ListBuffer[AvroPath]()
    val name = field name()
    if (field isArray) {
      if (field.arrayItemType == RECORD) {
        path += AvroPath(name, ARRAY, schemaPath ++ path.reverse, virtual)
        return (path, field arraySchema)
      } else if (!field.isPrimitiveArray)
        System.err.println(
          s"WARNING: 1 - Unknown type ${field arraySchema} for $name")
    } else if (field isRecord) {
      path += AvroPath(name, RECORD, schemaPath ++ path.reverse, virtual)
    } else if (!field.isPrimitive) {
      System.err.println(
        s"WARNING: 2 - Unknown type ${field.fieldType} for $name")
    }
    (path, field fieldSchema)
  }

  def updatePath(field: Field, virtual: Boolean = false): Unit = {
    val name = field name()
    if (field isArray) {
      if (field.arrayItemType == RECORD) {
        schemaPath += AvroPath(name, ARRAY, schemaPath, virtual)
        lastSchema = field.arraySchema
      } else if (!field.isPrimitiveArray)
        System.err.println(
          s"WARNING: 1 - Unknown type ${field.arraySchema} for $name")
    } else if (field isRecord) {
      schemaPath += AvroPath(name, RECORD, schemaPath, virtual)
      lastSchema = field.fieldSchema
    } else if (!field.isPrimitive)
      System.err.println(
        s"WARNING: 2 - Unknown type ${field.fieldType} for $name")
  }

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
    cal = DatatypeConverter.parseDateTime(text)
    cal.setTimeZone(timeZone)
    cal.getTimeInMillis
  }
}

object Utils {
  private val timeMap = mutable.Map[String, Long]()
  private val cal = Calendar.getInstance()

  def option(text: String): Option[String] = {
    if (Option(text) isDefined)
      if (text.trim == "") None else Option(text)
    else None
  }

  def supplied(obj: AnyRef, name: String): Boolean = if (Option(obj) isEmpty) throw ConversionError(s"$name is required") else true

  def startTimer(tag: String): Unit =
    timeMap += tag -> Calendar.getInstance().getTimeInMillis

  def endTimer(tag: String): Unit = {
    val endTime = Calendar.getInstance().getTimeInMillis
    val startTime = timeMap(tag)
    System.err.println(s"$tag took: ${(endTime - startTime) / 1000.0} seconds")
    timeMap.remove(tag)
  }

  def profile(tag: String)(func: => Unit): Unit = {
    val start = cal.getTimeInMillis
    func
    val end = cal.getTimeInMillis
    System.err.println(s"$tag took: ${end - start} seconds")
  }
}
