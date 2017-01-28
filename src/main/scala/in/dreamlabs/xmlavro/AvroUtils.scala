package in.dreamlabs.xmlavro

import java.util
import java.util.TimeZone
import javax.xml.bind.DatatypeConverter

import in.dreamlabs.xmlavro.AvroPath.countsMap
import in.dreamlabs.xmlavro.Implicits.RichRecord
import org.apache.avro.Schema
import org.apache.avro.Schema.Type._
import org.apache.avro.Schema.{Field, Type}
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericData.Record

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks._

/**
  * Created by Royce on 28/01/2017.
  */
class AvroPath(val name: String, val pathType: Type, val level: Int, val virtual: Boolean = false, create: Boolean = true) {
  private val innerName = s"$name@$level"
  val index: Int =
    if (countsMap contains innerName)
      if (create) {
        var currentIndex = countsMap(innerName)
        currentIndex += 1
        countsMap += (innerName -> currentIndex)
        currentIndex
      }
      else countsMap(innerName)
    else {
      countsMap += (innerName -> 0)
      0
    }

  override def toString: String = if (pathType == ARRAY) s"$name[$index]" else name
}


object AvroPath {
  def apply(name: String, pathType: Type, level: Int, virtual: Boolean = false) = new AvroPath(name, pathType, level, virtual)

  val countsMap: mutable.Map[String, Int] = mutable.Map[String, Int]()

}


object XMLEvents {
  val PRIMITIVES: List[Type] =
    List(STRING, INT, LONG, FLOAT, DOUBLE, BOOLEAN, NULL)

  var rootSchema: Schema = _
  private var lastSchema = rootSchema
  val eleStack: ListBuffer[String] = ListBuffer[String]()
  val schemaPath: ListBuffer[AvroPath] = ListBuffer[AvroPath]()

  def setSchema(schema: Schema): Unit = {
    rootSchema = schema
    lastSchema = rootSchema
  }

  def addElement(name: String): Unit = {
    eleStack.insert(0, name)
    if(name == "alias")
      println(name)
    if (eleStack.size != 1) {
      val field = findField(lastSchema, name)
      updatePath(field)
    }
  }

  def findField(schema: Schema, name: String): Field = {
    val field = schema.getField(name)
    if (field == null)
      breakable {
        schema.getFields.forEach {
          typeField =>
            if (typeField name() startsWith "type") {
              val innerField = findField(AvroUtils.getSchema(typeField), name)
              updatePath(typeField, virtual = true)
            }
        }
      }
    field
  }

  def updatePath(field: Field, virtual: Boolean = false): Unit = {
    val fieldSchema = AvroUtils.getSchema(field)
    val name = field name()
    fieldSchema getType match {
      case ARRAY =>
        val itemType = fieldSchema getElementType()
        if (itemType.getType == RECORD) {
          schemaPath += AvroPath(name, ARRAY, eleStack.size, virtual)
          lastSchema = itemType
        } else if (!PRIMITIVES.contains(itemType.getType))
          System.err.println(s"WARNING: 1 - Unknown type $itemType for $name")
      case RECORD =>
        schemaPath += AvroPath(name, RECORD, eleStack.size, virtual)
        lastSchema = fieldSchema
      case other => if (!PRIMITIVES.contains(other)) System.err.println(s"WARNING: 2 - Unknown type $other for $name")
    }
  }

  def removeElement(name: String): Unit = {
    eleStack.remove(0)
    val count = schemaPath.size
    if (count != 0 && schemaPath.last.name == name) {
      schemaPath.remove(count - 1)
      lastSchema = AvroUtils.createRecord(rootSchema).at(schemaPath.toList).getSchema
    }
  }

  def option(text: String): Option[String] = {
    if (text.trim == "") None else Option(text)
  }
}

object AvroUtils {
  private val TIMESTAMP_PATTERN =
    "^(\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.*\\d*)Z?$"

  var timeZone: TimeZone = TimeZone.getTimeZone("UTC-0")

  def getSchema(field: Field): Schema = {
    var fieldSchema = field schema()
    if (fieldSchema.getType == UNION)
      fieldSchema = fieldSchema.getTypes.get(1)
    fieldSchema
  }

  def createRecord(schema: Schema): Record = {
    val record = new GenericData.Record(schema)
    import scala.collection.JavaConverters._
    for (field <- record.getSchema.getFields.asScala) {
      if (getSchema(field).getType == ARRAY)
        record.put(field.name, new util.ArrayList[AnyRef]())
      if (field.name == Source.WILDCARD)
        record.put(field.name, new util.HashMap[String, AnyRef]())
    }
    record
  }

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
      case other => throw XMLException(s"Unsupported type $other")
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