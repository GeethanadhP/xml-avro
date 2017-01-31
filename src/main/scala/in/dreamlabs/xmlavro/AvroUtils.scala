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
import scala.collection.JavaConverters._
/**
  * Created by Royce on 28/01/2017.
  */
class AvroPath(val name: String, val pathType: Type, currentPath: ListBuffer[AvroPath], val virtual: Boolean = false) {
  private val innerName = StringBuilder.newBuilder
  innerName append s"$name"
  currentPath.foreach(path => innerName append path.toString)
  val index: Int =
    if (countsMap contains innerName.mkString) {
      var currentIndex = countsMap(innerName.mkString)
      currentIndex += 1
      countsMap += (innerName.mkString -> currentIndex)
      currentIndex
    }
    else {
      countsMap += (innerName.mkString -> 0)
      0
    }

  def destroy(): Unit = {
    var currentIndex = countsMap(innerName.mkString)
    currentIndex -= 1
    countsMap += (innerName.mkString -> currentIndex)
  }

  override def toString: String = if (pathType == ARRAY) s"$name[$index]" else name
}


object AvroPath {
  def apply(name: String, pathType: Type, currentPath: ListBuffer[AvroPath], virtual: Boolean = false) = new AvroPath(name, pathType, currentPath, virtual)

  val countsMap: mutable.Map[String, Int] = mutable.Map[String, Int]()

}


object XMLEvents {
  val PRIMITIVES: List[Type] =
    List(STRING, INT, LONG, FLOAT, DOUBLE, BOOLEAN, NULL)

  var rootSchema: Schema = _
  var rootRecord: Record = _
  private var lastSchema = rootSchema
  val eleStack: ListBuffer[String] = ListBuffer[String]()
  val schemaPath: ListBuffer[AvroPath] = ListBuffer[AvroPath]()

  def setSchema(schema: Schema, record: Record): Unit = {
    rootSchema = schema
    rootRecord = record
    lastSchema = rootSchema
  }

  def addElement(name: String): Boolean = {
    eleStack.insert(0, name)
    var found = false
    if (eleStack.size != 1) {
      val (field, path, schema) = findField(lastSchema, name)
      if (field != null) {
        schemaPath ++= path.reverse
        updatePath(field)
        found = true
      } else {
        val builder = StringBuilder.newBuilder
        eleStack.reverse.foreach(ele => builder append s"$ele/")
        System.err.println(s"WARNING: Element ${builder mkString} is not found in Schema")
      }
    } else found = true
    found
  }

  private def destroyLastPath(): Int = {
    val tempPath = schemaPath.last
    schemaPath -= tempPath
    //    tempPath destroy()
    schemaPath size
  }

  def removeElement(name: String): Unit = {
    eleStack.remove(0)
    var count = schemaPath.size
    if (count != 0) {
      if (schemaPath.last.name == name) {
        count = destroyLastPath()
        while (count != 0 && schemaPath.last.virtual) {
          count = destroyLastPath()
        }
      }
      else if (schemaPath.last.name.startsWith("type"))
        while (count != 0 && schemaPath.last.virtual) {
          count = destroyLastPath()
        }
      lastSchema = rootRecord.at(schemaPath.toList).getSchema
    }
  }

  def findField(schema: Schema, name: String): (Field, ListBuffer[AvroPath], Schema) = {
    var fieldSchema = AvroUtils.getSchema(schema)
    var field = fieldSchema.getField(name)
    val path = ListBuffer[AvroPath]()
    if (field == null)
      breakable {
        for (typeField <- fieldSchema.getFields.asScala){
          if (typeField name() startsWith "type") {
            val (resultField, resultPath, resultSchema) = findField(AvroUtils.getSchema(typeField), name)
            if (resultField != null) {
              val (tempPath, tempSchema) = getPath(typeField, virtual = true)
              resultPath ++= tempPath
              path ++= resultPath
              field = resultField
              fieldSchema = resultSchema
              break
            }
          }
        }
      }
    (field, path, fieldSchema)
  }

  def getPath(field: Field, virtual: Boolean = false): (ListBuffer[AvroPath], Schema) = {
    var fieldSchema = AvroUtils.getSchema(field)
    val path = ListBuffer[AvroPath]()
    val name = field name()
    fieldSchema getType match {
      case ARRAY =>
        val itemType = fieldSchema getElementType()
        if (itemType.getType == RECORD) {
          path += AvroPath(name, ARRAY, schemaPath ++ path.reverse, virtual)
          fieldSchema = itemType
        } else if (!PRIMITIVES.contains(itemType.getType))
          System.err.println(s"WARNING: 1 - Unknown type $itemType for $name")
      case RECORD =>
        path += AvroPath(name, RECORD, schemaPath ++ path.reverse, virtual)
      case other => if (!PRIMITIVES.contains(other)) System.err.println(s"WARNING: 2 - Unknown type $other for $name")
    }
    (path, fieldSchema)
  }

  def updatePath(field: Field, virtual: Boolean = false): Unit = {
    val fieldSchema = AvroUtils.getSchema(field)
    val name = field name()
    fieldSchema getType match {
      case ARRAY =>
        val itemType = fieldSchema getElementType()
        if (itemType.getType == RECORD) {
          schemaPath += AvroPath(name, ARRAY, schemaPath, virtual)
          lastSchema = itemType
        } else if (!PRIMITIVES.contains(itemType.getType))
          System.err.println(s"WARNING: 1 - Unknown type $itemType for $name")
      case RECORD =>
        schemaPath += AvroPath(name, RECORD, schemaPath, virtual)
        lastSchema = fieldSchema
      case other => if (!PRIMITIVES.contains(other)) System.err.println(s"WARNING: 2 - Unknown type $other for $name")
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

  def getSchema(schema: Schema): Schema = {
    schema getType match {
      case UNION => schema.getTypes.get(1)
      case ARRAY =>
        val itemType = schema getElementType()
        if (itemType.getType == UNION)
          itemType.getTypes.get(1)
        else
          itemType
      case other => schema
    }
  }

  def createRecord(schema: Schema): Record = {
    val record = new GenericData.Record(schema)
    import scala.collection.JavaConverters._
    for (field <- record.getSchema.getFields.asScala) {
      if (field.schema().getType == ARRAY)
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