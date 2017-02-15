package in.dreamlabs.xmlavro

import in.dreamlabs.xmlavro.RichAvro._
import in.dreamlabs.xmlavro.Utils._
import org.apache.avro.Schema
import org.apache.avro.Schema.Type._
import org.apache.avro.Schema.{Field, Type}
import org.apache.avro.generic.GenericData.Record

import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks.{break, breakable}

/**
  * Created by Royce on 13/02/2017.
  */
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
    eleStack.clear()
    schemaPath.clear()
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
        val message = s"Element ${builder.stripSuffix("/")} is not found in Schema"
        if (ignoreMissing && !suppressWarnings)
          warn(message)
        else if (!ignoreMissing)
          throw ConversionError(message)
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
        warn(s"1 - Unknown type ${field.arraySchema} for $name")
    } else if (field isRecord) {
      schemaPath += AvroPath(name, RECORD, schemaPath, virtual)
      lastSchema = field.fieldSchema
    } else if (!field.isPrimitive)
      warn(s"2 - Unknown type ${field.fieldType} for $name")
  }
}

