package in.dreamlabs.xmlavro

import java.util

import in.dreamlabs.xmlavro.RichAvro.{caseSensitive, ignoreMissing}
import org.apache.avro.Schema
import org.apache.avro.Schema.Type._
import org.apache.avro.Schema.{Field, Type}
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericData.Record

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.control.Breaks._

/**
  * Created by Royce on 26/01/2017.
  */
trait RichAvro {

  implicit class RichRecord(record: Record) {
    def at(path: List[AvroPath]): Record = {
      var resultRecord = record
      path.foreach { path =>
        if (path.pathType == ARRAY) {
          var array =
            resultRecord.get(path name).asInstanceOf[util.List[AnyRef]]
          if (array == null || array.size() - 1 < path.index) {
            val arraySchema =
              resultRecord.getSchema.getField(path name).arraySchema
            if (array == null) {
              array = new util.ArrayList[AnyRef]()
              resultRecord.put(path name, array)
            }
            resultRecord = arraySchema.newRecord
            array.add(resultRecord)
          } else
            resultRecord = array.get(path index).asInstanceOf[Record]
        } else {
          val tempSchema =
            resultRecord.getSchema.getField(path name).fieldSchema
          var tempRecord = resultRecord.get(path name).asInstanceOf[Record]
          if (tempRecord == null) {
            tempRecord = tempSchema.newRecord
            resultRecord.put(path name, tempRecord)
          }
          resultRecord = tempRecord
        }
      }
      resultRecord
    }

    def add(node: XNode, value: String): Unit = {
      val schema = record.getSchema
      var fieldOp = schema field node
      var wildcard = false

      if (fieldOp isEmpty) {
        fieldOp = schema wildcard (node attribute)
        if (fieldOp isDefined) wildcard = true
        else {
          val message =
            s"${if (node attribute) "Attribute" else "Element"} ${node name} not found in Schema (even as a wildcard)"
          if (ignoreMissing)
            System.err.println(s"WARNING: $message")
          else
            throw ConversionError(message)
        }
      }
      if (fieldOp isDefined) {
        val field = fieldOp.get

        if (wildcard) {
          val wildField =
            record.get(field name).asInstanceOf[util.Map[String, AnyRef]]
          wildField.put(node name, value)
        } else {
          if (field isArray) {
            var array = record.get(field name).asInstanceOf[util.List[AnyRef]]
            if (array == null) {
              array = new util.ArrayList[AnyRef]()
              record.put(field name, array)
            }
            array.add(AvroUtils.createValue(field arrayItemType, value))
          } else
            record.put(field name,
              AvroUtils.createValue(field fieldType, value))
        }
      }
    }
  }

  implicit class RichSchema(schema: Schema) {
    val PRIMITIVES: List[Type] =
      List(STRING, INT, LONG, FLOAT, DOUBLE, BOOLEAN, NULL)

    def wildcard(attribute: Boolean): Option[Field] =
      field(XNode.wildNode(attribute))

    def field(node: XNode): Option[Field] = {
      var resultField: Option[Field] = None
      breakable {
        schema.simplify.getFields.forEach { field =>
          val sourceField = field.getProp(XNode.SOURCE)
          if (node sourceMatches(sourceField, caseSensitive)) {
            resultField = Some(field)
            break
          }
        }
      }
      if (resultField isEmpty)
        resultField = Option(schema.simplify.getField(XNode.TEXT_VALUE))
      resultField
    }

    def simplify: Schema =
      if (schema.getType == UNION) schema.getTypes.get(1) else schema

    def customTypeFields(): mutable.Buffer[Field] =
      schema.simplify.getFields.asScala.filter(_.name.matches("type\\d+"))

    def deepSchema: Schema = schema getType match {
      case UNION => schema.getTypes.get(1)
      case ARRAY =>
        val itemType = schema getElementType()
        if (itemType.getType == UNION)
          itemType.getTypes.get(1)
        else
          itemType
      case _ => schema
    }

    def isArray: Boolean = schema.getType == ARRAY

    def isRecord: Boolean = schema.getType == RECORD

    def isPrimitive: Boolean = PRIMITIVES.contains(schemaType)

    def schemaType: Type = schema.getType

    def arraySchema: Schema = schema.getElementType

    def isPrimitiveArray: Boolean = PRIMITIVES contains arrayItemType

    def arrayItemType: Type = schema.getElementType.getType

    def newRecord: Record = {
      val record = new GenericData.Record(schema)
      for (field <- record.getSchema.getFields.asScala) {
        if (field isArray)
          record.put(field.name, new util.ArrayList[AnyRef]())
        if (field.name == XNode.WILDCARD)
          record.put(field.name, new util.HashMap[String, AnyRef]())
      }
      record
    }

  }

  implicit class RichField(field: Field) {

    def fieldType: Type = fieldSchema.getType

    def isArray: Boolean = fieldSchema.isArray

    def fieldSchema: Schema = field.schema().simplify

    def isRecord: Boolean = fieldSchema.isRecord

    def isPrimitive: Boolean = fieldSchema.isPrimitive

    def arraySchema: Schema = fieldSchema.arraySchema

    def arrayItemType: Type = fieldSchema.arrayItemType

    def isPrimitiveArray: Boolean = fieldSchema.isPrimitiveArray

  }

}

object RichAvro extends RichAvro {
  var ignoreMissing = false
  var caseSensitive = true
}
