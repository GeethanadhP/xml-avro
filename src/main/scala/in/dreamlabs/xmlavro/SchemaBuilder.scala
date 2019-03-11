package in.dreamlabs.xmlavro

import java.io.IOException

import in.dreamlabs.xmlavro.config.XSDConfig
import javax.xml.XMLConstants
import org.apache.avro.Schema
import org.apache.avro.Schema.Field
import org.apache.xerces.dom.DOMInputImpl
import org.apache.xerces.impl.Constants
import org.apache.xerces.impl.xs.{SchemaGrammar, XMLSchemaLoader, XSComplexTypeDecl}
import org.apache.xerces.xni.parser.{XMLEntityResolver, XMLInputSource}
import org.apache.xerces.xni.{XMLResourceIdentifier, XNIException}
import org.apache.xerces.xs.XSConstants.{ATTRIBUTE_DECLARATION, ELEMENT_DECLARATION, MODEL_GROUP, WILDCARD}
import org.apache.xerces.xs.XSTypeDefinition.{COMPLEX_TYPE, SIMPLE_TYPE}
import org.apache.xerces.xs._
import org.codehaus.jackson.JsonNode
import org.codehaus.jackson.node.NullNode

import scala.collection.JavaConverters._
import scala.collection.immutable.HashMap
import scala.collection.mutable
import scala.reflect.io.Path

/**
  * Created by Royce on 20/01/2017.
  */
final class SchemaBuilder(config: XSDConfig) {
  private val debug = config.debug
  XNode.namespaces = config.namespaces
  private val baseDir = config.baseDir
  private val stringTimestamp = config.stringTimestamp
  private val rebuildChoice = config.rebuildChoice
  private val ignoreHiveKeywords = config.ignoreHiveKeywords
  private val rootElementQName = config.rootElementQName
  private val xsdFile = config.xsdFile
  private val avscFile = config.avscFile
  private val schemas = mutable.Map[String, Schema]()
  private var typeCount = -1
  private var typeLevel = 0

  if (stringTimestamp)
    SchemaBuilder.PRIMITIVES += XSConstants.DATETIME_DT -> Schema.Type.STRING

  def createSchema(): Unit = {
    val errorHandler = new XSDErrorHandler
    val xsdIn = xsdFile.toFile.bufferedInput
    val model = {
      val schemaInput = new DOMInputImpl()
      schemaInput.setByteStream(xsdIn)
      val loader = new XMLSchemaLoader
      if (baseDir isDefined)
        loader.setEntityResolver(new SchemaResolver(baseDir.get))
      loader.setErrorHandler(errorHandler)
      loader.setParameter(Constants.DOM_ERROR_HANDLER, errorHandler)
      loader load schemaInput
    }
    xsdIn.close()
    errorHandler check()

    // Generate schema for all the elements
    val schema = {
      val tempSchemas = mutable.LinkedHashMap[XSObject, Schema]()
      val elements: XSNamedMap =
        model.getComponents(XSConstants ELEMENT_DECLARATION)
      val rootElements =
        if (rootElementQName isDefined) {
          val rootElement = Option(elements.itemByName(rootElementQName.get.getNamespaceURI match {
            case XMLConstants.NULL_NS_URI => null;
            case ns: String => ns
          }, rootElementQName.get.getLocalPart))
          if (rootElement isEmpty)
            throw new NoSuchElementException(s"The schema contains no root level element definition for QName '${rootElementQName.get}'")
          HashMap("1" -> rootElement.get)
        }
        else elements.asScala

      for ((_, ele: XSElementDeclaration) <- rootElements) {
        debug(s"Processing root element ${XNode(ele) toString}")
        tempSchemas += ele -> processType(ele.getTypeDefinition,
          optional = false,
          array = false)
      }

      if (tempSchemas isEmpty)
        throw ConversionException("No root element declaration found")

      // Create root record from the schemas generated
      if (tempSchemas.size == 1) tempSchemas.valuesIterator.next()
      else {
        val nullSchema = Schema.create(Schema.Type.NULL)
        val fields = mutable.ListBuffer[Field]()
        for ((ele, record) <- tempSchemas) {
          val optionalSchema = Schema createUnion List[Schema](nullSchema,
            record).asJava
          val field =
            new Field(validName(ele.getName).get, optionalSchema, null, null)
          field.addProp(XNode.SOURCE, XNode(ele).source)
          fields += field
        }
        val record = Schema.createRecord(generateTypeName, null, null, false)
        record.setFields(fields.asJava)
        record.addProp(XNode.SOURCE, XNode.DOCUMENT)
        record
      }
    }

    // Write the schema output
    val avscOut = avscFile.toFile.bufferedOutput()
    avscOut write schema.toString(true).getBytes()
    avscOut close()
  }

  private def processType(eleType: XSTypeDefinition,
                          optional: Boolean,
                          array: Boolean): Schema = {
    typeLevel += 1
    var schema = eleType.getTypeCategory match {
      case SIMPLE_TYPE => Schema.create(primaryType(eleType))
      case COMPLEX_TYPE =>
        val name = complexTypeName(eleType)
        debug(s"Creating schema for type $name")
        val tempSchema = schemas.getOrElse(name, createRecord(name, eleType))
        debug(s"Created schema for type $name")
        tempSchema
      case others =>
        throw ConversionException(s"Unknown Element type: $others")
    }
    if (array)
      schema = Schema createArray schema
    if (optional) {
      val nullSchema = Schema create Schema.Type.NULL
      schema = Schema createUnion List[Schema](nullSchema, schema).asJava
    }
    typeLevel -= 1
    schema
  }

  private def processGroup(
                            term: XSTerm,
                            innerOptional: Boolean = false,
                            array: Boolean = false): mutable.Map[String, Field] = {
    val fields = mutable.LinkedHashMap[String, Field]()
    val group = term.asInstanceOf[XSModelGroup]
    group.getCompositor match {
      case XSModelGroup.COMPOSITOR_CHOICE =>
        if (rebuildChoice)
          fields ++= processGroupParticle(group,
            innerOptional = true,
            innerArray = array)
        else if (!array)
          fields ++= processGroupParticle(group,
            innerOptional = true,
            innerArray = false)
        else {
          val name = generateTypeName
          val groupRecord = createRecord(generateTypeName, group)
          fields += (name -> new Field(name,
            Schema.createArray(groupRecord),
            null,
            null))
        }
      case XSModelGroup.COMPOSITOR_SEQUENCE | XSModelGroup.COMPOSITOR_ALL =>
        if (!array)
          fields ++= processGroupParticle(group,
            innerOptional,
            innerArray = false)
        else {
          val name = generateTypeName
          val groupRecord = createRecord(generateTypeName, group)
          fields += (name -> new Field(name,
            Schema.createArray(groupRecord),
            null,
            null))
        }

    }
  }

  private def processGroupParticle(
                                    group: XSModelGroup,
                                    innerOptional: Boolean,
                                    innerArray: Boolean): mutable.Map[String, Field] = {
    val fields = mutable.LinkedHashMap[String, Field]()
    for (particle <- group.getParticles.asScala.map(
      _.asInstanceOf[XSParticle])) {
      val optional = innerOptional || particle.getMinOccurs == 0
      val array = innerArray || particle.getMaxOccurs > 1 || particle.getMaxOccursUnbounded
      val innerTerm = particle.getTerm
      innerTerm.getType match {
        case ELEMENT_DECLARATION | WILDCARD =>
          val field = createField(innerTerm, optional, array)
          fields += (field.name() -> field)
        case MODEL_GROUP =>
          fields ++= processGroup(innerTerm, optional, array)
        case _ =>
          throw ConversionException(s"Unsupported term type ${group.getType}")
      }
    }
    fields
  }

  private def processAttributes(
                                 complexType: XSComplexTypeDefinition): mutable.Map[String, Field] = {
    val fields = mutable.LinkedHashMap[String, Field]()

    // Process normal attributes
    val attributes = complexType.getAttributeUses
    for (attribute <- attributes.asScala.map(_.asInstanceOf[XSAttributeUse])) {
      val optional = !attribute.getRequired
      val field = createField(attribute.getAttrDeclaration, optional)
      fields += (field.name() -> field)
    }

    // Process wildcard attribute
    val wildcard = Option(complexType.getAttributeWildcard)
    if (wildcard isDefined) {
      val field = createField(wildcard.get, optional = true)
      fields += (field.name() -> field)
    }
    fields
  }

  private def processExtension(
                                complexType: XSComplexTypeDefinition): mutable.Map[String, Field] = {
    val fields = mutable.LinkedHashMap[String, Field]()

    if (complexType derivedFromType(SchemaGrammar.fAnySimpleType, XSConstants.DERIVATION_EXTENSION)) {
      var extnType = complexType
      while (extnType.getBaseType.getTypeCategory == XSTypeDefinition.COMPLEX_TYPE) extnType =
        extnType.getBaseType.asInstanceOf[XSComplexTypeDefinition]
      val fieldSchema =
        processType(extnType.getBaseType, optional = true, array = false)
      val field = new Field(XNode.TEXT_VALUE, fieldSchema, null, null)
      field.addProp(XNode.SOURCE, XNode.textNode.source)
      fields += (field.name() -> field)
    }
    fields
  }

  private def processParticle(
                               complexType: XSComplexTypeDefinition): mutable.Map[String, Field] = {
    val fields = mutable.LinkedHashMap[String, Field]()
    val particle = Option(complexType.getParticle)
    if (particle isDefined)
      fields ++= processGroup(particle.get.getTerm)
    fields
  }

  // Create a list of avsc fields from the element (attributes, text, inner groups)
  private def createFields(typeDef: XSObject): List[Field] = {
    val fields = mutable.LinkedHashMap[String, Field]()
    typeDef match {
      case complexType: XSComplexTypeDefinition =>
        updateFields(fields, processExtension(complexType))
        updateFields(fields, processParticle(complexType))
        updateFields(fields, processAttributes(complexType))
      case complexType: XSTerm =>
        updateFields(fields, processGroup(complexType))
    }
    fields.values.toList
  }

  // Checks if the new set of fields are already existing and generates a new name for duplicate fields
  private def updateFields(originalFields: mutable.Map[String, Field],
                           newField: mutable.Map[String, Field]): Unit = {
    //TODO make it unique
    for ((key, field) <- newField) {
      if (key != "others" && (originalFields contains key)) {
        val newKey = (field getProp "source").split(" ")(0) + "_" + key
        val tempField: Field = new Field(validName(newKey).get, field.schema(), field.doc(), field.defaultValue())
        for ((name, json) <- field.getJsonProps.asScala)
          tempField.addProp(name, json)
        //        field.addProp(XNode.SOURCE, XNode(ele, attribute).source)
        originalFields += newKey -> tempField
      } else {
        originalFields += key -> field
      }
    }
  }

  // Create field for an element
  private def createField(ele: XSObject,
                          optional: Boolean,
                          array: Boolean = false): Schema.Field = {
    val field: Field = ele.getType match {
      case ELEMENT_DECLARATION | ATTRIBUTE_DECLARATION =>
        val (eleType, attribute) = ele.getType match {
          case ELEMENT_DECLARATION =>
            (ele.asInstanceOf[XSElementDeclaration].getTypeDefinition, false)
          case ATTRIBUTE_DECLARATION =>
            (ele.asInstanceOf[XSAttributeDeclaration].getTypeDefinition, true)
        }

        val fieldSchema: Schema = processType(eleType, optional, array)
        val defaultValue: JsonNode = if (optional) NullNode.getInstance() else null

        val field: Field =
          new Field(validName(ele.getName).get, fieldSchema, null, defaultValue)

        field.addProp(XNode.SOURCE, XNode(ele, attribute).source)

        if (eleType.getTypeCategory == SIMPLE_TYPE) {
          val tempType =
            eleType.asInstanceOf[XSSimpleTypeDefinition].getBuiltInKind
          if (tempType == XSConstants.DATETIME_DT && !stringTimestamp)
            field.addProp("comment", "timestamp")
        }
        field
      case WILDCARD =>
        val map = Schema.createMap(Schema.create(Schema.Type.STRING))
        new Field(XNode.WILDCARD, map, null, null)
      case _ =>
        throw ConversionException("Invalid source object type " + ele.getType)
    }
    field
  }

  // Create record for an element
  private def createRecord(name: String, eleType: XSObject): Schema = {
    val schema = Schema.createRecord(name, null, null, false)
    schemas += (name -> schema)
    schema setFields createFields(eleType).asJava
    schema
  }

  private def complexTypeName(eleType: XSTypeDefinition): String = {
    val name = validName(eleType.asInstanceOf[XSComplexTypeDecl].getTypeName)
    if (name isDefined) name.get
    else generateTypeName
  }

  private def generateTypeName: String = {
    typeCount += 1
    "type" + typeCount
  }

  private def validName(name: String): Option[String] = {
    val sourceName = Option(name)
    val finalName: Option[String] =
      if (sourceName isEmpty) None
      else {
        val chars = sourceName.get.toCharArray
        val result = new Array[Char](chars.length)
        var p = 0
        // Remove invalid characters, replace . or - with _
        for (c <- chars) {
          val valid = c >= 'a' && c <= 'z' || c >= 'A' && c <= 'z' || c >= '0' && c <= '9' || c == '_'
          val separator = c == '.' || c == '-'
          if (valid) {
            result(p) = c
            p += 1
          } else if (separator) {
            result(p) = '_'
            p += 1
          }
        }
        var finalName = new String(result, 0, p)

        try {
          // handle built-in types
          Schema.Type.valueOf(finalName.toUpperCase)
          finalName = finalName + "_value"
        } catch {
          case _: IllegalArgumentException =>
        }
        // Handle hive keywords
        if (!ignoreHiveKeywords && SchemaBuilder.HIVE_KEYWORDS.contains(finalName.toUpperCase))
          finalName = finalName + "_value"
        Option(finalName)
      }
    finalName
  }

  private def primaryType(schemaType: XSTypeDefinition): Schema.Type = {
    val avroType = SchemaBuilder.PRIMITIVES get schemaType
      .asInstanceOf[XSSimpleTypeDefinition]
      .getBuiltInKind
    if (avroType isEmpty) Schema.Type.STRING
    else avroType.get
  }

  private def debug(message: String): Unit =
    if (debug) Utils.info(s"${"-" * typeLevel * 4} $message")

  /** Read the referenced XSD as per name specified */
  private class SchemaResolver(private val baseDir: Path)
    extends XMLEntityResolver {
    System.setProperty("user.dir", baseDir.toAbsolute.path)

    @throws[XNIException]
    @throws[IOException]
    def resolveEntity(id: XMLResourceIdentifier): XMLInputSource = {
      val fileName = id.getLiteralSystemId
      val path = Path(fileName).toAbsoluteWithRoot(baseDir)
      debug(s"Resolving $fileName")
      val source = new XMLInputSource(id)
      if (path.exists)
        source.setByteStream(path.toFile.bufferedInput())
      source
    }
  }

}

object SchemaBuilder {
  var PRIMITIVES: Map[Short, Schema.Type] = Map(
    XSConstants.BOOLEAN_DT -> Schema.Type.BOOLEAN,
    XSConstants.INT_DT -> Schema.Type.INT,
    XSConstants.BYTE_DT -> Schema.Type.INT,
    XSConstants.SHORT_DT -> Schema.Type.INT,
    XSConstants.UNSIGNEDBYTE_DT -> Schema.Type.INT,
    XSConstants.UNSIGNEDSHORT_DT -> Schema.Type.INT,
    XSConstants.INTEGER_DT -> Schema.Type.STRING,
    XSConstants.NEGATIVEINTEGER_DT -> Schema.Type.STRING,
    XSConstants.NONNEGATIVEINTEGER_DT -> Schema.Type.STRING,
    XSConstants.POSITIVEINTEGER_DT -> Schema.Type.STRING,
    XSConstants.NONPOSITIVEINTEGER_DT -> Schema.Type.STRING,
    XSConstants.LONG_DT -> Schema.Type.LONG,
    XSConstants.UNSIGNEDINT_DT -> Schema.Type.LONG,
    XSConstants.FLOAT_DT -> Schema.Type.FLOAT,
    XSConstants.DOUBLE_DT -> Schema.Type.DOUBLE,
    XSConstants.DECIMAL_DT -> Schema.Type.DOUBLE,
    XSConstants.DATETIME_DT -> Schema.Type.LONG,
    XSConstants.STRING_DT -> Schema.Type.STRING
  )

  val HIVE_KEYWORDS: List[String] =
    List(
      "ALL",
      "ALTER",
      "AND",
      "ARRAY",
      "AS",
      "AUTHORIZATION",
      "BETWEEN",
      "BIGINT",
      "BINARY",
      "BOOLEAN",
      "BOTH",
      "BY",
      "CASE",
      "CAST",
      "CHAR",
      "COLUMN",
      "COLUMNS",
      "CONF",
      "CREATE",
      "CROSS",
      "CUBE",
      "CURRENT",
      "CURRENT_DATE",
      "CURRENT_TIMESTAMP",
      "CURSOR",
      "DATABASE",
      "DATE",
      "DATETIME",
      "DECIMAL",
      "DELETE",
      "DESCRIBE",
      "DISTINCT",
      "DOUBLE",
      "DROP",
      "ELSE",
      "END",
      "EXCHANGE",
      "EXISTS",
      "EXTENDED",
      "EXTERNAL",
      "FALSE",
      "FETCH",
      "FLOAT",
      "FOLLOWING",
      "FOR",
      "FROM",
      "FULL",
      "FUNCTION",
      "GRANT",
      "GROUP",
      "GROUPING",
      "HAVING",
      "IF",
      "IMPORT",
      "IN",
      "INNER",
      "INSERT",
      "INT",
      "INTERSECT",
      "INTERVAL",
      "INTO",
      "IS",
      "JOIN",
      "LATERAL",
      "LEFT",
      "LESS",
      "LIKE",
      "LOCAL",
      "MACRO",
      "MAP",
      "MORE",
      "NONE",
      "NOT",
      "NULL",
      "OF",
      "ON",
      "OR",
      "ORDER",
      "OUT",
      "OUTER",
      "OVER",
      "PARTIALSCAN",
      "PARTITION",
      "PERCENT",
      "PRECEDING",
      "PRESERVE",
      "PROCEDURE",
      "RANGE",
      "READS",
      "REDUCE",
      "REVOKE",
      "RIGHT",
      "ROLLUP",
      "ROW",
      "ROWS",
      "SELECT",
      "SET",
      "SMALLINT",
      "TABLE",
      "TABLESAMPLE",
      "THEN",
      "TIMESTAMP",
      "TO",
      "TRANSFORM",
      "TRIGGER",
      "TRUE",
      "TRUNCATE",
      "UNBOUNDED",
      "UNION",
      "UNIQUEJOIN",
      "UPDATE",
      "USER",
      "USING",
      "UTC_TMESTAMP",
      "VALUES",
      "VARCHAR",
      "WHEN",
      "WHERE",
      "WINDOW",
      "WITH"
    )

  def apply(config: XSDConfig) = new SchemaBuilder(config)
}
