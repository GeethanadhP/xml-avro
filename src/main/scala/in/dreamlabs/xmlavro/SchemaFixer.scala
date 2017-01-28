package in.dreamlabs.xmlavro

import java.io.IOException

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

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.reflect.io.Path

/**
  * Created by Royce on 20/01/2017.
  */
class SchemaFixer(rootFile: Path, baseDir: Option[Path] = None) {
  var debug = false
  var rebuildChoice = false
  private var typeCount = -1
  private var typeLevel = 0
  private val schemas = mutable.Map[String, Schema]()

  val schema: Schema = {
    val schemaInput = new DOMInputImpl()
    schemaInput.setByteStream(rootFile.toFile.bufferedInput)
    val loader = new XMLSchemaLoader
    if (baseDir isDefined)
      loader.setEntityResolver(new SchemaResolver(baseDir.get))
    val errorHandler = new ErrorHandler
    loader.setErrorHandler(errorHandler)
    loader.setParameter(Constants.DOM_ERROR_HANDLER, errorHandler)
    val model = loader load schemaInput
    errorHandler check()

    // Generate schema for all the elements
    val tempSchemas = mutable.LinkedHashMap[Source, Schema]()
    val elements: XSNamedMap =
      model.getComponents(XSConstants ELEMENT_DECLARATION)
    for ((_, ele: XSElementDeclaration) <- elements.asScala) {
      debug(
        "Processing root element " + ele.getName + "{" + ele.getNamespace + "}")
      tempSchemas += (Source(ele.getName) -> processType(ele.getTypeDefinition,
        optional = false,
        array = false))
    }

    if (tempSchemas isEmpty) throw XSDException("No root element declaration")

    // Create root record from the schemas generated
    if (tempSchemas.size == 1) tempSchemas.valuesIterator.next()
    else {
      val nullSchema = Schema.create(Schema.Type.NULL)
      val fields = mutable.ListBuffer[Field]()
      for ((source, record) <- tempSchemas) {
        val optionalSchema = Schema createUnion List[Schema](nullSchema,
          record).asJava
        val field = new Field(source.name, optionalSchema, null, null)
        field.addProp(Source.SOURCE, source.toString)
        fields += field
      }
      val record = Schema.createRecord(generateTypeName, null, null, false)
      record.setFields(fields.asJava)
      record.addProp(Source.SOURCE, Source.DOCUMENT)
      record
    }
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
        schemas.getOrElse(name, createRecord(name, eleType))
      case others => throw XSDException(s"Unknown Element type: $others")
    }
    if (array)
      schema = Schema createArray schema
    // TODO Check if or else if
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
          fields ++= processGroupParticle(group, innerOptional = true, innerArray = array)
        else if (!array)
          fields ++= processGroupParticle(group, innerOptional = true, innerArray = false)
        else {
          val name = generateTypeName
          val groupRecord = createRecord(generateTypeName, group)
          fields += (name -> new Field(name,
            Schema.createArray(groupRecord),
            null,
            null))
        }
      case XSModelGroup.COMPOSITOR_SEQUENCE =>
        if (!array)
          fields ++= processGroupParticle(group, innerOptional, innerArray = false)
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
                                    innerOptional: Boolean, innerArray: Boolean): mutable.Map[String, Field] = {
    val fields = mutable.LinkedHashMap[String, Field]()
    for (particle <- group.getParticles.asScala.map(
      _.asInstanceOf[XSParticle])) {
      val optional = innerOptional || particle.getMinOccurs == 0
      val array = innerArray || particle.getMaxOccurs > 1 || particle.getMaxOccursUnbounded
      val innerTerm = particle.getTerm
      innerTerm.getType match {
        case ELEMENT_DECLARATION | WILDCARD =>
          val field = createField(innerTerm, optional, array)
          fields += (field.getProp(Source.SOURCE) -> field)
        case MODEL_GROUP =>
          fields ++= processGroup(innerTerm, optional, array)
        case _ => throw XSDException(s"Unsupported term type ${group.getType}")
      }
    }
    fields
  }

  private def processSequence(
                               group: XSModelGroup,
                               innerOptional: Boolean): mutable.Map[String, Field] = {
    val fields = mutable.LinkedHashMap[String, Field]()
    for (particle <- group.getParticles.asScala.map(
      _.asInstanceOf[XSParticle])) {
      val optional = innerOptional || particle.getMinOccurs == 0
      val array = particle.getMaxOccurs > 1 || particle.getMaxOccursUnbounded
      val innerTerm = particle.getTerm
      innerTerm.getType match {
        case ELEMENT_DECLARATION | WILDCARD =>
          val field = createField(innerTerm, optional, array)
          fields += (field.getProp(Source.SOURCE) -> field)
        case MODEL_GROUP =>
          fields ++= processGroup(innerTerm, optional, array)
        case _ => throw XSDException(s"Unsupported term type ${group.getType}")
      }
    }
    fields
  }

  private def processChoice(group: XSModelGroup): mutable.Map[String, Field] = {
    val fields = mutable.LinkedHashMap[String, Field]()
    for (particle <- group.getParticles.asScala.map(
      _.asInstanceOf[XSParticle])) {
      val array = particle.getMaxOccurs > 1 || particle.getMaxOccursUnbounded
      val innerTerm = particle.getTerm
      innerTerm.getType match {
        case ELEMENT_DECLARATION | WILDCARD =>
          val field = createField(innerTerm, optional = true, array)
          fields += (field.getProp(Source.SOURCE) -> field)
        case MODEL_GROUP =>
          fields ++= processGroup(innerTerm,
            innerOptional = true,
            array = array)
        case _ => throw XSDException(s"Unsupported term type ${group.getType}")
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
      fields += (field.getProp(Source.SOURCE) -> field)
    }

    // Process wildcard attribute
    val wildcard = Option(complexType.getAttributeWildcard)
    if (wildcard isDefined) {
      val field = createField(wildcard.get, optional = true)
      fields += (field.getProp(Source.SOURCE) -> field)
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
      val name = validName(Source.TEXT_VALUE)
      val fieldSchema =
        processType(extnType.getBaseType, optional = true, array = false)
      val field = new Field(name.get, fieldSchema, null, null)
      field.addProp(Source.SOURCE, Source(Source.TEXT_VALUE).toString())
      fields += (field.getProp(Source.SOURCE) -> field)
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

  private def createFields(typeDef: XSObject): List[Field] = {
    val fields = mutable.LinkedHashMap[String, Field]()
    typeDef match {
      case complexType: XSComplexTypeDefinition =>
        fields ++= processAttributes(complexType)
        fields ++= processExtension(complexType)
        fields ++= processParticle(complexType)
      case complexType: XSTerm => fields ++= processGroup(complexType)
    }
    fields.values.toList
  }

  private def createField(source: XSObject,
                          optional: Boolean,
                          array: Boolean = false): Schema.Field = {
    val field: Field = source.getType match {
      case ELEMENT_DECLARATION | ATTRIBUTE_DECLARATION =>
        val (sourceType, attribute) = source.getType match {
          case ELEMENT_DECLARATION =>
            (source.asInstanceOf[XSElementDeclaration].getTypeDefinition,
              false)
          case ATTRIBUTE_DECLARATION =>
            (source.asInstanceOf[XSAttributeDeclaration].getTypeDefinition,
              true)
        }
        val fieldSchema: Schema = processType(sourceType, optional, array)
        val name: Option[String] = validName(source.getName)
        //TODO Generate Unique Name

        val field: Field = new Field(name.get, fieldSchema, null, null)
        field.addProp(Source.SOURCE,
          Source(source.getName, attribute).toString())

        if (sourceType.getTypeCategory == SIMPLE_TYPE) {
          val tempType =
            sourceType.asInstanceOf[XSSimpleTypeDefinition].getBuiltInKind
          if (tempType == XSConstants.DATETIME_DT)
            field.addProp("comment", "timestamp")
        }
        field
      case WILDCARD =>
        val map = Schema.createMap(Schema.create(Schema.Type.STRING))
        new Field(Source.WILDCARD, map, null, null)
      case _ =>
        throw XSDException("Invalid source object type " + source.getType)
    }
    field
  }

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
        if (SchemaFixer.HIVE_KEYWORDS.contains(finalName.toUpperCase))
          finalName = finalName + "_value"
        Option(finalName)
      }
    finalName
  }

  private def primaryType(schemaType: XSTypeDefinition): Schema.Type = {
    val avroType = SchemaFixer.PRIMITIVES get schemaType
      .asInstanceOf[XSSimpleTypeDefinition]
      .getBuiltInKind
    if (avroType isEmpty) Schema.Type.STRING
    else avroType.get
  }

  private def debug(message: String): Unit =
    if (debug) println(("-" * typeLevel) + message)

  /** Read the referenced XSD as per name specified */
  private class SchemaResolver(private val baseDir: Path)
    extends XMLEntityResolver {
    System.setProperty("user.dir", baseDir.toAbsolute.path)

    @throws[XNIException]
    @throws[IOException]
    def resolveEntity(id: XMLResourceIdentifier): XMLInputSource = {
      val fileName = id.getLiteralSystemId
      debug("Resolving " + fileName)
      val source = new XMLInputSource(id)
      source.setByteStream(
        Path(fileName).toAbsoluteWithRoot(baseDir).toFile.bufferedInput())
      source
    }
  }

}

object SchemaFixer {
  val PRIMITIVES: Map[Short, Schema.Type] = Map(
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
    List("EXCHANGE", "OVER", "TIMESTAMP", "DATETIME")


  def main(args: Array[String]): Unit = {
    var schema: SchemaFixer = null
    schema = new SchemaFixer(
      Path("C:/Users/p3008264/git/xml-avro/src/test/resources/books.xsd"),
      Option(
        Path("C:/Users/p3008264/git/xml-avro/xml-avro/src/test/resources")))
    println(schema.schema)
    //    schema = new SchemaFixer(
    //      Path("C:/Users/p3008264/git/xml-avro/xsd/sales/MnSEnvelope.xsd"),
    //      Option(Path("C:/Users/p3008264/git/xml-avro/xsd/sales")))
    //    println(schema.schema)
  }
}
