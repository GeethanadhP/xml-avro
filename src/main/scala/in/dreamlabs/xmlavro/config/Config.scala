package in.dreamlabs.xmlavro.config

import java.util

import in.dreamlabs.xmlavro.ConversionException
import in.dreamlabs.xmlavro.Utils.option
import javax.xml.namespace.QName

import scala.beans.BeanProperty
import scala.collection.JavaConverters._
import scala.reflect.io.Path

/**
  * Created by Royce on 01/02/2017.
  */
class Config() {
  @BeanProperty var dynamic: Boolean = false
  @BeanProperty var dynamicSource: String = ""
  @BeanProperty var debug: Boolean = false
  var baseDir: Option[Path] = None
  @BeanProperty var namespaces: Boolean = true
  var XSD: Option[XSDConfig] = None
  var XML: Option[XMLConfig] = None

  def getBaseDir: String = if (baseDir isDefined) baseDir.get.path else null

  def setBaseDir(value: String): Unit =
    baseDir = Option(Path(value).toAbsolute)

  def getXSD: XSDConfig = XSD.orNull

  def setXSD(value: XSDConfig): Unit = XSD = Option(value)

  def getXML: XMLConfig = XML.orNull

  def setXML(value: XMLConfig): Unit = XML = Option(value)

  def validate(): Unit = {
    if (XSD isDefined) {
      XSD.get.namespaces = namespaces
      XSD.get.debug = debug
      XSD.get.baseDir = baseDir
      XSD.get.validate()
    }
    if (XML isDefined) {
      XML.get.namespaces = namespaces
      XML.get.debug = debug
      XML.get.baseDir = baseDir
      XML.get.validate(XSD)
    }
  }
}

class XSDConfig {
  var namespaces: Boolean = _
  var debug: Boolean = _
  var baseDir: Option[Path] = _
  var xsdFile: Path = _
  var avscFile: Path = _

  @BeanProperty var logicalTypes: LogicalTypesConfig = _
  @BeanProperty var rebuildChoice: Boolean = true
  @BeanProperty var stringTimestamp: Boolean = false
  @BeanProperty var ignoreHiveKeywords: Boolean = false
  @BeanProperty var rootElementQName: Option[QName] = None
  @BeanProperty var attributePrefix: String = ""

  def getXsdFile: String = xsdFile.path

  def setXsdFile(value: String): Unit = xsdFile = Path(value)

  def getAvscFile: String = avscFile.path

  def setAvscFile(value: String): Unit = avscFile = Path(value)

  def validate(): Unit = {
    if (baseDir.isDefined) {
      xsdFile = xsdFile toAbsoluteWithRoot baseDir.get
      if (Option(avscFile) isDefined)
        avscFile = avscFile toAbsoluteWithRoot baseDir.get
      else
        avscFile = xsdFile changeExtension "avsc"
    }
    logicalTypes = Option(logicalTypes) getOrElse new LogicalTypesConfig
    logicalTypes.validate()
    if (stringTimestamp) {
      logicalTypes.xsDateTime = LogicalType.STRING
    }
  }
}

object LogicalType {
  /**
    * Logical type "timestamp-millis" annotating a long type.
    */
  val TIMESTAMP_MILLIS = "timestamp-millis"
  /**
    * Logical type "timestamp-micros" annotating a long type.
    */
  val TIMESTAMP_MICROS = "timestamp-micros"

  /**
    * Logical type "times-millis" annotating a long type.
    */
  val TIME_MILLIS = "time-millis"

  /**
    * Logical type "times-micros" annotating a long type.
    */
  val TIME_MICROS = "time-micros"

  /**
    * Logical type "date" annotating an int type.
    */
  val DATE = "date"

  /**
    * Dummy logical type for handling values as string without indicating a logicalType.
    */
  val STRING = "string"

  /**
    * Dummy logical type for handling values as long without indicating a logicalType.
    */
  val LONG = "long"

}

class LogicalTypesConfig {

  @BeanProperty
  var xsDateTime: String = LogicalType.LONG
  @BeanProperty
  var xsTime: String = LogicalType.STRING
  @BeanProperty
  var xsDate: String = LogicalType.STRING
  @BeanProperty
  var xsDecimal: XSDecimalConfig = new XSDecimalConfig

  def validate(): Unit = {
    xsDateTime = Option(xsDateTime) getOrElse ""
    xsDateTime match {
      case LogicalType.LONG
           | LogicalType.STRING
           | LogicalType.TIMESTAMP_MILLIS
           | LogicalType.TIMESTAMP_MICROS => /* accept */
      case _ =>
        throw new IllegalArgumentException("Invalid configuration for xs:dateTime logical type.")
    }

    xsTime = Option(xsTime) getOrElse ""
    xsTime match {
      case LogicalType.STRING
           | LogicalType.TIME_MILLIS
           | LogicalType.TIME_MICROS => /* accept */
      case _ =>
        throw new IllegalArgumentException("Invalid configuration for xs:time logical type.")
    }

    xsDate = Option(xsDate) getOrElse ""
    xsDate match {
      case LogicalType.STRING | LogicalType.DATE => /* accept */
      case _ =>
        throw new IllegalArgumentException("Invalid configuration for xs:date logical type.")
    }

    xsDecimal = Option(xsDecimal) getOrElse new XSDecimalConfig
    xsDecimal.validate()
  }

}

object XSDecimalConfigLogicalType {
  val DOUBLE = "double"

  val STRING = "string"

  val DECIMAL = "decimal"
}


class XSDecimalConfig {
  @BeanProperty
  var avroType = XSDecimalConfigLogicalType.DOUBLE
  @BeanProperty
  var fallbackType = XSDecimalConfigLogicalType.STRING
  @BeanProperty
  var fallbackPrecision : Integer = null
  @BeanProperty
  var fallbackScale : Integer = 0

  def validate(): Unit = {

    val acceptedAvroTypes = List(
      XSDecimalConfigLogicalType.DECIMAL,
      XSDecimalConfigLogicalType.DOUBLE,
      XSDecimalConfigLogicalType.STRING
    )

    if (!acceptedAvroTypes.contains(avroType)) {
      throw new IllegalArgumentException(s"Invalid configuration value '$avroType' for xsDecimal avroType.")
    }

    if (!acceptedAvroTypes.contains(fallbackType)) {
      throw new IllegalArgumentException(s"Invalid configuration value '$fallbackType' for xsDecimal fallbackType.")
    }

    if (fallbackType == XSDecimalConfigLogicalType.DECIMAL) {
      if (Option(fallbackPrecision) isEmpty) {
        throw new IllegalArgumentException(s"Missing xsDecimal fallbackPrecision " +
          s"configuration for '$fallbackType' fallback type.")
      }
      if (Option(fallbackScale) isEmpty) {
        throw new IllegalArgumentException(s"Missing xsDecimal fallbackScale " +
          s"configuration for '$fallbackType' fallback type.")
      }
      if (fallbackPrecision <= 0) {
        throw new IllegalArgumentException(s"Invalid configuration value $fallbackPrecision for xsDecimal fallbackPrecision.")
      }
      if (fallbackScale <= 0 || fallbackScale > fallbackPrecision) {
        throw new IllegalArgumentException(s"Invalid configuration value $fallbackScale for xsDecimal fallbackScale.")
      }
    }

  }
}

class XMLConfig {
  var namespaces: Boolean = _
  var debug: Boolean = _
  var baseDir: Option[Path] = None
  var qaDir: Option[Path] = None
  var xmlFile: Path = _
  var streamingInput, streamingOutput: Boolean = false
  var validationXSD: Option[Path] = None
  var splitBy: String = ""
  var avscFile: Path = _
  var avroFile: Path = _
  var errorFile: Option[Path] = None

  @BeanProperty var documentRootTag: String = _
  @BeanProperty var ignoreMissing: Boolean = false
  @BeanProperty var suppressWarnings: Boolean = false
  @BeanProperty var xmlInput: String = _
  @BeanProperty var avroOutput: String = _
  @BeanProperty var docErrorLevel: String = "WARNING"
  @BeanProperty var split: util.List[AvroSplit] =
    new util.ArrayList[AvroSplit]()
  @BeanProperty var caseSensitive: Boolean = true
  @BeanProperty var ignoreCaseFor: util.List[String] =
    new util.ArrayList[String]

  @BeanProperty var useAvroInput: Boolean = false
  var inputAvroMappings: Map[String, String] = _
  var inputAvroKey: String = _
  var inputAvroUniqueKey: Option[String] = None


  def getQaDir: String = if (qaDir isDefined) qaDir.get.path else null

  def setQaDir(value: String): Unit = qaDir = Option(Path(value))

  def getValidationXSD: String =
    if (validationXSD isDefined) validationXSD.get.path else null

  def setValidationXSD(value: String): Unit =
    validationXSD = Option(Path(value))

  def getErrorFile: String =
    if (errorFile isDefined) errorFile.get.path else null

  def setErrorFile(value: String): Unit =
    errorFile = Option(Path(value))

  def getAvscFile: String = avscFile.path

  def setAvscFile(value: String): Unit = avscFile = Path(value)

  def getAvroFile: String = avroFile.path

  def setAvroFile(value: String): Unit = avroFile = Path(value)

  def getInputAvroMappings: util.Map[String, String] =
    if (Option(inputAvroMappings) isDefined) inputAvroMappings.asJava else null

  def setInputAvroMappings(value: util.Map[String, String]): Unit =
    inputAvroMappings = value.asScala.toMap

  def validate(xsdConfig: Option[XSDConfig]): Unit = {
    if (Option(xmlInput) isDefined)
      if (xmlInput == "stdin") {
        streamingInput = true
        if (Option(avroOutput).isEmpty || avroOutput == "stdout")
          streamingOutput = true
        else avroFile = Path(avroOutput)
      } else {
        xmlFile = Path(xmlInput)
        if (Option(avroOutput) isDefined) avroFile = Path(avroOutput)
        else avroFile = xmlFile changeExtension "avro"
      } else
      throw ConversionException("XML Input is not specified in the config")

    if (baseDir.isDefined && !streamingInput)
      xmlFile = xmlFile toAbsoluteWithRoot baseDir.get

    if (baseDir.isDefined && !streamingOutput)
      avroFile = avroFile toAbsoluteWithRoot baseDir.get

    if (Option(avscFile).isDefined) {
      if (baseDir.isDefined)
        avscFile = avscFile toAbsoluteWithRoot baseDir.get
    } else if (xsdConfig.isDefined)
      avscFile = xsdConfig.get.xsdFile changeExtension "avsc"

    if (baseDir.isDefined && validationXSD.isDefined)
      validationXSD = Option(validationXSD.get.toAbsoluteWithRoot(baseDir.get))

    if (baseDir.isDefined && qaDir.isDefined)
      qaDir = Option(qaDir.get.toAbsoluteWithRoot(baseDir.get))

    if (baseDir.isDefined && errorFile.isDefined)
      errorFile = Option(errorFile.get.toAbsoluteWithRoot(baseDir.get))

    if (Option(documentRootTag) isEmpty)
      throw ConversionException("Document Root Tag is not specified in the config")

    if (option(splitBy) isEmpty)
      splitBy = documentRootTag

    if (split isEmpty) {
      val tempSplit = new AvroSplit
      tempSplit.avscFile = avscFile
      tempSplit.avroFile = avroFile
      tempSplit.stream = streamingOutput
      tempSplit.by = splitBy
      split.add(tempSplit)
    }

    split.forEach(item => item.validate(baseDir))

    if (useAvroInput) {
      inputAvroMappings.foreach {
        case (key, value) =>
          if (value == "xmlInput") inputAvroKey = key
          else if (value == "unique_id") inputAvroUniqueKey = Option(key)
      }

      if (Option(inputAvroKey) isEmpty)
        throw ConversionException("No xmlInput specified in inputAvroMappings")
    }
  }
}

class AvroSplit {
  @BeanProperty var by: String = ""
  var avscFile: Path = _
  var avroFile: Path = _
  var stream: Boolean = false

  def getAvscFile: String = avscFile.path

  def setAvscFile(value: String): Unit = avscFile = Path(value)

  def getAvroFile: String = avroFile.path

  def setAvroFile(value: String): Unit = avroFile = Path(value)

  def validate(baseDir: Option[Path]): Unit = {
    if (option(by) isEmpty)
      ConversionException("Split by is not specified in the config")

    if (Option(avroFile) isEmpty)
      ConversionException(
        s"Avro Output is not specified in the config for tag $by")
    else if (baseDir isDefined)
      avroFile = avroFile toAbsoluteWithRoot baseDir.get

    if (Option(avscFile) isEmpty)
      ConversionException(
        s"Avsc Schema is not specified in the config for tag $by")
    else if (baseDir isDefined)
      avscFile = avscFile toAbsoluteWithRoot baseDir.get
  }
}
