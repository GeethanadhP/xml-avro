package in.dreamlabs.xmlavro.config

import java.util

import in.dreamlabs.xmlavro.ConversionError

import scala.beans.BeanProperty
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

  def setBaseDir(value: String): Unit = baseDir = Option(Path(value).toAbsolute)

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
  @BeanProperty var rebuildChoice: Boolean = true

  def getXsdFile: String = xsdFile.path

  def setXsdFile(value: String): Unit = xsdFile = Path(value)

  def getAvscFile: String = avscFile.path

  def setAvscFile(value: String): Unit = avscFile = Path(value)

  def validate(): Unit = if (baseDir.isDefined) {
    xsdFile = xsdFile toAbsoluteWithRoot baseDir.get
    if (Option(avscFile) isDefined)
      avscFile = avscFile toAbsoluteWithRoot baseDir.get
    else
      avscFile = xsdFile changeExtension "avsc"
  }
}

class XMLConfig {
  var namespaces: Boolean = _
  var debug: Boolean = _
  var baseDir: Option[Path] = None
  var xmlFile: Path = _
  var streamingInput: Boolean = false
  var validationXSD: Option[Path] = None
  var splitBy: String = ""
  var avscFile: Path = _
  var avroFile: Path = _

  @BeanProperty var split: util.List[AvroSplit] = new util.ArrayList[AvroSplit]()
  @BeanProperty var xmlInput: String = _
  @BeanProperty var ignoreMissing: Boolean = false
  @BeanProperty var caseSensitive: Boolean = true
  @BeanProperty var ignoreCaseFor: util.List[String] =
    new util.ArrayList[String]

  def getValidationXSD: String =
    if (validationXSD isDefined) validationXSD.get.path else null

  def setValidationXSD(value: String): Unit =
    validationXSD = Option(Path(value))

  def getAvscFile: String = avscFile.path

  def setAvscFile(value: String): Unit = avscFile = Path(value)

  def getAvroFile: String = avroFile.path

  def setAvroFile(value: String): Unit = avroFile = Path(value)

  def validate(xsdConfig: Option[XSDConfig]): Unit = {
    if (xmlInput == "stdin")
      streamingInput = true
    else if (baseDir isDefined) {
      xmlFile = Path(xmlInput) toAbsoluteWithRoot baseDir.get

      if (Option(avscFile) isDefined)
        avscFile = avscFile toAbsoluteWithRoot baseDir.get
      else if (xsdConfig isDefined)
        avscFile = xsdConfig.get.xsdFile changeExtension "avsc"

      if (Option(avroFile) isDefined)
        avroFile = avroFile toAbsoluteWithRoot avroFile
      else
        avroFile = xmlFile changeExtension "avro"
    }

    if (split isEmpty) {
      val tempSplit = new AvroSplit
      tempSplit.avscFile = avscFile
      tempSplit.avroFile = avroFile
      tempSplit.by = splitBy
      split.add(tempSplit)
    }

    split.forEach(item => item.validate(baseDir))
  }
}

class AvroSplit {
  @BeanProperty var by: String = ""
  var avscFile: Path = _
  var avroFile: Path = _

  def getAvscFile: String = avscFile.path

  def setAvscFile(value: String): Unit = avscFile = Path(value)

  def getAvroFile: String = avroFile.path

  def setAvroFile(value: String): Unit = avroFile = Path(value)

  def validate(baseDir: Option[Path]): Unit = {
    if (Option(avroFile) isEmpty)
      ConversionError("Avro Output is not specified")
    else if (baseDir isDefined)
      avroFile = avroFile toAbsoluteWithRoot baseDir.get

    if (Option(avscFile) isEmpty)
      ConversionError("Avsc Schema is not specified")
    else if (baseDir isDefined)
      avscFile = avscFile toAbsoluteWithRoot baseDir.get
  }

}
