package in.dreamlabs.xmlavro.config

import java.util

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

  def setBaseDir(value: String): Unit = {
    baseDir = Option(Path(value).toAbsolute)
  }

  def getXSD: XSDConfig = XSD.orNull

  def setXSD(value: XSDConfig): Unit = XSD = Option(value)

  def getXML: XMLConfig = XML.orNull

  def setXML(value: XMLConfig): Unit = XML = Option(value)

  def finish(): Unit = {
    if (XSD.isDefined && baseDir.isDefined)
      XSD.get.use(baseDir get)
    if (XSD.isDefined && XML.isDefined && baseDir.isDefined)
      XML.get.use(baseDir get, XSD get)

  }

}

class XSDConfig {
  var xsdFile: Path = _
  var avscFile: Path = _
  @BeanProperty var rebuildChoice: Boolean = true

  def getXsdFile: String = xsdFile.path

  def setXsdFile(value: String): Unit = {
    xsdFile = Path(value)
  }

  def getAvscFile: String = avscFile.path

  def setAvscFile(value: String): Unit = {
    avscFile = Path(value)
  }

  def use(baseDir: Path): Unit = {
    xsdFile = xsdFile toAbsoluteWithRoot baseDir
    if (Option(avscFile) isDefined)
      avscFile = avscFile toAbsoluteWithRoot baseDir
    else
      avscFile = xsdFile changeExtension "avsc"
  }
}

class XMLConfig {
  var xmlFile: Path = _
  var avscFile: Path = _
  var avroFile: Path = _
  var validationXSD: Path = _
  @BeanProperty var splitBy: String = ""
  @BeanProperty var ignoreMissing: Boolean = false
  @BeanProperty var streamingInput: Boolean = false
  @BeanProperty var caseSensitive: Boolean = true
  @BeanProperty var ignoreCaseFor: util.List[String] = new util.ArrayList[String]

  def getXmlFile: String = xmlFile.path

  def setXmlFile(value: String): Unit = {
    xmlFile = Path(value)
  }

  def getAvscFile: String = avscFile.path

  def setAvscFile(value: String): Unit = {
    avscFile = Path(value)
  }

  def getAvroFile: String = avroFile.path

  def setAvroFile(value: String): Unit = {
    avroFile = Path(value)
  }

  def getValidationXSD: String = validationXSD.path

  def setValidationXSD(value: String): Unit = {
    validationXSD = Path(value)
  }

  def use(baseDir: Path, xsdConfig: XSDConfig): Unit = {
    xmlFile = xmlFile toAbsoluteWithRoot baseDir

    if (Option(avroFile) isDefined)
      avroFile = avroFile toAbsoluteWithRoot baseDir
    else
      avroFile = xmlFile changeExtension "avsc"

    if (Option(avscFile) isDefined)
      avscFile = avscFile toAbsoluteWithRoot baseDir
    else
      avscFile = xsdConfig avscFile
  }
}
