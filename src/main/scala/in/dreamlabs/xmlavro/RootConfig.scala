package in.dreamlabs.xmlavro

import java.util

import scala.beans.BeanProperty
import scala.reflect.io.Path

/**
  * Created by Royce on 01/02/2017.
  */
class RootConfig() {
  @BeanProperty var dynamic: Boolean = false
  @BeanProperty var dynamicSource: String = ""
  @BeanProperty var debug: Boolean = false
  var baseDir: Path = Path("")
  @BeanProperty var namespaces: Boolean = true
  var XSD: XSDConfig = _
  var XML: XMLConfig = _


  def getBaseDir: String = baseDir.path

  def setBaseDir(value: String): Unit = {
    baseDir = Path(value).toAbsolute
  }

  def getXSD: XSDConfig = XSD

  def setXSD(value: XSDConfig): Unit = {
    XSD = value
    if (Some(baseDir) isDefined)
      XSD.use(baseDir)
  }

  def getXML: XMLConfig = XML

  def setXML(value: XMLConfig): Unit = {
    XML = value
    if (Some(baseDir) isDefined)
      XML.use(baseDir, XSD)
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
    if (Some(avscFile) isDefined)
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
  @BeanProperty var ignoreWarnings: Boolean = false
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

    if (Some(avroFile) isDefined)
      avroFile = avroFile toAbsoluteWithRoot baseDir
    else
      avroFile = xmlFile changeExtension "avsc"

    if (Some(avscFile) isDefined)
      avscFile = avscFile toAbsoluteWithRoot baseDir
    else
      avscFile = xsdConfig avscFile
  }
}
