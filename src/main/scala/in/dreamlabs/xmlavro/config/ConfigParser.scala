package in.dreamlabs.xmlavro.config

import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.Constructor

import scala.reflect.io.Path

/**
  * Created by Royce on 21/12/2016.
  */
class ConfigParser(args: Seq[String]) extends ArgParse(args) {

  val config: Config = {
    val configFile = opt[Path]("config", 'c')
    if (configFile isDefined) {
      fetchConfig(configFile get)
    } else {
      new Config
    }
  }

  processArgs()
  config.finish()

  private def processArgs(): Unit = {
    val debug = toggle("debug", 'd')
    val baseDir = opt[Path]("baseDir", 'b')
    val stream = toggle("stream", 's')
    val xsd = opt[List[Path]]("toAvsc", 'd')
    val xml = opt[List[Path]]("toAvro", 'x')
    val splitBy = opt[String]("splitBy", 'y')
    val ignoreMissing = toggle("ignoreMissing", 'i')
    val validateSchema = opt[Path]("validateSchema", 'v')

    if (debug isDefined) config.debug = debug.get
    if (baseDir isDefined) config.baseDir = baseDir
    if (xsd isDefined) {
      val tempConfig =
        if (config.XSD isDefined) config.XSD.get
        else {
          val temp = new XSDConfig
          config.XSD = Option(temp)
          temp
        }
      val temp = xsd.get
      tempConfig.xsdFile = temp.head
      if (temp.length > 1) tempConfig.avscFile = temp(1)
      if (temp.length > 2)
        throw new IllegalArgumentException(
          "Too many values provided for xsd option")
    }
    if (xml isDefined) {
      val tempConfig =
        if (config.XML isDefined) config.XML.get
        else {
          val temp = new XMLConfig
          config.XML = Option(temp)
          temp
        }
      val temp = xml.get
      tempConfig.avscFile = temp.head
      if (temp.length > 1) tempConfig.xmlFile = temp(1)
      if (temp.length > 2) tempConfig.avroFile = temp(2)
      if (temp.length > 3)
        throw new IllegalArgumentException(
          "Too many values provided for xml option")
      if (stream isDefined) tempConfig.streamingInput = stream.get
      if (splitBy isDefined) tempConfig.splitBy = splitBy.get
      if (ignoreMissing isDefined) tempConfig.ignoreMissing = ignoreMissing.get
      if (validateSchema isDefined)
        tempConfig.validationXSD = validateSchema.get
    }
  }

  private def fetchConfig(configFile: Path): Config = {
    val configReader = configFile.toFile.bufferedReader()
    val obj = new Yaml(new Constructor(classOf[Config])) load configReader
    obj.asInstanceOf[Config]
  }
}

object ConfigParser {
  val USAGE1 =
    "{-d|--debug} {-b|--baseDir <baseDir>} -xsd|--toAvsc <xsdFile> {<avscFile>}"
  val USAGE2 =
    "{-b|--baseDir <baseDir>} {-s|--stream|--stdout} -xml|--toAvro <avscFile> {<xmlFile>} {<avroFile>} {-sb|--splitby <splitBy>} {-i|--ignoreMissing} {-v|--validateSchema <xsdFile>}"
  val USAGE3 =
    "{-d|--debug} {-b|--baseDir <baseDir>} {-xsd|--toAvsc <xsdFile> {<avscFile>}} {-s|--stream|--stdout} {-xml|--toAvro {<xmlFile>} {<avroFile>} {-sb|--splitby <splitBy>}} {-i|--ignoreMissing}"
  val USAGE: String = "XSD to AVSC Usage : " + USAGE1 + "\nXML to AVRO Usage : " + USAGE2 + "\nMixed Usage : " + USAGE3

  def apply(args: Array[String]): ConfigParser = new ConfigParser(args)
}
