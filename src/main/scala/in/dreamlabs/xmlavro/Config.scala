package in.dreamlabs.xmlavro

import in.dreamlabs.xmlavro.ConvertMode.ConvertMode
import in.dreamlabs.xmlavro.Utils._

import scala.collection.mutable.ListBuffer
import scala.reflect.io.Path

/**
  * Created by Royce on 21/12/2016.
  */
trait Config {
  val modes = new ListBuffer[ConvertMode]()
  var xsdFile, xmlFile: Path = _
  var avroFile, avscFile: Path = _
  var baseDir: Path = _
  var validationSchema: Path = _
  var debug, stdout, skipMissing: Boolean = false
  var split: String = ""
  var configFile: Path = _
  var isAdvanced = false
}

object Config {
  def apply(args: Array[String]): Config = CommandLineConfig(args)
}

object ConvertMode extends Enumeration {
  type ConvertMode = Value
  val XML, XSD = Value
}

class CommandLineConfig(args: Array[String]) extends Config {
  private val length = args.length
  private var i = 0
  while (i < length) {
    val arg = args(i)
    if (arg startsWith "-") arg match {
      case "-d" | "--debug" => debug = true
      case "-b" | "--baseDir" =>
        baseDir = Path(fetchArg("Base directory location"))
      case "-c" | "--config" =>
        configFile = Path(fetchArg("Config file location"));
        isAdvanced = true;
        processConfigFile()
      case "-s" | "--stdout" | "--stream" => stdout = true
      case "-xsd" | "--toAvsc" => fetchXSDParams
      case "-xml" | "--toAvro" => fetchXMLParams
      case "-sb" | "--splitby" => split = fetchArg("Split element name")
      case "-i" | "--ignoreMissing" => skipMissing = true
      case "-v" | "--validateSchema" =>
        validationSchema =
          replaceBaseDir(fetchArg("Validation schema location"), baseDir)
      case _ => throw new IllegalArgumentException("Unsupported option " + arg)
    } else
      throw new IllegalArgumentException("Unsupported arguments format " + arg)
    i += 1
  }

  private def fetchArg(name: String): String = {
    if (i == length - 1)
      throw new IllegalArgumentException("$name missing in arguments")
    else
      i += 1
    args(i)
  }

  private def fetchXSDParams: Unit = {
    if (i == length - 1)
      throw new IllegalArgumentException("XSD File missing in arguments")
    i += 1
    xsdFile = replaceBaseDir(args(i), baseDir)
    if (i < length - 1 && !args(i + 1).startsWith("-")) {
      i += 1
      avscFile = replaceBaseDir(args(i), baseDir)
    } else avscFile = replaceExtension(xsdFile, "avsc")
    modes += ConvertMode.XSD
  }

  private def fetchXMLParams: Unit = {
    if (avscFile == null && i == args.length - 1)
      throw new IllegalArgumentException("AVSC File missing in arguments")
    else if (avscFile == null) {
      i += 1
      avscFile = replaceBaseDir(args(i), baseDir)
    }
    if (!stdout && i == args.length - 1)
      throw new IllegalArgumentException("XML File missing in arguments")
    if (!stdout) {
      i += 1
      xmlFile = replaceBaseDir(args(i), baseDir)
      if (i < args.length - 1 && !args(i + 1).startsWith("-")) {
        i += 1
        xmlFile = replaceBaseDir(args(i), baseDir)
      } else avroFile = replaceExtension(xmlFile, "avro")
    }
    modes += ConvertMode.XML
  }

  private def processConfigFile(): Unit = {

  }
}

object CommandLineConfig {
  val USAGE1 =
    "{-d|--debug} {-b|--baseDir <baseDir>} -xsd|--toAvsc <xsdFile> {<avscFile>}"
  val USAGE2 =
    "{-b|--baseDir <baseDir>} {-s|--stream|--stdout} -xml|--toAvro <avscFile> {<xmlFile>} {<avroFile>} {-sb|--splitby <splitBy>} {-i|--ignoreMissing} {-v|--validateSchema <xsdFile>}"
  val USAGE3 =
    "{-d|--debug} {-b|--baseDir <baseDir>} {-xsd|--toAvsc <xsdFile> {<avscFile>}} {-s|--stream|--stdout} {-xml|--toAvro {<xmlFile>} {<avroFile>} {-sb|--splitby <splitBy>}} {-i|--ignoreMissing}"
  val USAGE: String = "XSD to AVSC Usage : " + USAGE1 + "\nXML to AVRO Usage : " + USAGE2 + "\nMixed Usage : " + USAGE3

  def apply(args: Array[String]): CommandLineConfig =    new CommandLineConfig(args)
}

class ConfigFile(val ignoreWarings: Boolean,
                 val splitBy: String,
                 val caseSensitive: Boolean,
                 val ignoreCaseList: List[String])
