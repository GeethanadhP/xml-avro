package in.dreamlabs.xmlavro

import java.io._

import in.dreamlabs.xmlavro.config.{Config, ConfigParser, XMLConfig, XSDConfig}

/**
  * Created by Royce on 22/12/2016.
  */
class Converter(val config: Config) {

  if (config.XSD isDefined)
    convertXSD(config.XSD.get)
  if (config.XML isDefined) {
    val xConfig = config.XML.get
    if (!xConfig.streamingInput)
      Utils.info("Converting: " + xConfig.xmlFile + " -> " + xConfig.avroFile)
    convertXML(xConfig)
  }

  @throws[IOException]
  private def convertXSD(xConfig: XSDConfig) {
    Utils.info("Converting: " + xConfig.xsdFile + " -> " + xConfig.avscFile)
    val schemaBuilder = SchemaBuilder(xConfig)
    schemaBuilder createSchema()
  }

  private def convertXML(xConfig: XMLConfig) {
    Utils.profile("Avro Conversion") {
      val builder = new AvroBuilder(xConfig)
      builder.createDatums()
    }
  }
}

object Converter {
  @throws[IOException]
  def main(args: Array[String]): Unit = {
    val config = try {
      if (args isEmpty)
        throw new IllegalArgumentException("No Arguments specified")
      else ConfigParser apply args
    } catch {
      case e: IllegalArgumentException =>
        Utils.log("ERROR",
          "XML Avro converter\nError: " + e.getMessage + "\n\n" + ConfigParser.USAGE + "\n")
        System.exit(1)
    }
    Converter apply config.asInstanceOf[ConfigParser]
  }

  def apply(config: ConfigParser): Converter = new Converter(config.config)
}
