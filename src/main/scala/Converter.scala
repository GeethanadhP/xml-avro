import java.io._
import java.util
import java.util.Calendar

import in.dreamlabs.xmlavro.config.{Config, ConfigParser, XSDConfig}
import in.dreamlabs.xmlavro.{AvroBuilder, SchemaBuilder}
import net.elodina.xmlavro.OldSchemaBuilder
import org.apache.avro.Schema
import org.apache.avro.file.{CodecFactory, DataFileWriter}
import org.apache.avro.specific.SpecificDatumWriter

import scala.collection.JavaConverters._
import scala.reflect.io.Path

/**
  * Created by Royce on 22/12/2016.
  */
class Converter(val config: Config) {

  private var avscOut, avroOut: BufferedOutputStream = _
  private var xmlIn, xsdIn: BufferedInputStream = _

  if (config.XSD isDefined)
    convertXSD(config.XSD.get)
  if (config.XML isDefined) {
    val xConfig = config.XML.get
    if (xConfig.streamingInput) {
      xmlIn = new BufferedInputStream(System.in)
      avroOut = new BufferedOutputStream(System.out)
    } else {
      xmlIn = (xConfig.xmlFile toFile) bufferedInput()
      avroOut = (xConfig.avroFile toFile) bufferedOutput()
      System.out.println(
        "Converting: " + xConfig.xmlFile + " -> " + xConfig.avroFile)
    }
    convertXML(xConfig.avscFile,
      xConfig.splitBy,
      xConfig.ignoreMissing,
      xConfig.validationXSD)
  }

  @throws[IOException]
  private def convertXSD(xConfig: XSDConfig) {
    System.out.println(
      "Converting: " + xConfig.xsdFile + " -> " + xConfig.avscFile)

    val schemaBuilder = SchemaBuilder(xConfig xsdFile, xConfig avscFile)
    schemaBuilder debug = config.debug
    schemaBuilder baseDir = config.baseDir
    schemaBuilder rebuildChoice = xConfig.rebuildChoice
    schemaBuilder namespaces = config.namespaces
    schemaBuilder createSchema()
  }

  private def convertXML(avscFile: Path,
                         split: String,
                         skipMissing: Boolean,
                         validationSchema: Path) {
    val schema = new Schema.Parser().parse(avscFile.jfile)
    val startTime = Calendar.getInstance().getTimeInMillis
    val datums = new AvroBuilder(schema, split, true).createDatums(xmlIn)
    val endTime = Calendar.getInstance().getTimeInMillis
    System.err.println("Time taken: " + (endTime - startTime))
    xmlIn close()
    writeAvro(schema, datums)
  }

  @throws[IOException]
  private def writeAvro(schema: Schema, datums: util.List[AnyRef]) {
    val datumWriter = new SpecificDatumWriter[AnyRef](schema)
    val fileWriter = new DataFileWriter[AnyRef](datumWriter)
    fileWriter.setCodec(CodecFactory.snappyCodec)
    fileWriter.create(schema, avroOut)
    for (datum <- datums.asScala) fileWriter.append(datum)
    fileWriter flush()
    fileWriter close()
    avroOut close()
  }

  private class BaseDirResolver(val baseDir: Path)
    extends OldSchemaBuilder.Resolver {
    // Change Working directory to the base directory
    System.setProperty("user.dir", baseDir.toAbsolute.path)

    def getStream(systemId: String): InputStream = {
      try Path(systemId).toAbsoluteWithRoot(baseDir).toFile.inputStream()
      catch {
        case _: FileNotFoundException => null
      }
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
        println(
          "XML Avro converter\nError: " + e.getMessage + "\n\n" + ConfigParser.USAGE + "\n")
        System.exit(1)
    }
    Converter apply config.asInstanceOf[ConfigParser]
  }

  def apply(config: ConfigParser): Converter = new Converter(config.config)
}
