package in.dreamlabs.xmlavro

import java.io._
import java.lang.management.ManagementFactory
import java.util
import java.util.Calendar

import net.elodina.xmlavro.SchemaBuilder
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

  for (mode <- config.modes) {
    mode match {
      case ConvertMode.XSD =>
        //        xsdIn = (config.xsdFile toFile) bufferedInput()
        //        avscOut = (config.avscFile toFile) bufferedOutput()
        convertXSD()
      case ConvertMode.XML =>
        if (config.stdout) {
          xmlIn = new BufferedInputStream(System.in)
          avroOut = new BufferedOutputStream(System.out)
        } else {
          xmlIn = (config.xmlFile toFile) bufferedInput ()
          avroOut = (config.avroFile toFile) bufferedOutput ()
          System.out.println(
            "Converting: " + config.xmlFile + " -> " + config.avroFile)
        }
        convertXML(config.avscFile,
                   config.split,
                   config.skipMissing,
                   config.validationSchema)
    }
  }

  @throws[IOException]
  private def convertXSD() {
    if (!config.stdout)
      System.out.println(
        "Converting: " + config.xsdFile + " -> " + config.avscFile)

    val schemaBuilder = SchemaFixer(config xsdFile, config avscFile)
    schemaBuilder debug = config.debug
    schemaBuilder baseDir = config.baseDir
    schemaBuilder rebuildChoice = config.rebuildChoice
    schemaBuilder createSchema ()
    //val schemaBuilder = new SchemaFixer(xsdIn,Option(config.baseDir))//new SchemaBuilder
    //    schemaBuilder setDebug (config debug)
    //    if (config.baseDir != null)
    //      schemaBuilder setResolver new BaseDirResolver(config.baseDir)
    //    val schema = schemaBuilder.createSchema(xsdIn)
    //    xsdIn close()
    //    writeAvsc(schema)
    //    writeAvsc
  }

  //  @throws[IOException]
  //  private def writeAvsc(schema: Schema) {
  //    avscOut write schema.toString(true).getBytes()
  //    avscOut close()
  //  }

  @throws[IOException]
  private def convertXML(avscFile: Path,
                         split: String,
                         skipMissing: Boolean,
                         validationSchema: Path) {
    val schema = new Schema.Parser().parse(avscFile.jfile)
    val startTime = Calendar.getInstance().getTimeInMillis
    //    val datumBuilder =
    //      new DatumBuilder(schema, split, skipMissing, if (validationSchema != null) validationSchema.jfile else null)
    //                val datums: util.ArrayList[AnyRef] = datumBuilder.createDatum(xmlIn)
    //            val datums: util.List[Any]= new DatumFixer(schema,xmlIn,Option(split),true).datums.asJava

    val datums = new AvroBuilder(schema, split, true).createDatums(xmlIn)
    val endTime = Calendar.getInstance().getTimeInMillis
    System.err.println("Time taken: " + (endTime - startTime))
    xmlIn close ()
    writeAvro(schema, datums)
  }

  @throws[IOException]
  private def writeAvro(schema: Schema, datums: util.List[AnyRef]) {
    val datumWriter = new SpecificDatumWriter[AnyRef](schema)
    val fileWriter = new DataFileWriter[AnyRef](datumWriter)
    fileWriter.setCodec(CodecFactory.snappyCodec)
    fileWriter.create(schema, avroOut)
    for (datum <- datums.asScala) fileWriter.append(datum)
    fileWriter flush ()
    fileWriter close ()
    avroOut close ()
  }

  private class BaseDirResolver(val baseDir: Path)
      extends SchemaBuilder.Resolver {
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
      else Config apply args
    } catch {
      case e: IllegalArgumentException =>
        println(
          "XML Avro converter\nError: " + e.getMessage + "\n\n" + CommandLineConfig.USAGE + "\n")
        System.exit(1)
    }
    Converter apply config.asInstanceOf[Config]
  }

  def apply(config: Config): Converter = new Converter(config)
}
