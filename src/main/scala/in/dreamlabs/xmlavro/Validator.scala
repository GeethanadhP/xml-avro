package in.dreamlabs.xmlavro

import javax.xml.XMLConstants
import javax.xml.transform.stream.StreamSource
import javax.xml.validation.SchemaFactory
import org.xml.sax.SAXException

object Validator {

  def validate(xmlFile: String, xsdFile: String): Boolean = {
    try {
      val schema = SchemaFactory
        .newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI)
        .newSchema(new StreamSource("i180.xsd"))
      val validator = schema.newValidator()
      validator.validate(new StreamSource(xmlFile))
    } catch {
      case ex: SAXException => ex.printStackTrace(); return false
      case ex: Exception => ex.printStackTrace()
    }
    true
  }

  def main(args: Array[String]) {
    println(validate("i180.xml", "i180.xsd"))
  }
}