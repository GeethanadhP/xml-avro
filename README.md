# XSD => Avsc & XML => Avro
****
This project was initially a fork of [xml-avro-elodina](https://github.com/elodina/xml-avro).
Later evolved to separate project with lotsss of bug fixes, memory & performance improvements, options, re-coded in Scala
****
- Converts any XSD to a proper usable Avro schema (Avsc)
- Converts any XML to avro using the provided schema. What can it do? See the list below.
    - Handle any large size XML (even in GigaBytes), as it streams the xml
    - Read xml from stdin and output to stdout
    - Validate the XML with XSD
    - Split the data at any specified element (can have any no.of splits)
    - Handle multiple documents in single file (useful when streaming continuous data)
    - Write out failed documents without killing the whole process
    - Completely configurable

### Running Project
1. `git clone` to clone the repository to local
2. `gradle build` to generate the jar file
3. `java -jar xml-avro.jar <options>` to run the code (options as below) 

### Basic Command line Options
```
XSD to AVSC Usage : {-d|--debug} {-b|--baseDir <baseDir>} -xsd|--toAvsc <xsdFile> {<avscFile>}
XML to AVRO Usage : {-b|--baseDir <baseDir>} {-s|--stream|--stdout} -xml|--toAvro <avscFile> {<xmlFile>} {<avroFile>} {-sb|--splitby <splitBy>} {-i|--ignoreMissing} {-v|--validateSchema <xsdFile>}
Mixed Usage : {-d|--debug} {-b|--baseDir <baseDir>} -xsd|--toAvsc <xsdFile> {<avscFile>} {-s|--stream|--stdout} -xml|--toAvro {<xmlFile>} {<avroFile>} {-sb|--splitby <splitBy>} {-i|--ignoreMissing} {-v|--validateSchema <xsdFile>}
Use Config File: -c <configFile>
```

### Advanced Configuration
For advanced configuration create yml config file. As per the below format
```
debug: false                    # Enable printing of debug messages
baseDir: "files"                # Base directory where most files are relative to
namespaces: true                # Enable/Disable usage of namespaces in schema/conversion - Optional (default: true)

XML:                            # Convert XML
  xmlInput: stdin               # Source of XML [ stdin | "somefile.xml" ]
  avscFile: "books.avsc"        # Avsc file to use for conversion - (If not using splits)
  avroOutput: stdout             # Traget location [ stdout | "somefile.avro" ] - Optional (Uses the xmlInput to assume the output) (If not using splits)
  documentRootTag: "books"      # Root tag of the XML (without namespace)
  validationXSD: "books.xsd"    # Enable validation with specified xsd
  ignoreMissing: true           # Incase you use a smaller version of avsc (to take only required tags),
                                # tags in the xml may not exist in the trimmed avsc.. 
                                # This option enables to ignore the missing tags instead of failing
  suppressWarnings: true        # In case of a lot of missing fields don't print them as warnings
  split:                        # Split the avro records based on specifed list
    -
      by: "bookName"            # Split tag name
      avscFile: "name.avsc"     # Avsc File for the split part
      avroFile: "name.avro"     # Avro file name to save to
    -
      by: "bookPublisher"
      avscFile: "publisher.avsc"
      avroFile: "publisher.avro"
  qaDir: "some path"            # Writes some count details 
  caseSensitive: true           # Tags matching xml & avsc are case sensitive - Optional (default: true) 
  ignoreCaseFor:                # Ignore case senitivity for the below list
    - "SomeTag"
  docErrorLevel: "WARNING"      # Use this level to log in case of error in a document 
  errorFile: "failures.xml"     # Writes the failed documents to this file
  useAvroInput: true            # Read xml data from inside an avro file
  inputAvroMappings:            # Set of mappings from source field name to target, use "xmlInput" as target to mark it as the xml data, use "unique_id" as target to mark the value as unique key
      "headers" : "avroHeader"
      "body" : "xmlInput"
      "headers.unique_id" : "unique_id"
XSD:
  xsdFile: "somefile.xsd"       # Source of XSD
  avscFile: "books.avsc"        # Avsc file to save as - Optional (Uses the xsdFile to assume the output)
  stringTimestamp: true         # Represent timestamp as string instead of long
  onlyFirstRootElement: true    # Only process the first root element (useful with nested xsds). Default value is false.
  rootElementQName: "{ns}name"  # Only generate schema for root element matching this QName
```