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
3. `java -jar ./build/libs/xml-avro-all-<VERSION>.jar -c <CONFIG_FILE>` to run the code (options as below)

Check `./example/config.yml` for sample configuration file

### Config File
Create yml config file as per the below format
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
  stringTimestamp: true         # Represent timestamp as string instead of long. Defaults to false. Setting this value to "true" overrides XSD.logicalTypes.xsDateTime to "string".
  attributePrefix: "_"          # Optional, will assign the specified prefix for attributes in the avsc schema

  ignoreHiveKeywords: true      # Do not suffix field name with `_value` when matching Hive keywords. Default value is false.
  rootElementQName: "{ns}name"  # Only generate schema for root element matching this QName
  logicalTypes:
    xsDateTime: "long"          # Configures the Avro mapping of xs:dateTime XML types. [ long | string | timestamp-micros | timestamp-millis ]
                                #   "long" (the default) maps xs:dateTime types to regular Avro "long". Same as the default mapping for xs:dateTime in older xml-avro versions. 
                                #   "string" maps xs:dateTime types to Avro "string"
                                #   "timestamp-micros" maps xs:dateTime types to Avro "timestamp-micros" logical type annotating a "long". 
                                #   "timestamp-millis" maps xs:dateTime types to Avro "timestamp-millis" logical type annotating a "long".
                                #   Note: Setting the stringTimestamp will override this config value to "string" for backward compatibility reasons. 
    xsDate: "string"            # Configures the Avro mapping of xs:date XML types. [ string | date ].
                                #   "string" (the default) maps xs:date types to Avro "string"
                                #   "date" maps xs:date types to Avro "date" logical type annotating an "int".  
    xsTime: "string"            # Configures the Avro mapping of xs:time XML types. [ string | time-micros | time-millis ]
                                #   "string" (the default) maps xs:time types to Avro "string".
                                #   "time-micros" maps xs:time types to Avro "time-micros" logical type annotating a "long". 
                                #   "time-millis" maps xs:time types to Avro "time-millis" logical type annotating a "long".
                                #
    xsDecimal:                  # Configurations controlling the mapping of xs:decimal XML types
                                #
     avroType: "decimal"        #   Configures the Avro type mapping of xs:Decimal derived xml types.
                                #   Possible values are: [ double | string | decimal ]
                                #   - "double" (the default) maps xs:decimal types to Avro "double".
                                #   - "string" maps xs:decimal types to Avro "string".
                                #   - "decimal" maps xs:decimal types to Avro "decimal" logical types annotating "bytes".
                                #   When using the "decimal" option, the mandatory precision and scale properties of the Avro
                                #   decimal type are picked up from any xs:totalDigits and xs:fractionDigits restriction facets, if any.
                                #   In the absense of these restriction facets, the mapping will instead fall back to using a backup strategy defined
                                #   by a combination of the fallbackType, fallbackPrecision and fallbackScale configurations. 
                                #     
     fallbackType: "string"     # Configures a fallback type mapping for xs:decimal types with unrestricted precision and scale. (i.e. types without
                                #   declared xs:totalDigits and xs:fractionDigits restriction facets). This configuration is ignored, unless the
                                #   avroType setting is configured to "decimal".
                                #   The possible values are: [ string | double | decimal ]
                                #   All options are identical to those described under the avroType configuration, the only exception being 
                                #   "decimal" that uses the fallbackPrecision and fallbackScale configurations as a defaults for missing
                                #   precision and scale information.
                                #   
     fallbackPrecision: 5       #  Configures the fallback precision for decimal types without declared xs:totalDigits
                                #    and restriction. Required when fallbackType is set to "decimal".
                                #
     fallbackScale: 3           #  Configures the fallback scale for decimal types without declared xs:fractionDigits restriction.
                                #   Required when fallbackType is set to "decimal".
```