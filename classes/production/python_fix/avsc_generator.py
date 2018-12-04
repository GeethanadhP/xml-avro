import json
import os
import sys

from avsc_fix import AvroSchema


def setup(config):
    # xsd_file = config['XSD_FILE']
    jar_file = config['XML_AVRO_JAR']
    local_avsc = config['AVRO_SCHEMA_LOCATION']
    hdfs_avsc = config['AVRO_SCHEMA_HDFS']
    avsc_file_name = os.path.basename(local_avsc)
    hdfs_folder = os.path.dirname(hdfs_avsc)
    split_by = config['XML_SPLIT_BY']
    avsc_config = config['AVSC_CONVERT_CONFIG']
    mkdirs(os.path.dirname(local_avsc))

    run_external_cmd('java',
                     arguments=['-jar', jar_file, '-c', avsc_config])

    backup_avsc = os.path.join(os.path.dirname(local_avsc),
                               avsc_file_name + '.bak')
    if os.path.exists(backup_avsc):
        os.remove(backup_avsc)
    print(
        'Backing up avsc - renaming {} to {}'.format(backup_avsc,
                                                     local_avsc))
    os.rename(local_avsc, backup_avsc)
    print('Loading the avsc from ' + backup_avsc)

    use_flume_avro = config.selected('USE_FLUME_AVRO_INPUT')
    if use_flume_avro:
        header_col_name = config['FLUME_AVRO_HEADER_COLUMN']
        extra_schema = '''
                {
                  "name": "{}",
                  "type": [ "null", {
                    "type": "map",
                    "values": "string"
                  }]
                }
                '''
        extra_schema = json.loads(extra_schema)
        extra_schema['name'] = extra_schema['name'].format(header_col_name)
        print('Adding additional schema {}'.format(extra_schema))
        temp = AvroSchema(backup_avsc, extra_schema)
    else:
        temp = AvroSchema(backup_avsc)

    print('Saving the avsc to {} after updating for '
          'split by {}'.format(local_avsc, split_by))
    temp.recreate_schema(split_by=split_by, new_file=local_avsc)
