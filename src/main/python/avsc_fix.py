import json
import sys
from collections import OrderedDict

import os


class AvroSchema:
    def __init__(self, file_path):
        with open(file_path) as file_in:
            root = json.load(file_in)
            self._root_name = root.get('name')
            self._level = 0
            self._known_types_list = []

            file_name = os.path.basename(file_path)
            self._root_prefix = file_name.split('.')[0]
            self._prefix = self._root_prefix
            self._base_fields = []

            for node in root.get('fields'):
                self._base_fields.append(Node(node))
            self._fields = self._base_fields

    # Recreate the schema splitting with specified element and save to new file
    def recreate_schema(self, split_by=None, new_file=None):
        self._known_types_list = []
        if split_by:
            if split_by != self._root_name:
                search_res = self._search(self._base_fields, split_by)
                if search_res:
                    search_res = search_res.content
                    search_res.name = split_by
                self._fields = search_res
            else:
                split_by = None

        if not self._fields:
            print 'Split element {} not found'.format(split_by)
            exit(1)
        schema = self._generate_schema(self._fields)

        if not split_by:
            schema['source'] = 'document'
        if new_file:
            with open(new_file, 'w') as file_out:
                json.dump(schema, file_out, indent=2)
        return schema

    def _generate_schema(self, node):
        schema = OrderedDict()
        # Generate schema for list of nodes
        if type(node) is list:
            inner_schema = []
            schema['type'] = 'record'
            schema['name'] = self._root_name
            for inner_node in node:
                inner_schema.append(self._generate_schema(inner_node))
            schema['fields'] = inner_schema
        else:
            # Generate schema for primitive types
            if node.node_type == Node.primitive_type:
                schema['name'] = node.name
                if node.optional:
                    schema['type'] = ['null', node.source_type]
                else:
                    schema['type'] = node.source_type
                schema['source'] = node.source
            # Generate schema for complex types
            elif node.node_type == Node.complex_type:
                sql_type = node.sql_type
                inner_type = OrderedDict()
                # primitive complexes
                if type(node.content) is str:
                    if sql_type == 'ARRAY':
                        inner_type['type'] = 'array'
                        if node.original_type:
                            inner_type['items'] = node.original_type
                        else:
                            inner_type['items'] = node.content
                    elif sql_type == 'MAP':
                        inner_type['type'] = 'map'
                        if node.original_type:
                            inner_type['values'] = \
                                node.original_type.split(',')[
                                    1].strip()
                        else:
                            inner_type['values'] = node.content.split(',')[
                                1].strip()
                    else:
                        inner_type['type'] = 'record'
                        inner_type['fields'] = node.content
                    schema['name'] = node.name
                    if node.optional:
                        schema['type'] = ['null', inner_type]
                    else:
                        schema['type'] = inner_type
                    if node.name != 'others':
                        schema['source'] = node.source
                # custom complexes
                else:
                    # Array
                    if sql_type == 'ARRAY':
                        inner_type['type'] = 'array'
                        inner_type['items'] = self._generate_schema(
                            node.content)
                        schema['name'] = node.name
                        if node.optional:
                            schema['type'] = ['null', inner_type]
                        else:
                            schema['type'] = inner_type
                        schema['source'] = node.source
                    # Map
                    elif sql_type == 'MAP':
                        inner_type['type'] = 'map'
                        inner_type['values'] = self._generate_schema(
                            node.content)
                        schema['name'] = node.name
                        if node.optional:
                            schema['type'] = ['null', inner_type]
                        else:
                            schema['type'] = inner_type
                        schema['source'] = node.source
                    # Struct
                    else:
                        schema['name'] = node.name
                        if node.optional:
                            schema['type'] = ['null', self._generate_schema(
                                node.content)]
                        else:
                            schema['type'] = self._generate_schema(node.content)
                        schema['source'] = node.source
            # Generate schema for custom defined types
            else:
                if node.name not in self._known_types_list:
                    schema = self._generate_schema(node.content)
                    self._known_types_list.append(node.name)
                    schema['name'] = node.name
                else:
                    schema = node.name
        return schema

    def _search(self, node, key):
        if type(node) is list:
            for inner_node in node:
                search_res = self._search(inner_node, key)
                if search_res:
                    break
        else:
            if node.node_type == Node.primitive_type:
                search_res = node if node.name == key else None
            elif node.node_type == Node.complex_type:
                # primitive complexes
                if type(node.content) is str:
                    search_res = node if node.name == key else None
                # custom complexes
                else:
                    if node.name == key:
                        search_res = node
                    else:
                        search_res = self._search(node.content, key)
            else:
                search_res = self._search(node.content, key)
        return search_res


class Node:
    primitive_type = 'PRIMITIVE'
    complex_type = 'COMPLEX'
    custom_type = 'CUSTOM'

    primitives_map = {'int': 'int', 'long': 'bigint', 'float': 'float',
                      'double': 'double', 'bytes': 'string',
                      'string': 'string', 'boolean': 'boolean'}
    type_dict = {}

    def __init__(self, node):
        self.sql_type = None
        self.content = None
        self.optional = False
        self.source = None
        self.comment = None
        self.original_type = None
        self.name = str(node.get('name'))
        node_type = node.get('type')

        # Parsing union - complex type and
        # take valid complex/primitive type in the union
        if type(node_type) is list:
            node_type = node_type[1]
            self.optional = True

        # Detect Primitives
        if node_type in Node.primitives_map.keys():
            self.node_type = Node.primitive_type
            self.sql_type = Node.primitives_map[node_type]
            self.source = str(node.get('source'))
            if 'comment' in node:
                self.comment = node['comment']
            self.source_type = node_type

        # Parse the inner record
        elif node_type == 'record':
            self.node_type = self.custom_type
            self.content = self._parse_list(node.get('fields'))
            self.source = str(node.get('source'))
            Node.type_dict[self.name] = self

        # Parse complex types
        else:
            self.node_type = self.complex_type
            self.sql_type, self.content = self._parse_complex_type(node_type)
            self.source = str(node.get('source'))

    # Parse a list of nodes
    @staticmethod
    def _parse_list(element_list):
        node_list = []
        for node in element_list:
            node_list.append(Node(node))
        return node_list

    # Parse a complex datatype
    def _parse_complex_type(self, node_type):
        if type(node_type) is dict:
            temp_type = node_type.get('type')
            # Parse array complex type
            if temp_type == 'array':
                items = node_type.get('items')

                # Array of primitives
                if items in Node.primitives_map.keys():
                    self.original_type = items
                    return 'ARRAY', Node.primitives_map[items]

                # Array of known custom types
                elif items in Node.type_dict.keys():
                    return 'ARRAY', Node.type_dict[items]

                # Array of new custom types
                else:
                    return 'ARRAY', Node(items)

            # Parse map complex type
            elif temp_type == 'map':
                value_type = node_type.get('values')

                # Map of primitives
                if value_type in Node.primitives_map.keys():
                    self.original_type = '{}, {}'.format('STRING', value_type)
                    return 'MAP', '{}, {}'.format('STRING', Node.primitives_map[
                        value_type])

                # Map of custom types
                else:
                    if value_type in Node.type_dict.keys():
                        value_type = self.type_dict[node_type]
                    else:
                        print '1 - {} type not found in the schema'.format(
                            node_type)
                        exit(1)
                    return 'MAP', '{}, {}'.format('STRING', value_type)

            # Parse other struct types
            else:
                return 'STRUCT', Node(node_type)

        elif node_type in Node.type_dict.keys():
            return 'STRUCT', self.type_dict[node_type]
        else:
            print '2 - {} type not found in the schema'.format(node_type)
            exit(1)

    def __repr__(self):
        optional = 'Optional' if self.optional else 'Mandatory'
        return '{}, {}, {}, {}, {}'.format(self.name, self.node_type,
                                           self.sql_type, optional, self.source)


file_path = sys.argv[1]
split_by = sys.argv[2]
temp = AvroSchema(file_path)
temp.recreate_schema(split_by=split_by, new_file=file_path)
