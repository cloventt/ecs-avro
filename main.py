import json
import logging
from pathlib import Path
from typing import Union

import avro.schema
import yaml

SCHEMA_PATH_FLAT = Path(__file__).resolve(strict=True).parent / 'ecs' / 'generated' / 'ecs' / 'ecs_flat.yml'

TYPE_MAPPING = {
    'keyword': 'string',
    'date': 'string',
    'ip': 'string',
    'flattened': 'string',
    'nested': 'string',
    'match_only_text': 'string',
    'constant_keyword': 'string',
    'wildcard': 'string',
    'long': 'long',
    'double': 'double',
    'float': 'float',
    'boolean': 'boolean',
    'scaled_float': 'float',
    'geo_point': {
        'type': 'record',
        'name': 'GeoPoint',
        'fields': [
            {
                'name': 'lon',
                'type': 'float'
            },
            {
                'name': 'lat',
                'type': 'float'
            }
        ]
    },
    'object': {
        'type': 'map',
        'values': 'string',
        'default': {}
    }
}


def load_schema(path: Path) -> dict:
    try:
        with open(path, 'r') as schema_file:
            schema = yaml.load(schema_file, Loader=yaml.Loader)
    except FileNotFoundError:
        logging.error("Failed to load the schema file, try running: git submodule update")
    return schema


def get_type(conf: dict):
    def map_type(c: dict):
        t = c.get('type')

        return_value = TYPE_MAPPING.get(t)
        if t == 'geo_point' and type(TYPE_MAPPING.get('geo_point')) == dict:
            TYPE_MAPPING['geo_point'] = 'io.github.cloventt.ecs.GeoPoint'
        return return_value

    mapped_type = map_type(conf)

    if 'array' in conf.get('normalize'):
        mapped_type = {'type': 'array', 'items': mapped_type}

    if conf.get('required', False):
        result = mapped_type
    else:
        result = ['null', mapped_type]
    return result


def convert_flat_schema(schema: dict) -> dict:
    """
    Convert the provided ECS flat schema dict into a valid Avro schema.
    :param schema: the schema to convert
    :return: a dict containing a valid Avro schema
    """
    result = {
        'type': 'record',
        'name': 'ElasticCommonSchemaRecord',
        'namespace': 'io.github.cloventt.ecs',
        'fields': []
    }
    for key, field_conf in schema.items():
        result['fields'].append(
            {
                'name': field_conf['dashed_name'],
                'doc': field_conf['description'],
                'type': get_type(field_conf)
            }
        )
    return result


def validate_schema(schema_to_validate: str):
    avro.schema.parse(schema_to_validate)


if __name__ == '__main__':
    input_schema = load_schema(SCHEMA_PATH_FLAT)
    converted_schema = convert_flat_schema(input_schema)
    validate_schema(json.dumps(converted_schema))

    with open(Path(__file__).resolve(strict=True).parent / 'elastic-common-schema.avsc', 'w') as output_file:
        json.dump(converted_schema, output_file, indent=2)
