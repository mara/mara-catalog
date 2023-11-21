from enum import Enum
import json
import pathlib
from typing import Optional, Union, Iterator, Dict

from mara_db import formats
from ..catalog import DbCatalog, StorageCatalog
from ..formats import DeltaFormat, HudiFormat


class DiscoverMode(Enum):
    DB = 'db'
    STORAGE = 'storage'


def dbt_file_format_to_format(file_format: str) -> formats.Format:
    """Returns a mara format from a dbt model 'file_format' config parameter"""
    if file_format == 'parquet':
        return formats.ParquetFormat()
    elif file_format == 'delta':
        return DeltaFormat()
    elif file_format == 'hudi':
        return HudiFormat()
    elif file_format == 'csv':
        return formats.CsvFormat()
    elif file_format == 'orc':
        return formats.OrcFormat()
    else:  # json,text,jdbc,hive,libsvm
        raise ValueError(f'The dbt file format "{file_format}" is not supported')


def discover_tables_from_dbt_manifest(manifest_file: str, discover_mode: DiscoverMode, base_path: Optional[str] = None) -> Iterator[Dict]:
    """
    Reads the tables from a dbt manifest file.

    Args:
        manifest_file: The local path to the db manifest file.
        discover_mode: The type of table discovery.
        base_path: When discover_mode is STORAGE, give here the base path which should be removed
                   from the model config parameter 'location_root'
    """
    manifest = {}
    with open(manifest_file, mode='r', encoding='utf-8') as json_file:
        manifest = json.load(json_file)

    for node_name, node_dict in manifest['nodes'].items():
        if not node_name.startswith('model.'):
            continue

        node_config = node_dict['config']

        if not node_config.get('enabled', False):
            continue

        if discover_mode == DiscoverMode.STORAGE:
            if node_config.get('materialized', None) == 'view':
                # views are not stored on storage, so we skip them here
                continue

        table_name = node_dict.get('alias', node_dict['name'])
        schema_name = node_dict['schema']

        if discover_mode == DiscoverMode.DB:
            yield {
                'name': table_name,
                'schema': schema_name,
                'location': None,
                'format': None
            }

        elif discover_mode == DiscoverMode.STORAGE:
            file_format = node_config.get('file_format', None)
            if not file_format:
                # Skip tables with undefined file format. They are probably managed by the db engine.
                # We select only unmanaged tables. There the storage is user defined.
                continue

            location = node_config.get('location_root', None)

            # remove leading 'base_path' part
            if base_path and location.startswith(base_path):
                location = location[len(base_path):]
                if location.startswith('/'):
                    location = location[1:]

            # remove ending '/'
            if location.endswith('/'):
                location = location[:-1]

            yield {
                'name': table_name,
                'schema': schema_name,
                'location': f"{location}/{table_name}" if location else table_name,
                'format': dbt_file_format_to_format(file_format)
            }



class DbtDbCatalog(DbCatalog):
    def __init__(self, manifest_file: str, db_alias: str, has_schemas: bool = False, default_schema: str = 'public') -> None:
        super().__init__(db_alias=db_alias, has_schemas=has_schemas, default_schema=default_schema)
        self.manifest_file = manifest_file

    def discover(self):
        tables = []
        for table in discover_tables_from_dbt_manifest(self.manifest_file, discover_mode=DiscoverMode.DB):
            tables.append(table)
        self.tables = tables


class DbtStorageCatalog(StorageCatalog):
    def __init__(self, manifest_file: str, storage_alias: Optional[str] = None, base_path: Optional[Union[str, pathlib.Path]] = None, has_schemas: bool = False, default_schema: str = 'public') -> None:
        super().__init__(storage_alias=storage_alias, base_path=base_path, has_schemas=has_schemas, default_schema=default_schema)
        self.manifest_file = manifest_file

    def discover(self, base_path: str = None):
        """
        Discover storage tables from the dbt manifest file.

        Args:
            base_path: The base path which should be removed from the model config parameter 'location_root'
        """
        tables = []
        for table in discover_tables_from_dbt_manifest(self.manifest_file, discover_mode=DiscoverMode.STORAGE, base_path=base_path):
            tables.append(table)
        self.tables = tables
