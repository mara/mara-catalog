"""Functions to export tables from a database to a storage"""

from typing import Iterator, Union, List

from mara_pipelines.pipelines import Command
from mara_db import formats
from .catalog import DbCatalog
from .schema import WriteSchema

## TBD
from app.pipelines.transfer_to.write_file import WriteFile


def export_catalog_mara_commands(catalog: DbCatalog, storage_alias: str, base_path: str,
    format: formats.Format, write_schema_file: bool = False, include: List[str] = None,
    db_alias: str = None) -> Iterator[Union[Command, List[Command]]]:
    """
    Returns pipeline tasks which exports a catalog to a storage.

    Args:
        catalog: The catalog to be exported
        storage_alias: the storage where the tables shall be exported to
        base_path: the base path
        format: the format as it should be exported
        write_schema_file: if a sqlalchemy schema file shall be added into the table directory.
        list: if you want to include only a predefined list of tables, pass over a list of table names here.
              This is applied accross schemas since schema selection is not yet supported. (TODO Might be changed in the future.)
    """

    for table in catalog.tables:
        table_name = table['name']
        schema_name = table.get('schema', catalog.default_schema)
        if include:
            if not table_name in include:
                # skip tables defined in include
                continue
        table_path = f'{base_path}/{schema_name}/{table_name}' if catalog.has_schemas else f'{base_path}/{table_name}'
        #yield Task(id=table_to_id(schema_name, table_name),
        #           description=f"Export table {schema_name}.{table_name} to storage {storage_alias}",
        #           commands=
        yield      [
                       # TBD: when format is parquet, delete the folder content first
                       WriteFile(sql_statement=f'SELECT * FROM "{schema_name}"."{table_name}"',
                                 db_alias=db_alias or catalog.db_alias,
                                 storage_alias=storage_alias,
                                 dest_file_name=clean_hadoop_path(f'{table_path}/part.0.parquet'),  # TODO generic format ending would be nice here
                                 compression=('snappy' if isinstance(format, formats.ParquetFormat) else None),
                                 format=format)
                   ] + ([
                       WriteSchema(table_name,
                                   schema=schema_name,
                                   db_alias=db_alias or catalog.db_alias,
                                   storage_alias=storage_alias,
                                   file_name=f'{clean_hadoop_path(table_path)}/_sqlalchemy_metadata.py')
                   ] if write_schema_file else [])
        #,
        #           max_retries=2)


def table_to_id(schema_name, table_name) -> str:
    return f'{schema_name}_{table_name}'.lower()


def clean_hadoop_path(path) -> str:
    """
    Hadoop hides paths starting with '_' and '.'. With this function paths for tables are renamed so that they can be read via Hadoop.
    """
    parts = []
    for part in str(path).split('/'):
        if part.startswith('_') or part.startswith('.'):
            parts.append('x' + part)
        else:
            parts.append(part)

    return '/'.join(parts)
