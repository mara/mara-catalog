from functools import singledispatch
import pathlib
from typing import Iterable, List, Union, Tuple, Dict
from sqlalchemy import Table, MetaData

from mara_db import dbs, formats
import mara_db.sqlalchemy_engine
import mara_storage.storages
from mara_pipelines.pipelines import Command
from mara_pipelines.commands.sql import ExecuteSQL

from ._pyarrow import pyarrow_filesystem
from .catalog import StorageCatalog
from .ddl import create_external_table
from .formats import DeltaFormat
from .table import detect_table_schema


@singledispatch
def format_to_ddl_format(db: Union[str, dbs.DB], table_format: formats.Format) -> Tuple[str, Dict[str, str]]:
    """
    Converts a format into a ddl specific format_name and format_options.

    Args:
        db: the database as alias or class for which the format_name and options dict
            shall be created
        table_format: the mara format with the custom settings

    Returns:
        A tuble which holds the type name for the specific database and a dictionary with
        db specific options which are used when accessing the table.
    """
    raise NotImplementedError(
        f'Please implement mara_format_to_tuple for types "{db.__class__.__name__}" and "{table_format.__class__.__name__}"'
    )


@format_to_ddl_format.register(str)
def __(db_alias: str, table_format: formats.Format) -> Tuple[str, Dict[str, str]]:
    return format_to_ddl_format(dbs.db(db_alias), table_format=table_format)


@format_to_ddl_format.register(dbs.SQLServerDB)
def __(db: dbs.SQLServerDB, table_format: formats.Format) -> Tuple[str, Dict[str, str]]:
    """
    SQL Server in its dialects (Polybase, Azure Synapse etc.) uses a extra
    function CREATE EXTERNAL FILE FORMAT to define its file format. We assume
    here that these file formats have already been created in their most meaningful
    way.
    """
    if isinstance(table_format, formats.ParquetFormat):
        return ('PARQUET', {})
    elif isinstance(table_format, formats.OrcFormat):
        return ('ORC', {})
    #elif isinstance(format, formats.Json):  # not supported
    elif isinstance(table_format, DeltaFormat):
        return ('DELTA', {})
    elif isinstance(table_format, formats.CsvFormat):
        if table_format.header:
            raise ValueError('A CSV format with header is not supported for SQL Server')
        if table_format.delimiter_char == ',':
            return ('CSV', {})
        elif table_format.delimiter_char == '\t':
            return ('TSV', {})
        else:
            raise ValueError('Only CSV with delimiter_char , or \\t is supported for SQL Server')
    else:
        raise NotImplementedError(f'The format {table_format} is not supported for SQLServerDB')


@format_to_ddl_format.register(dbs.DatabricksDB)
def __(db: dbs.DatabricksDB, table_format: formats.Format) -> Tuple[str, Dict[str, str]]:
    if isinstance(table_format, formats.ParquetFormat):
        return ('PARQUET', {})
    elif isinstance(table_format, formats.OrcFormat):
        return ('ORC', {})
    elif isinstance(table_format, formats.JsonlFormat):
        return ('JSON', {})
    elif isinstance(table_format, DeltaFormat):
        return ('DELTA', {})
    elif isinstance(table_format, formats.CsvFormat):
        options = {}
        if table_format.header is not None:
            options['header'] = 'true' if table_format.header else 'false'
        if table_format.delimiter_char is not None:
            options['sep'] = f"'{table_format.delimiter_char}'"
        if table_format.quote_char is not None:
            if table_format.quote_char == '\'':
                options['quote'] = "'\''"
            elif table_format.quote_char == '':
                options['quote'] = 'null'
            else:
                options['quote'] = f"'{table_format.quote_char}'"
        if table_format.null_value_string:
            options['nullValue'] = f"'{table_format.null_value_string}'"
        return ('CSV', options)
    else:
        raise NotImplementedError(f'The format {table_format} is not supported for DatabricksDB')


@format_to_ddl_format.register(dbs.SnowflakeDB)
def __(db: dbs.SnowflakeDB, table_format: formats.Format) -> Tuple[str, Dict[str, str]]:
    if isinstance(table_format, formats.CsvFormat):
        options = {
            'TYPE': 'CSV'
        }
        if table_format.header:
            options['SKIP_HEADER'] = '1'
        if table_format.delimiter_char is not None:
            options['FIELD_DELIMITER'] = f"'{table_format.delimiter_char}'"
        if table_format.quote_char:
            if table_format.quote_char == '\'':
                options['quote'] = "0x27"
            else:
                options['quote'] = f"'{table_format.quote_char}'"
        if table_format.null_value_string:
            options['NULL_IF'] = f"('{table_format.null_value_string}')"
        options['CSV']
        return ('CSV', {'FILE_FORMAT': '( '
                                       + ', '.join([f'{k} = {v}' for k, v in options.items()])
                                       + ' )'})
    elif isinstance(table_format, formats.JsonlFormat):
        return ('JSON', {})
    elif isinstance(table_format, formats.AvroFormat):
        return ('AVRO', {})
    elif isinstance(table_format, formats.OrcFormat):
        return ('ORC', {})
    elif isinstance(table_format, formats.ParquetFormat):
        return ('PARQUET', {})
    else:
        raise NotImplementedError(f'The format {table_format} is not supported for SnowflakeDB')


def connect_catalog_mara_commands(catalog: Union[str, StorageCatalog], db_alias: str,
        or_replace: bool = False) -> Iterable[Union[Command, List[Command]]]:
    """
    Returns a list of commands which connects a table list as external storage.

    Args:
        db_alias: The database alias where the tables shall be connected to.
        storage_alias: The storage alias to be used.
        or_replace: If True, the commands will call CREATE OR REPLACE EXTERNAL TABLE instead of just creating the table (`CREATE EXTERNAL TABLE`).
    """
    if isinstance(catalog, str):
        print(f'catalog by alias: {catalog}')
        from . import config
        catalog = config.catalogs()[catalog]

    storage = mara_storage.storages.storage(catalog.storage_alias)
    engine = mara_db.sqlalchemy_engine.engine(db_alias)
    fs = pyarrow_filesystem(storage)
    path_prefix = ''
    if isinstance(storage, mara_storage.storages.AzureStorage):
        path_prefix = storage.container_name
    else:
        raise NotImplementedError(f'Not supported storage type: {storage.__class__}')

    for table in catalog.tables:
        table_name = table['name']
        schema_name = table.get('schema', catalog.default_schema)
        table_format = table.get('format', None)
        location = table.get('location', None)
        if location is None:
            raise Exception(f'Location of table `{schema_name}`.`{table_name}` not defined')
        location = str(catalog.base_path / location)

        format_name, format_options = format_to_ddl_format(db_alias, table_format=table_format)

        print(f'path: {format_name}.`{location}`')

        sqlalchemy_table = None

        metadata_file = str(pathlib.Path(path_prefix) / location / '_sqlalchemy_metadata.py')
        if fs.isfile(metadata_file):
            _globals = {}
            _locals = {}
            exec(fs.open(metadata_file).read(), _globals, _locals)

            if 'metadata' not in _locals or not _locals['metadata']:
                print('skip _sqlalchemy_metadata.py: Could not find variable metadata')
            else:
                for table in _locals['metadata'].tables:
                    print(f'take table {table} from _sqlalchemy_metadata')
                    sqlalchemy_table = _locals['metadata'].tables[table]
                    sqlalchemy_table.name = table_name
                    sqlalchemy_table.schema = schema_name
                    break

        if sqlalchemy_table is None:
            if engine.dialect.name == 'databricks' and (\
                    isinstance(table_format, DeltaFormat) or \
                    isinstance(table_format, formats.ParquetFormat)):
                # dialect databricks do not require a sqlalchemy table with columns,
                # so we create just an raw table here.
                sqlalchemy_table = Table(
                    table_name,
                    MetaData(),
                    schema=schema_name)

            else:
                print('read metadata from storage ...')
                sqlalchemy_table = detect_table_schema(table_format,
                    path=location,
                    storage_alias=catalog.storage_alias,
                    table_name=table_name,
                    schema_name=schema_name)

        sql_statement = create_external_table(bind=engine,
            table=sqlalchemy_table, storage_name=catalog.storage_alias, path=location,
            format_name=format_name, or_replace=or_replace, options=format_options)

        yield ExecuteSQL(sql_statement, db_alias=db_alias)

        #yield Task(
        #    id=table_to_id(schema_name, table_name),
        #    description=f"Connect table {schema_name}.{table_name} to db {db_alias}",
        #    commands=[ExecuteSQL(sql_statement, db_alias=db_alias)])


def table_to_id(schema_name, table_name) -> str:
    return f'{schema_name}_{table_name}'.lower()
