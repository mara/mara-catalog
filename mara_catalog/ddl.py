"""
Helper functions to generate DDL code for creating storage based tables for a database
engine using the SQLAlchemy engine.

This code is required because SQLAlchemy does not (yet) support the creation of storage
based tables.
"""

from sqlalchemy import Table
from sqlalchemy import Integer, TIMESTAMP, String, BINARY
import sqlalchemy.engine
from typing import List, Union, Dict

import mara_db.formats
import mara_storage.storages
from mara_storage.compression import Compression

from .formats import DeltaFormat


MSSQL_DATA_COMPRESSION_MAP = {
    Compression.NONE: None,
    Compression.GZIP: 'org.apache.hadoop.io.compress.GzipCodec',
    'snappy': 'org.apache.hadoop.io.compress.SnappyCodec',
}

def create_file_format(bind: sqlalchemy.engine.Engine, name: str, format: mara_db.formats.Format,
                       compression: Compression = Compression.NONE,
                       or_replace: bool = False, if_not_exists: bool = False) -> str:
    """
    A function creating an CREATE [EXTERNAL] FILE FORMAT statement from a mara_db file format definition.

    Args:
        bind: The SQLAlchemy engine
        name: the name for the file format in SQL
        format: the mara_db file format definition
        compression: the compression for the file
        or_replace: if the statement shall look like 'CREATE OR REPLACE EXTERNAL TABLE' if the db engine does not support 'OR REPLACE' the object is dropped first
        if_not_exists: if the statement shall look like 'CREATE EXTERNAL TABLE IF EXISTS'
    """
    if or_replace and if_not_exists:
        raise ValueError('You cannot use or_replace and if_not_exists at the same time')

    # sqlalchemy dialect
    dialect = bind.dialect

    if dialect.name == 'snowflake':
        format_options = []

        if isinstance(format, mara_db.formats.CsvFormat):
            format_type = 'CSV'
        elif isinstance(format, mara_db.formats.AvroFormat):
            format_type = 'AVRO'
        elif isinstance(format, mara_db.formats.ParquetFormat):
            format_type = 'PARQUET'
        elif isinstance(format, mara_db.formats.OrcFormat):
            format_type = 'OCR'
        else:
            # Note: JSON and XML is not implemented yet, see html
            raise ValueError('Not supported format for Snowflake')

        return ('CREATE '
            + ('OR REPLACE ' if or_replace else '')
            + 'FILE FORMAT '
            + ('IF NOT EXISTS ' if if_not_exists else '')
            + dialect.identifier_preparer.format_schema('public')
            + '.'
            + dialect.identifier_preparer.quote_identifier(name)
            + '\n\t'
            + '\n\t'.join([f"TYPE = '{format_type}'"] + format_options)
            + ';')
    elif dialect.name == 'mssql':
        format_options = None

        if isinstance(format, mara_db.formats.CsvFormat):
            if compression not in [Compression.GZIP, Compression.NONE]:
                raise ValueError(f'Not supported compression for function create_external_file_format {compression} for format: {format}')

            format_type = 'DELIMITEDTEXT'

            format_options = []

            if format.delimiter_char:
                if format.delimiter_char == '\t':
                    format_options.append(f"FIELD_TERMINATOR = '\\t'")
                else:
                    format_options.append(f"FIELD_TERMINATOR = '{format.delimiter_char}'")
            if format.quote_char:
                if format.quote_char == '"':
                    format_options.append(f"STRING_DELIMITER = '0x22'")
                else:
                    format_options.append(f"STRING_DELIMITER = '{format.quote_char}'")
            if format.header:
                format_options.append(f"FIRST_ROW = 2")

            format_options.append("ENCODING = 'UTF8'")
            format_options.append("USE_TYPE_DEFAULT = FALSE")

            format_options = ', '.join(format_options)
        elif isinstance(format, mara_db.formats.OrcFormat):
            if compression not in ['snappy', Compression.NONE]:
                raise ValueError(f'Not supported compression for function create_external_file_format {compression} for format: {format}')

            format_type = 'ORC'
        elif isinstance(format, mara_db.formats.ParquetFormat):
            if compression not in ['snappy', Compression.NONE]:
                raise ValueError(f'Not supported compression for function create_external_file_format {compression} for format: {format}')

            format_type = 'PARQUET'
        elif isinstance(format, DeltaFormat):
            format_type = 'DELTA'
        else:
            raise ValueError(f'Not supported file format for function create_external_file_format: {format}')

        if compression in MSSQL_DATA_COMPRESSION_MAP:
            compression_str = MSSQL_DATA_COMPRESSION_MAP[compression]
        else:
            raise ValueError(f'Not supported compression: {compression}')

        return (f"""IF EXISTS (SELECT * FROM sys.external_file_formats WHERE name = '{name}')
    DROP EXTERNAL FILE FORMAT {dialect.identifier_preparer.quote_identifier(name)};
""" if or_replace else '') + \
            (f"""IF NOT EXISTS (SELECT * FROM sys.external_file_formats WHERE name = '{name}')
""" if if_not_exists else '') + \
            (f"CREATE EXTERNAL FILE FORMAT {dialect.identifier_preparer.quote_identifier(name)}"
            + f"\nWITH ("
            + f"\n\tFORMAT_TYPE = {format_type}"
            + (f"\n\t, FORMAT_OPTIONS({format_options})" if format_options else "")
            + (f"\n\t, DATA_COMPRESSION = '{compression_str}'" if compression_str else "")
            + "\n);")
    elif dialect.name == 'databricks':
        return ';' # Databricks does not support to define external file formats. The file format is described directly e.g. when using CREATE TABLE ... USE <format_name>
    else:
        raise NotImplementedError(f'Not supported for SQL dialect {dialect.name}')


def create_external_storage(bind: sqlalchemy.engine.Engine, name: str, storage: Union[str, mara_storage.storages.Storage],
                            or_replace: bool = False, if_not_exists: bool = False, or_update: bool = None) -> str:
    """
    A function creating an external storage connection according to the database engine respective syntax, e.g. CREATE DATA SOURCE or CREATE STAGE statement.
    It uses the mara storage definition as basis.

    Args:
        bind: The SQLAlchemy engine
        name: the name for the file format in SQL
        format: the mara_db file format definition
        compression: the compression for the file
        or_replace: if the statement shall look like 'CREATE OR REPLACE EXTERNAL TABLE' if the db engine does not support 'OR REPLACE' the object is dropped first
        if_not_exists: if the statement shall look like 'CREATE EXTERNAL TABLE IF EXISTS'
        or_update: if the statement shall update mutable metadata e.g. the storage credentials etc. if the storage already exists. When `None` it is `True` when or_replace is not True.
    """
    if or_replace and if_not_exists:
        raise ValueError('You cannot use or_replace and if_not_exists at the same time')
    if or_replace and or_update:
        raise ValueError('You cannot use or_replace and or_update at the same time')
    if or_update is None:
        or_update = not or_replace

    if isinstance(storage, str):
        storage = mara_storage.storages.storage(storage)

    # sqlalchemy dialect
    dialect = bind.dialect

    if dialect.name == 'snowflake':
        params = []

        if isinstance(storage, mara_storage.storages.AzureStorage):
            params.append(f"URL = 'azure://{storage.account_name}.{storage.storage_type}.core.windows.net/{storage.container_name}'")
            if storage.sas:
                params.append(f"CREDENTIALS = (AZURE_SAS_TOKEN = '{storage.sas}')")
            else:
                raise ValueError('The azure storage must have an sas token to be able to use it with Snowflake')
        else:
            raise ValueError(f'Not supported storage for create_external_storage: {storage.__class__.__name__}')

        return ('CREATE STAGE '
            + ('IF NOT EXISTS ' if if_not_exists else '')
            + dialect.identifier_preparer.format_schema('public')
            + '.'
            + dialect.identifier_preparer.quote_identifier(name)
            + ('\n\t' if params else '')
            + '\n\t'.join(params)
            + ';')

    elif dialect.name == 'mssql':
        assert isinstance(storage, mara_storage.storages.AzureStorage)

        storage_type = None # Azure Synapse does not support the 'TYPE = HADOOP' option
        #storage_type = 'HADOOP' # NOTE: BLOB_STORAGE is currently not supported with this function

        if storage.sas:
            identity = 'SHARED ACCESS SIGNATURE'
            secret = storage.sas
        #elif storage.spa_client_secret:
        #    identity = f'{storage.spa_application}@https://login.microsoftonline.com/organizations/oauth2/token'
        #    secret = storage.spa_client_secret
        else:
            raise ValueError(f"The method create_external_storage currently only support Azure storages with SAS token.")

        credential_name = f'{name}__CREDENTIALS'

        return (f"""
IF EXISTS (SELECT * FROM sys.external_data_sources WHERE name = '{name}')
    DROP EXTERNAL DATA SOURCE {dialect.identifier_preparer.quote_identifier(name)};
""" if or_replace else '') + \
\
            (f"""
IF EXISTS (SELECT * FROM sys.database_credentials WHERE name = '{credential_name}')
	DROP DATABASE SCOPED CREDENTIAL {dialect.identifier_preparer.quote_identifier(credential_name)};
""" if or_replace else '') + \
            (f"""
IF NOT EXISTS (SELECT * FROM sys.database_credentials WHERE name = '{credential_name}')
""" if if_not_exists or or_update else '') + \
            (f"""CREATE DATABASE SCOPED CREDENTIAL {dialect.identifier_preparer.quote_identifier(credential_name)}
WITH
    IDENTITY = '{identity}',
    SECRET = '{secret}'\n""") + \
            (f"""ELSE
ALTER DATABASE SCOPED CREDENTIAL {dialect.identifier_preparer.quote_identifier(credential_name)}
WITH
    IDENTITY = '{identity}',
    SECRET = '{secret}'\n""" if or_update else '') + \
            ('\n') +\
\
            (f"""IF NOT EXISTS (SELECT * FROM sys.external_data_sources WHERE name = '{name}')\n""" if if_not_exists else '') + \
            (f"CREATE EXTERNAL DATA SOURCE {dialect.identifier_preparer.quote_identifier(name)} WITH ("
            + f"LOCATION = '{storage.base_uri}'"
            + f', CREDENTIAL = {dialect.identifier_preparer.quote_identifier(credential_name)}'
            + (f', TYPE = {storage_type.upper()}' if storage_type else '')
            + ')')
    elif dialect.name == 'databricks':
        credential_name = f'{name}__CREDENTIALS'

        if isinstance(storage, mara_storage.storages.AzureStorage):
            if storage.storage_type == 'dfs':
                location_url = f'abfss://{storage.container_name}@{storage.account_name}.dfs.core.windows.net/'
            elif storage.storage_type == 'blob':
                location_url = f'wasbs://{storage.container_name}@{storage.account_name}.blob.core.windows.net/'
            else:
                raise ValueError(f'Not supported storage type for AzureStorage. storage_type must be blob or dfs')
        else:
            raise ValueError(f'Not supported storage for SQL dialect {dialect.name}: {storage.__class__.__name__}')

        return ('CREATE EXTERNAL LOCATION '
            + ('IF NOT EXISTS ' if if_not_exists else '')
            + dialect.identifier_preparer.quote_identifier(name)
            + f" URL '{location_url}'"
            + f' WITH (STORAGE CREDENTIAL {credential_name})'
            + ';')
    else:
        raise NotImplementedError(f'Not supported for SQL dialect {dialect.name}')


def create_external_table(bind: sqlalchemy.engine.Engine, table: Table, storage_name: str, path: str, format_name: str,
                          partition_by: List[str] = None, or_replace: bool = False, if_not_exists:bool = False, options: Dict[str, str] = {}) -> str:
    """
    A function creating an CREATE EXTERNAL TABLE statement from a SQLAlchemy table.

    Args:
        bind: The SQLAlchemy engine
        table: The SQLAlchemy table object
        storage_name: the file storage name
        path: the path on the storage
        format_name: the format name of the file, e.g. PARQUET. Needs to be created first in the db engine
        partition_by: In case the table is partitioned a list of the partition columns
        or_replace: if the statement shall look like 'CREATE OR REPLACE EXTERNAL TABLE'
        if_not_exists: if the statement shall look like 'CREATE EXTERNAL TABLE IF EXISTS'
        options: Additinal options for a table.
    """
    if or_replace and if_not_exists:
        raise ValueError('You cannot use or_replace and if_not_exists at the same time')

    # sqlalchemy dialect
    dialect = bind.dialect

    if dialect.name == 'mssql':
        with bind.connect() as c:
            # makes sure that the dialect is initalized so that e.g. server version info is loaded.
            dialect.initialize(c)

    columns = []  # only the column names
    column_definitions = []  # the column definitions
    for column in table.columns:
        if dialect.name == 'snowflake' and column.name.lower() == 'value':
            col = dialect.identifier_preparer.quote_identifier(column.name + '_')
        else:
            col = dialect.identifier_preparer.format_column(column)

        columns.append(col)

        column_type = column.type
        if dialect.name == 'databricks':
            import sqlalchemy.dialects.mssql

            # map MSSQL types to SQLAlchemy types
            if isinstance(column_type, sqlalchemy.dialects.mssql.TINYINT):
                column_type = Integer()
            elif isinstance(column_type, sqlalchemy.dialects.mssql.DATETIME2):
                column_type = TIMESTAMP()
            elif isinstance(column_type, sqlalchemy.dialects.mssql.DATETIMEOFFSET):
                column_type = TIMESTAMP()
            elif isinstance(column_type, sqlalchemy.dialects.mssql.UNIQUEIDENTIFIER):
                column_type = String()
            elif isinstance(column_type, sqlalchemy.dialects.mssql.TIMESTAMP):
                column_type = BINARY()

            if isinstance(column_type, BINARY):
                column_type = BINARY() # remove the length parameter --> not supported in databricks/spark
        elif dialect.name == 'mssql':
            if isinstance(column_type, BINARY):
                if column.name == 'timestamp' and not column_type.length:
                    # Expect the column to be a Rowversion or Timestamp column which has a binary length of 4
                    column_type.length = 8

        col += ' ' + dialect.type_compiler.process(column_type, type_expression=column)

        if dialect.name == 'snowflake':
            col += ' AS (value:'+dialect.identifier_preparer.format_column(column)+'::'+dialect.type_compiler.process(column_type, type_expression=column)+')'
        elif dialect.name == 'mssql':
            # SQL Server Polybase does not support definitions with 'max'
            col = col.replace(' NVARCHAR(max)', ' NVARCHAR(4000)')
            col = col.replace(' VARCHAR(max)', ' VARCHAR(8000)')
            col = col.replace(' NCHAR(max)', ' NCHAR(4000)')
            col = col.replace(' CHAR(max)', ' CHAR(8000)')
            # SQL Server Polybase does not support data type NUMERIC, but DECIMAL
            col = col.replace(' NUMERIC(', ' DECIMAL(')
            # We use odbc2parquet which saves DATETIMEOFFSET to TIMESTAMP in UTC. See:
            #   https://github.com/pacman82/odbc2parquet/issues/249
            # Therefore, we need to use DATETIME2 instead of DATETIMEOFFSET
            col = col.replace(' DATETIMEOFFSET', ' DATETIME2')
            # TYPE 'timestamp/rowversion' is not supported
            col = col.replace(' TIMESTAMP', ' BINARY(8)')
            # TYPE 'text' is not supported
            col = col.replace(' TEXT(2147483647)', ' VARCHAR(8000)')
            #col = col.replace(' NTEXT(2147483647)', ' NVARCHAR(4000)') (not tested)

        column_definitions.append(col)

    if dialect.name == 'snowflake':
        options['LOCATION'] = f'@public.{dialect.identifier_preparer.quote_identifier(storage_name)}/{path.replace(" ","%20")}'
        if 'FILE_FORMAT' not in options:
            options['FILE_FORMAT'] = f'(FORMAT_NAME=\'{format_name}\')'

        return ('CREATE '
            + ('OR REPLACE ' if or_replace else '')
            + 'EXTERNAL TABLE '
            + ('IF NOT EXISTS ' if if_not_exists else '')
            + dialect.identifier_preparer.format_table(table)
            + f' (\n\t'
            + ',\n\t'.join(column_definitions)
            + f'\n)\n'
            + ('PARTITION BY (' + ','.join(partition_by) + ')' if partition_by else '')
            + (f'WITH\n\t' + '\n\t'.join([f'{k} = {v}' for k, v in options.items()]) if options else '')
            + ';')
    elif dialect.name == 'mssql':
        if bind.url.host.lower().endswith('-ondemand.sql.azuresynapse.net') and format_name.upper() == 'DELTA':
            options['BULK'] = f"'{path}'"
            options['DATA_SOURCE'] = f"= '{storage_name}'"
            options['FORMAT'] = f"= '{format_name}'"
            # The database is a Synapse Serverless instance and it connects to a DELTA table.
            # For such cases, we use a SQL View with OPENROWSET as the following article points out:
            # https://www.serverlesssql.com/partition-pruning-delta-tables-in-azure-synapse-analytics/
            return (
                (f"IF EXISTS (SELECT * FROM sys.external_tables WHERE object_id = OBJECT_ID('{dialect.identifier_preparer.format_table(table)}'))\n\tDROP EXTERNAL TABLE {dialect.identifier_preparer.format_table(table)};\nGO\n" if or_replace else '')
                + (f"IF EXISTS (SELECT * FROM sys.views WHERE object_id = OBJECT_ID('{dialect.identifier_preparer.format_table(table)}'))\n\tDROP VIEW {dialect.identifier_preparer.format_table(table)};\nGO\n" if or_replace else '')
                + (f"IF NOT EXISTS (SELECT * FROM sys.views WHERE object_id = OBJECT_ID('{dialect.identifier_preparer.format_table(table)}'))\n" if if_not_exists else '')
                + 'CREATE VIEW '
                + dialect.identifier_preparer.format_table(table)
                + '\nAS\nSELECT\n\t'
                + ',\n\t'.join(columns)
                + '\nFROM OPENROWSET(\n\t'
                + ',\n\t'.join([f'{k} {v}' for k, v in options.items()])
                + '\n)\nWITH\n(\n\t'
                + ',\n\t'.join(column_definitions)
                + '\n) AS src;')
        else:
            options['LOCATION'] = f"'{path}'"
            options['DATA_SOURCE'] = dialect.identifier_preparer.quote_identifier(storage_name)
            options['FILE_FORMAT'] = dialect.identifier_preparer.quote_identifier(format_name)
            return (
                (f"IF EXISTS (SELECT * FROM sys.external_tables WHERE object_id = OBJECT_ID('{dialect.identifier_preparer.format_table(table)}'))\n\tDROP EXTERNAL TABLE {dialect.identifier_preparer.format_table(table)};\nGO\n" if or_replace else '')
                + (f"IF NOT EXISTS (SELECT * FROM sys.external_tables WHERE object_id = OBJECT_ID('{dialect.identifier_preparer.format_table(table)}'))\n" if if_not_exists else '')
                + 'CREATE EXTERNAL TABLE '
                + dialect.identifier_preparer.format_table(table)
                + '\n(\n\t'
                + ',\n\t'.join(column_definitions)
                + '\n)\n'
                + 'WITH (\n\t'
                + ',\n\t'.join([f'{k} = {v}' for k, v in options.items()])
                + '\n);')
    elif dialect.name == 'databricks':
        # we assume that the storage name matches the storage alias
        storage = mara_storage.storages.storage(storage_name)

        if format_name == 'CSV':
            if 'header' not in options:
                options['header'] = 'false'

        if isinstance(storage, mara_storage.storages.AzureStorage):
            if storage.storage_type == 'dfs':
                location_path = f'abfss://{storage.container_name}@{storage.account_name}.dfs.core.windows.net/{path}'
            elif storage.storage_type == 'blob':
                location_path = f'wasbs://{storage.container_name}@{storage.account_name}.blob.core.windows.net/{path}'
            else:
                raise ValueError('Not supported storage type for AzureStorage. storage_type must be blob or dfs')
        else:
            raise ValueError(f'Not supported storage for SQL dialect {dialect.name}: {storage.__class__.__name__}')

        if options is None:
            options = {}

        return ((f'DROP TABLE IF EXISTS {dialect.identifier_preparer.format_table(table)};\n' if or_replace else '')
            + 'CREATE TABLE '
            + ('IF NOT EXISTS ' if if_not_exists else '')
            + dialect.identifier_preparer.format_table(table).replace(' ', '_')
            + ((' (\n\t'
                + ',\n\t'.join(column_definitions)
                + '\n)\n') if column_definitions else '')
            + (f'\n  USING {format_name}' if format_name else '')
            + (('\n  OPTIONS (\n\t'
                + ',\n\t'.join([f'{k} = {v}' for k, v in options.items()])
                + '\n)') if options else '')
            + ('\n  PARTITIONED BY (' + ', '.join(partition_by) + ')' if partition_by else '')
            + (f"\n  LOCATION '{location_path}'" if location_path else '')
            + ';')
    else:
        raise NotImplementedError(f'Not supported for SQL dialect {dialect.name}')
