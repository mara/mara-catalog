from functools import singledispatch
import pathlib
from typing import Iterator, Dict, List, Optional

from mara_db import dbs, formats
from mara_storage import storages
from mara_storage.client import StorageClient
from .formats import DeltaFormat, HudiFormat


@singledispatch
def discover_tables_from_db(db, include_schemas: List[str] = None) -> Iterator[Dict]:
    """
    Discovers tables and views from a database. System schemas like 'INFORMATION_SCHEMA' are skipped.

    Args:
        db: the db alis or class from which the tables shall be read.
        include_schemas: A list of schemas which shall be included. If not given, all schemas are taken.
    """
    raise NotImplementedError(f'Please implement discover_tables_from_db for type "{db.__class__.__name__}"')


@discover_tables_from_db.register(str)
def __(db_alias: str, include_schemas: List[str] = None) -> Iterator[Dict]:
    return discover_tables_from_db(dbs.db(db_alias), include_schemas=include_schemas)


@discover_tables_from_db.register(dbs.SQLServerDB)
def __(db: dbs.SQLServerDB, include_schemas: List[str] = None) -> Iterator[Dict]:
    schemas: List = include_schemas

    if not schemas:
        # read schemas
        with dbs.cursor_context(db) as con:
            con.execute("""SELECT
        name
    FROM sys.schemas
    WHERE name NOT IN ('INFORMATION_SCHEMA','sys')
    ORDER BY name ASC""")
            schemas = [str(row[0]) for row in con.fetchall()]

    # read tables per schema
    for schema in schemas or []:
        tables: List = None

        with dbs.cursor_context(db) as con:
            con.execute(f"""SELECT objects.name
FROM sys.objects
INNER JOIN sys.schemas ON schemas.schema_id = objects.schema_id
WHERE objects.type IN (
	'U', -- USER_TABLE
	'V' -- VIEW
)
AND schemas.name = '{schema}'
ORDER BY name ASC""")
            tables = [str(row[0]) for row in con.fetchall()]

        for table in tables:
            yield {
                'name': table,
                'schema': schema,
                'location': None,
                'format': None
            }



@singledispatch
def discover_tables_from_storage(storage, base_path: str, with_schema_folders: bool,
                                 table_format: Optional[formats.Format] = None
                                 ) -> Iterator[Dict]:
    """
    Discovers tables from a a storage path. Folders starting with '_' and '.' are ignored.

    Args:
        storage: the storage to be read from as alias or class
        base_path: The base path from which the tables shall be read.
        with_schema_folders: If the folders in the first level should be used as schema name
        table_format: the format to be used for a table. For some formats e.g. Delta or Hudi,
                      auto discovery will ignore this parameter.
    """
    raise NotImplementedError(f'Please implement discover_tables_from_storage for type "{storage.__class__.__name__}"')


@discover_tables_from_storage.register(str)
def __(storage_alias: str, base_path: str, with_schema_folders: bool,
       table_format: Optional[formats.Format] = None) -> Iterator[Dict]:
    return discover_tables_from_storage(storages.storage(storage_alias),
        base_path=base_path, with_schema_folders=with_schema_folders, table_format=table_format)


@discover_tables_from_storage.register(storages.AzureStorage)
def __(storage: storages.AzureStorage, base_path: str, with_schema_folders: bool,
       table_format: Optional[formats.Format] = None) -> Iterator[Dict]:
    azure_container_client = None

    if isinstance(storage, storages.AzureStorage):
        from mara_storage import azure
        client = azure.init_service_client(storage)
        container_client = client.get_container_client(storage.container_name)
        azure_container_client = container_client
    else:
        raise NotImplementedError()

    def iterate_storage(path) -> Iterator[str]:
        for item in azure_container_client.walk_blobs(name_starts_with=path + '/' if not path.endswith('/') else path):
            yield item.name

    def iterate_directories(path) -> Iterator[str]:
        for item in iterate_storage(path):
            if item.endswith('/'):
                yield item[:-1]

    def _search_schemas(path: pathlib.Path):
        """Take all directories and pass it over to _iterate_schema()"""
        _path = str(path) if str(path) != '.' else ''
        for item in iterate_directories(_path): #  client.iterate_files(_path):
            if item.startswith('_') or item.startswith('.'):
                # ignore folders starting with '_' and '.'
                continue

            yield item

    def _iterate_schema(path: pathlib.Path, schema_name: Optional[str]) -> Iterator:
        _path = str(path) if str(path) != '.' else ''
        for item in iterate_directories(_path): #  client.iterate_files(path):
            directory_name = pathlib.Path(item).name
            if directory_name.startswith('_') or directory_name.startswith('.'):
                # ignore folders starting with '_' and '.'
                continue

            yield directory_name

    def _discover_table(path: pathlib.Path, schema_name: str, table_name: str,
                        table_format: Optional[formats.Format] = None) -> Optional[dict]:
        format = None
        #partitions = []  # currently not implemented to detect partitions from the hadoop file structure
        _path = str(path) if str(path) != '.' else ''
        for file_name in iterate_storage(_path):
            if file_name.endswith('/_delta_log/'):
                format = DeltaFormat()
            elif file_name.endswith('.hoodie/'):
                format = HudiFormat()

            if format:
                break

            if file_name.endswith('/'):
                # skip folders
                continue

            if not format and table_format:
                format = table_format

            if file_name.startswith('_') or file_name.startswith('.'):
                # ignore files starting with '_' and '.'
                continue

            if not format:
                if file_name.endswith('.parquet'):
                    format = formats.ParquetFormat()
                elif file_name.endswith('.avro'):
                    format = formats.AvroFormat()
                elif file_name.endswith('.csv'):
                    format = formats.CsvFormat()
                elif file_name.endswith('.orc'):
                    format = formats.OrcFormat()
                elif file_name.endswith('.csv'):
                    format = formats.CsvFormat()
                elif file_name.endswith('.tsv'):
                    format = formats.CsvFormat(delimiter_char='\t')

            if format:
                break

        if not format:
            return None

        return {
            'name': table_name,
            'schema': schema_name,
            'location': str(path.relative_to(base_path)),
            'format': format
        }

    client = StorageClient(storage)
    path = base_path or pathlib.Path()

    schemas = _search_schemas(path) if with_schema_folders else [None]

    for schema_name in schemas:
        schema_path = (path / schema_name) if with_schema_folders else path

        for potential_table_name in _iterate_schema(path=schema_path,
                                                    schema_name=schema_name):
            table_root_path = schema_path / potential_table_name
            table = _discover_table(path=table_root_path, schema_name=schema_name,
                                    table_name=potential_table_name,
                                    table_format=table_format)

            if table:
                yield table
