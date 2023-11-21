from functools import singledispatch
from sqlalchemy import Table, MetaData
from typing import Optional

import mara_pipelines.config
from mara_storage import storages

from mara_db.formats import ParquetFormat
from .formats import Format, DeltaFormat


def detect_table_format(path: str, storage_alias: Optional[str] = None) -> Format:
    """
    Tries to detect the table format from a table path.

    Args:
        path: Path to the table root folder
        storage_alias: The storage to be used.
    """
    raise NotImplementedError("TBD")  # TODO


@singledispatch
def detect_table_schema(format: Format, path: str, table_name: str, storage_alias: Optional[str] = None, schema_name: Optional[str] = None, metadata: Optional[MetaData] = None) -> Table:
    """
    Returns the schema of a hadoop table as sqlalchemy table definition.

    Args:
        path: Path to the table root folder
        format: Format of the table. E.g. ParquetFormat()
        storage_alias: The storage to be used.
        table_name: The table name for the sqlalchemy table
        schema_name: Optional. The schema name for the sqlalchemy table
        metadata: the sqlalchemy metadata object for the table
    """
    raise NotImplementedError(f'Please implement detect_table_schema for type "{format.__class__.__name__}"')


@detect_table_schema.register(ParquetFormat)
def __(format: ParquetFormat, path: str, table_name: str, storage_alias: Optional[str] = None, schema_name: Optional[str] = None, metadata: Optional[MetaData] = None) -> Table:
    import pyarrow.dataset as ds
    from ._pyarrow import pyarrow_filesystem, pyarrow_schema_to_sqlalchemy_table

    storage = storages.storage(storage_alias or mara_pipelines.config.default_storage_alias())

    if isinstance(storage, storages.AzureStorage):
        path = f"{storage.container_name}/{path}"

    dataset = ds.dataset(path, filesystem=pyarrow_filesystem(storage))
    return pyarrow_schema_to_sqlalchemy_table(dataset.schema, name=table_name, schema=schema_name, metadata=metadata)


@detect_table_schema.register(DeltaFormat)
def __(format: DeltaFormat, path: str, table_name: str, storage_alias: Optional[str] = None, schema_name: Optional[str] = None, metadata: Optional[MetaData] = None) -> Table:
    from deltalake import DeltaTable
    from ._pyarrow import pyarrow_schema_to_sqlalchemy_table
    from ._deltalake import deltalake_build_uri, deltalake_storage_options

    storage = storages.storage(storage_alias or mara_pipelines.config.default_storage_alias())
    file_uri = deltalake_build_uri(storage, path=path)

    deltaTable = DeltaTable(file_uri, storage_options=deltalake_storage_options(storage))
    pyarrow_schema = deltaTable.schema().to_pyarrow()
    return pyarrow_schema_to_sqlalchemy_table(pyarrow_schema, name=table_name, schema=schema_name, metadata=metadata)
