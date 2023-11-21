"""
Helper functions interacting with module pyarrow.
"""

from functools import singledispatch
import pyarrow
import sqlalchemy
from sqlalchemy import Table, Column, MetaData
from typing import Optional
from mara_storage import storages


# NOTE Maybe move this over to mara_storage.pyarrow
@singledispatch
def pyarrow_filesystem(storage):
    """
    Returns a pyarrow compatible filesystem for a storage alias.
    """
    raise NotImplementedError(f'Please implement pyarrow_filesystem for type "{storage.__class__.__name__}"')


@pyarrow_filesystem.register(str)
def __(storage: str):
    return pyarrow_filesystem(storages.storage(storage))


@pyarrow_filesystem.register(storages.AzureStorage)
def __(storage: storages.AzureStorage):
    import adlfs
    return adlfs.AzureBlobFileSystem(
        account_name=storage.account_name,
        account_key=storage.account_key)



def pyarrow_to_sqlalchemy_type(type: pyarrow.DataType):
    """
    Returns a SQLAlchemy type from a pyarrow data type.
    """
    import pyarrow.types as pt
    if pt.is_boolean(type):
        return sqlalchemy.Boolean()
    elif pt.is_int8(type):
        return sqlalchemy.SMALLINT() # this is a fallback/workaround. We should here use a smaller type e.g 'TINYINT' if possible
    elif pt.is_int16(type):
        return sqlalchemy.SMALLINT()
    elif pt.is_int32(type) or pt.is_null(type):
        return sqlalchemy.Integer()
    elif pt.is_int64(type):
        return sqlalchemy.BigInteger()
    elif pt.is_decimal(type):
        return sqlalchemy.DECIMAL(type.precision, type.scale)
    elif pt.is_float32(type):
        return sqlalchemy.Float()
    elif pt.is_float64(type):
        return sqlalchemy.Float(asdecimal=True)
    elif pt.is_string(type):
        return sqlalchemy.Unicode()
    elif pt.is_date32(type):
        return sqlalchemy.Date()
    elif pt.is_date64(type):
        return sqlalchemy.DateTime()
    elif pt.is_date(type):
        return sqlalchemy.Date()
    elif pt.is_timestamp(type):
        # TODO no indentification if timezone is included is given
        return sqlalchemy.DateTime()
    elif pt.is_binary(type):
        return sqlalchemy.BINARY()
    elif pt.is_struct(type):
        # NOTE we assume here that the upperlaying engine uses JSON to read the data.
        #return sqlalchemy.JSON()
        return sqlalchemy.String()
    elif pt.is_list(type):
        # NOTE we assume here that the upperlaying engine uses JSON to read the data.
        #return sqlalchemy.JSON()
        return sqlalchemy.String()
    else:
        raise NotImplementedError(f'Not implemented pyarrow.DataType: {type}')


def pyarrow_schema_to_sqlalchemy_table(pyarrow_schema, name: str, schema: Optional[str]=None, metadata: Optional[MetaData]=None) -> Table:
    """
    Creates a sqlalchemy table from a PyArrow schema.

    Args:
        pyarrow_schema: The pyarrow schema to convert
        name: The table name for the sqlalchemy table
        schema: Optional. The schema name for the sqlalchemy table.
        metadata: Optional. The metadata object for the table.
    """
    columns = []
    for field in pyarrow_schema:
        #print(f'field: {field.name}, {field.type} ==> {pyarrow_to_sqlalchemy_type(field.type)}')
        columns.append(
            Column(field.name, pyarrow_to_sqlalchemy_type(field.type)))

    if not metadata:
        metadata = MetaData()

    return Table(name, metadata, *columns, schema=schema)
