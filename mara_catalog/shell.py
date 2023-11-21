import shlex
from mara_db import dbs


def sqlalchemy_schema_to_stdout_command(db_alias: str, table_name: str, schema: str):
    """
    Returns a bash command which returns the sqlalchemy table/metadata.
    """
    current_db = dbs.db(db_alias)
    return (f'sqlacodegen "{current_db.sqlalchemy_url}" --generator tables --tables {shlex.quote(table_name)}'
            + (f' --schema {shlex.quote(schema)}' if schema else ''))
