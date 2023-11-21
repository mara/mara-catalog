from mara_pipelines.commands.bash import RunBash
from mara_storage.shell import write_file_command
import mara_pipelines.config
from .shell import sqlalchemy_schema_to_stdout_command



def WriteSchema(table_name: str, file_name: str, db_alias: str = None, schema: str = None, storage_alias: str = None):
    """
    Returns a command which writes the table schema as sqlalchemy model.
    """
    command = (sqlalchemy_schema_to_stdout_command(db_alias or mara_pipelines.config.default_db_alias(), table_name, schema)
                + ' \\\n  | '
                + write_file_command(storage_alias, file_name))

    return RunBash(command=command)
