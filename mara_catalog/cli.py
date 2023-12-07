"""Auto-migrate command line interface"""

import sys

import click


@click.group()
def mara_catalog():
    """Commands to interact with data lakes and lakehouses"""
    pass


@mara_catalog.command()
@click.option('--catalog',
              help="The catalog to connect. If not given, all catalogs will be connected.")
@click.option('--db-alias',
              help='The database the catalog(s) shall be connected to. If not given, the default db alias is used.')
@click.option('--disable-colors', default=False, is_flag=True,
              help='Output logs without coloring them.')
def connect(
    catalog: str = None,
    db_alias: str = None,

    # from mara_pipelines.ui.cli.run_pipeline
    disable_colors: bool= False
):
    """
    Connects a data lake or lakehouse catalog to a database

    Args:
        catalog: The catalog to connect to. If not set, all configured catalogs will be connected.
        db_alias: The db alias the catalog shall be connected to. If not set, the default db alias is taken.

        disable_colors: If true, don't use escape sequences to make the log colorful (default: colorful logging)
    """

    from mara_pipelines.pipelines import Pipeline, Task
    from mara_pipelines.commands.python import RunFunction
    import mara_pipelines.cli
    import mara_pipelines.config
    from . import config
    from .connect import connect_catalog_mara_commands

    # create pipeline
    pipeline = Pipeline(
        id='_mara_catalog_connect',
        description="Connects a catalog with a database")

    def create_schema_if_not_exist(db_alias: str, schema_name: str) -> bool:
        import sqlalchemy
        import sqlalchemy.sql
        import sqlalchemy.schema
        import mara_db.sqlalchemy_engine

        eng = mara_db.sqlalchemy_engine.engine(db_alias)

        with eng.connect() as conn:
            if eng.dialect.has_schema(connection=conn, schema_name=schema_name):
                print(f'Schema {schema_name} already exists')
            else:
                create_schema = sqlalchemy.schema.CreateSchema(schema_name)
                print(create_schema)
                conn.execute(create_schema)
                conn.commit()

        return True

    _catalogs = config.catalogs()  # make sure to call the function once
    for catalog_name in [catalog] or _catalogs:
        catalog_pipeline = Pipeline(
            id=catalog_name,
            description=f"Connect catalog {catalog_name}")

        if catalog_name not in _catalogs:
            raise ValueError(f"Could not find catalog '{catalog_name}' in the registered catalogs. Please check your configured values for 'mara_catalog.config.catalogs'.")
        catalog = _catalogs[catalog_name]

        if catalog.tables:
            schemas = list(set([table.get('schema', catalog.default_schema) for table in catalog.tables]))

            for schema_name in schemas:
                # create schema if it does not exist
                print(f'found schema: {schema_name}')
                catalog_pipeline.add_initial(
                    Task(id='create_schema',
                        description=f'Creates the schema {schema_name} if it does not exist',
                        commands=[
                            RunFunction(
                                function=create_schema_if_not_exist,
                                args=[
                                    mara_pipelines.config.default_db_alias(),
                                    schema_name
                                ])]))

            catalog_pipeline.add(
                Task(id='create_tables',
                    description=f'Create tables for schema {catalog.default_schema}',
                    commands=connect_catalog_mara_commands(catalog=catalog,
                                                            db_alias=db_alias or mara_pipelines.config.default_db_alias(),
                                                            or_replace=True)))

            pipeline.add(catalog_pipeline)

    # run connect pipeline
    if mara_pipelines.cli.run_pipeline(pipeline, disable_colors=disable_colors):
        sys.exit(0)
    else:
        sys.exit(1)
