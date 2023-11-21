"""Auto-migrate command line interface"""

import click


@click.group()
def mara_catalog():
    """Commands to interact with data lakes and lakehouses"""
    pass


@mara_catalog.command()
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
    import mara_pipelines.ui.cli
    import mara_pipelines.config
    from . import config
    from .connect import connect_catalog_mara_commands

    # create pipeline
    pipeline = Pipeline(
        id='_mara_catalog_connect',
        description="Connects a catalog with a database")

    def create_schema_if_not_exist(db_alias: str, schema_name: str):
        import sqlalchemy
        import sqlalchemy.schema
        import mara_db.sqlalchemy_engine

        eng = mara_db.sqlalchemy_engine.engine(db_alias)

        if not eng.dialect.has_schema(eng):
            create_schema = sqlalchemy.schema.CreateSchema(schema_name)
            print(create_schema)
            eng.execute(create_schema)

    for catalog_name in [catalog] or config.catalogs():
        catalog_pipeline = Pipeline(
            id=catalog_name,
            description=f"Connect catalog {catalog_name}")

        catalog = config.catalogs()[catalog_name]

        if catalog.schema_name:
            # create schema if it does not exist
            catalog_pipeline.add_initial(
                Task(id='create_schema',
                     description=f'Creates tthe schema {catalog.schema_name} if it does not exist',
                     commands=[
                        RunFunction(
                            function=create_schema_if_not_exist,
                            args=[
                                mara_pipelines.config.default_db_alias(),
                                catalog.schema_name
                            ])]))

        for command in connect_catalog_mara_commands(catalog=catalog,
                                                     db_alias=db_alias or mara_pipelines.config.default_db_alias(),
                                                     or_replace=True):
            catalog_pipeline.add(command)

        pipeline.add(catalog_pipeline)

    # run connect pipeline
    mara_pipelines.ui.cli.run_pipeline(pipeline, disable_colors=disable_colors)
