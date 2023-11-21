import functools
import itertools
import pathlib
from typing import List, Optional, Union, Dict
from . import discover

from mara_db import formats


@functools.lru_cache(maxsize=None)
def catalog(alias) -> 'Catalog':
    """Returns a catalog by alias"""
    from . import config
    catalogs = config.catalogs()
    if alias not in catalogs:
        raise KeyError(f'catalog alias "{alias}" not configured')
    return catalogs[alias]


class Catalog:
    """
    A catalog is a set of tables.
    """
    _tables: List[dict] = None
    _auto_discover_executed: bool = False

    def __init__(self, has_schemas: bool = False, default_schema: str = None, auto_discover: bool = False) -> None:
        """
        Creates a catalog

        Args:
            has_schemas: If the catalog supports schemas.
            default_schema: a default schema to be used when self.has_schema is True
            auto_discover: If the table should run discover automatically when the property tables is accessed
        """
        self.has_schemas = has_schemas
        self.default_schema = default_schema
        self.auto_discover = auto_discover

    @property
    def tables(self) -> List[dict]:
        """A dictionary representing a table to be connected to."""
        if self._tables is None:
            if self.auto_discover and not self._auto_discover_executed:
                self._auto_discover_executed = True
                self.discover()
            else:
                # set an empty table because we do not use autodiscover
                self._tables = []

        return self._tables

    @tables.setter
    def tables(self, value):
        self._tables = value

    @property
    def schemas(self) -> Dict[str, List[dict]]:
        """Returns a dictionary with the schema as key and a table list as value."""
        if not self.tables:
            return {}
        if not self.has_schemas:
            return {self.default_schema: self.tables}

        def key(table) -> str:
            return table['schema']

        return {k: list(v) for k, v in itertools.groupby(sorted(self.tables, key=key), key=key)}

    def discover(self):
        """Discover catalog tables."""
        raise NotImplementedError(f'Please implement member discover for type "{self.__class__.__name__}"')


class StorageCatalog(Catalog):
    """
    A catalog connected to a storage.
    """
    def __init__(self, storage_alias: Optional[str] = None, base_path: Optional[Union[str, pathlib.Path]] = None,
                 has_schemas: bool = False, default_schema: str = 'public', auto_discover: bool = True,
                 table_format: Optional[formats.Format] = None) -> None:
        """
        Creates a storage catalog

        Args:
            storage_alias: the storage to be used
            base_path: the base path of the
            has_schemas: If the catalog supports schemas.
            default_schema: a default schema to be used when self.has_schema is True
        """
        super().__init__(has_schemas=has_schemas, default_schema=default_schema, auto_discover=auto_discover)
        self._storage_alias = storage_alias
        self._base_path = base_path
        self.table_format = table_format

    @property
    def storage_alias(self):
        if self._storage_alias:
            return self._storage_alias
        else:
            import mara_pipelines.config
            return mara_pipelines.config.default_storage_alias()

    @property
    def base_path(self) -> pathlib.Path:
        if self._base_path:
            return pathlib.Path(self._base_path)
        else:
            return pathlib.Path()

    def discover(self):
        """
        Discover catalog tables from a storage path.
        """
        tables = []
        for table in discover.discover_tables_from_storage(self.storage_alias, self.base_path, with_schema_folders=self.has_schemas, table_format=self.table_format):
            if table.get('schema', None) is None:
                table['schema'] = self.default_schema

            tables.append(table)

        self.tables = tables


class DbCatalog(Catalog):
    """
    A catalog connected to a database.
    """
    def __init__(self, db_alias: Optional[str] = None, has_schemas: bool = False, default_schema: str = 'public', auto_discover: bool = True) -> None:
        super().__init__(has_schemas=has_schemas, default_schema=default_schema, auto_discover=auto_discover)
        import mara_pipelines.config
        self.db_alias = db_alias or mara_pipelines.config.default_db_alias()

    def discover(self):
        tables = []
        for table in discover.discover_tables_from_db(self.db_alias):
            tables.append(table)

        self.tables = tables
