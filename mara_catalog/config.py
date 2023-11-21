import functools
from .catalog import Catalog
from typing import Dict


@functools.lru_cache(maxsize=None)
def catalogs() -> Dict[str, Catalog]:
    """Returns all available catalogs"""
    return {}
