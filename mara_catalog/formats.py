"""A list of lakehouse formats."""

from mara_db.formats import Format


class DeltaFormat(Format):
    """Delta.io lakehouse format"""
    pass


class HudiFormat(Format):
    """Apache Hudi lakehouse format"""
    pass


class IcebergFormat(Format):
    """Apache Iceberg lakehouse format"""
    pass
