__version__ = '0.1.0'


def MARA_CONFIG_MODULES():
    from . import config
    return [config]


def MARA_CLICK_COMMANDS():
    from . import cli
    return [cli.mara_catalog]
