"""
Helper functions interacting with module deltalake.
"""

from functools import singledispatch
from mara_storage import storages


@singledispatch
def deltalake_build_uri(storage, path: str):
    """
    Creates a URI path for deltalake
    """
    raise NotImplementedError(f'Please implement deltalake_build_uri for type "{storage.__class__.__name__}"')


@deltalake_build_uri.register(str)
def __(storage: str, path: str):
    return deltalake_build_uri(storages.storage(storage), path=path)


@deltalake_build_uri.register(storages.AzureStorage)
def __(storage: storages.AzureStorage, path: str):
    return f'abfs://{storage.container_name}/{path}'


@deltalake_build_uri.register(storages.GoogleCloudStorage)
def __(storage: storages.AzureStorage, path: str):
    return f'gs://{storage.bucket_name}/{path}'



@singledispatch
def deltalake_storage_options(storage):
    """
    Returns the storage options required for the deltalake module.

    See as well:
        https://docs.rs/object_store/latest/object_store/azure/enum.AzureConfigKey.html#variants

    Args:
        storage: The storage as alias or class.
    """
    raise NotImplementedError(f'Please implement deltalake_storage_options for type "{storage.__class__.__name__}"')


@deltalake_storage_options.register(str)
def __(storage: str):
    return deltalake_storage_options(storages.storage(storage))


@deltalake_storage_options.register(storages.AzureStorage)
def __(storage: storages.AzureStorage):
    return {
        # See https://docs.rs/object_store/latest/object_store/azure/enum.AzureConfigKey.html#variants
        'account_name': storage.account_name,
        'account_key': storage.account_key,
        'sas_key': storage.sas
    }


@deltalake_storage_options.register(storages.GoogleCloudStorage)
def __(storage: storages.GoogleCloudStorage):
    return {
        # See https://docs.rs/object_store/latest/object_store/gcp/enum.GoogleConfigKey.html#variants
        'bucket_name': storage.bucket_name,
        'service_account': storage.service_account_file
    }
