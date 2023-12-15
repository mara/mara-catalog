# This file contains secrets used by the tests

from mara_db import dbs

# supported placeholders
#   host='DOCKER_IP' will be replaced with the ip address given from pytest-docker
#   port=-1 will be replaced with the ip address given from pytest-docker

POSTGRES_DB = dbs.PostgreSQLDB(host='DOCKER_IP', port=-1, user="mara", password="mara", database="mara")
MSSQL_DB = dbs.SQLServerDB(host='DOCKER_IP', port=-1, user='sa', password='YourStrong@Passw0rd', database='master')
DATABRICKS_DB = None #dbs.DatabricksDB(host='DBSQLCLI_HOST_NAME', http_path='DBSQLCLI_HTTP_PATH', access_token='DBSQLCLI_ACCESS_TOKEN')
