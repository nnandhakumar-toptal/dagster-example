import json

import pandas as pd
import psycopg2 as pg
import pandas.io.sql as psql
from sqlalchemy import create_engine

from dagster import (
    Field,
    InitResourceContext,
    InputContext,
    IOManager,
    OutputContext,
    StringSource,
    io_manager,
)

# Learn more about custom I/O managers in Dagster docs:
# https://docs.dagster.io/concepts/io-management/io-managers#a-custom-io-manager-that-stores-pandas-dataframes-in-tables


class PluralDwDataframeIOManager(IOManager):
    def __init__(self, host: str, dbname: str, user: str, password: str, port: int) -> None:

        self._connection = pg.connect(f"host={host} dbname={dbname} user={user} password={password} port={port}")

    def handle_output(self, context: OutputContext, obj: pd.DataFrame):
        # Skip handling if the output is None
        if obj is None:
            return

        table_name = context.asset_key.to_python_identifier()

        # converting data to sql
        obj.to_sql(table_name, self._connection, if_exists= 'replace')
  

        # Recording metadata from an I/O manager:
        # https://docs.dagster.io/concepts/io-management/io-managers#recording-metadata-from-an-io-manager
        context.add_output_metadata({"dataset_id": self._dataset_id, "table_name": table_name})

    def load_input(self, context: InputContext):
        # upstream_output.asset_key is the asset key given to the Out that we're loading for
        table_name = context.asset_key.to_python_identifier()
        df = psql.read_sql(f"SELECT * FROM `{table_name}`", self._connection)
        return df


@io_manager(
    config_schema={
        "host": StringSource,
        "user": StringSource,
        "password": StringSource,
        "dbname": Field(
            str, default_value="postgress", description="Datbase id. Defaults to 'postgress'"
        ),
        "port": Field(
            int, default_value=5432, description="default port. defaults to 5432"
        ),
    }
)
def plural_dw_io_manager(init_context: InitResourceContext) -> PluralDwDataframeIOManager:
    return PluralDwDataframeIOManager(
        host=init_context.resource_config["host"],
        dbname=init_context.resource_config["dbname"],
        user=init_context.resource_config["user"],
        password=init_context.resource_config["password"],
        port=init_context.resource_config["port"],
    )