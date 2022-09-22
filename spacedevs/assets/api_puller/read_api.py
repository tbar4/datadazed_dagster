import requests
import json
import pandas
import pandas as pd
import configparser
import re
import os
from sqlalchemy import create_engine
from datetime import datetime
import psycopg2.extras
import pathlib

psycopg2.extras.register_default_json(loads=lambda x: x)
psycopg2.extensions.register_adapter(dict, psycopg2.extras.Json)

def parse_json_col(df, column, new_col_name, index_num = 0, index_label = id, drop_col=True):
    df[new_col_name] = None
    for index, row in df.iterrows():
        try:
            column_id = df[f'{column}'].tolist()[index][index_num][index_label]
            df.loc[index, new_col_name] = column_id
        except:
            df.loc[index, new_col_name] = None
    if drop_col:
        return df.drop(columns=[column])
    else:
        return df

class BaseAPI:

    def __init__(self, env = 'prod', params='', schema="spacedevs", library=None, filter=None):
        # Read in Config Details
        parent_path = pathlib.Path(__file__).parent.resolve()
        self.config = configparser.ConfigParser()
        self.config.read(f'{parent_path}/config.ini')

        # Init variables
        self.env = env
        self.library = library
        self.schema = schema
        self.table = re.sub("/", "_", self.library)
        self.filter = filter
        self.base_url = self.config['URLs'][env]
        self.limit = self.config[self.library]['limit']
        self.offset = self.config[self.library]['offset']
        self.url = f"{self.base_url}{self.library}"
        self.params = params
        try:
            self.pg_user = os.environ.get("pg_user")
            self.pg_password = os.environ.get("pg_password")
            self.pg_host = os.environ.get("pg_host")
            self.pg_port = os.environ.get("pg_port")
            self.pg_db = os.environ.get("pg_db")
            self.engine = create_engine(f"postgresql://{self.pg_user}:{self.pg_password}@{self.pg_host}:{self.pg_port}/{self.pg_db}")
        except:
            print("No Environment Variables for Postgres found!")
        try:
            self.api_token = os.environ.get("space_devs_token")
        except:
            print("No Environment Variables for SpaceDevs found!")

    def get_api_data(self):
        count = 100
        offset = 0
        limit = 100
        final_df = pd.DataFrame()
        headers = {'Authorization':  f'Token {self.api_token}'}
        print(headers)
        while count > 99:
            try:
                try:
                    ordering = self.config[self.library]['ordering']
                    if self.env == "prod":
                        max_ordering_df = pd.read_sql(f"SELECT MAX({ordering}) FROM {self.schema}.{self.table}", self.engine)
                    else:
                        max_ordering_df = pd.read_sql(f"SELECT MAX({ordering}) FROM {self.schema}_dev.{self.table}", self.engine)
                    params = {f"{self.filter}": f"{max_ordering}",
                                   'ordering': ordering,
                                   'limit': limit,
                                   'offset': offset}

                except:
                    params = {'limit': limit,
                              'offset': offset}
                try:
                    response = requests.get(self.url, headers=headers, params=params)
                    print("Running with Token")
                except:
                    response = requests.get(self.url, params=params)
                print(response.url)
                results = response.json()['results']
                data_df = pd.json_normalize(results)
                count = data_df["id"].count()
                offset = offset + count
                try:
                    max = data_df[ordering].max()
                except:
                    max = None

                final_df = pd.concat([data_df, final_df], axis = 0)
                print(f"{self.table} ran as expected:\tcount:", count, "\tmax_id:", max, "\toffset", offset)

            except:
                params = {}
                try:
                    response = requests.get(self.url, headers=self.headers, params=params)
                except:
                    response = requests.get(self.url, params=params)
                print(response.url)
                results = response.json()['results']
                data_df = pd.json_normalize(results)
                count = data_df['id'].count()
                offset = offset + count
                max = data_df['id'].max()

                final_df = pd.concat([data_df, final_df], axis = 0)
                print("There was an error in the base params, but we were still able to pull your data:\tcount:", count, "\tmax_id:", max, "\toffset", offset)

        final_df.columns = [re.sub("\.", "_", col) for col in final_df.columns]
        return final_df

    def write_to_sink(self, df):
        if self.filter is not None:
            if_exists = "replace"
        else:
            if_exists = "replace"

        if self.env == "prod":
            df.to_sql(f'{self.table}', con = self.engine, index = False, if_exists = if_exists, schema = self.schema)
            print(f"Table {self.table} written to {self.schema}\n")
        elif self.env == "dev":
            df.to_sql(f'{self.table}', con = self.engine, index = False, if_exists = if_exists, schema = f'{self.schema}_dev')
            print(f"Table {self.table} written to {self.schema}_dev\n")
        else:
            pass
