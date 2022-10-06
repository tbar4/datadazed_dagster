import requests
import json
from pandas.io.json import json_normalize
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
        print("Beginning Write to Postgres")
        print(f'Engine: {self.engine}')
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

class SpacedevsAPI:

    def __init__(self, max_id, type, _sort = "id", _limit = 1000, base_url = "https://api.spaceflightnewsapi.net/v3/"):
        self.max_id = max_id
        self._sort = _sort
        self._limit = _limit
        self.type = type
        self.base_url = base_url
        self.url = f"{base_url}{type}"
        self.params = {'_sort': _sort,
                       'id_gt': max_id,
                       '_limit': _limit
                      }

    def get_data(self):
        response = requests.get(self.url, params=self.params)
        print(response.url)
        results = response.json()
        data_df = json_normalize(results)
        if 'launches' not in data_df:
            data_df['launches'] = None
        if 'events' not in data_df:
            data_df['events'] = None
        data_df['launches'] = data_df['launches'].apply(json.dumps)
        data_df['events'] = data_df['events'].apply(json.dumps)
        data_df["launch_id"] = None
        for index, row in data_df.iterrows():
            try:
                column_id = json.loads(data_df['launches'].tolist()[index])[0]['id']
                data_df.loc[index, "launch_id"] = column_id
            except:
                data_df.loc[index, "launch_id"] = None
        data_df = data_df.drop(columns=['launches'])
        data_df["event_id"] = None
        for index, row in data_df.iterrows():
            try:
                column_id = json.loads(data_df['events'].tolist()[index])[0]['id']
                data_df.loc[index, "event_id"] = column_id
            except:
                data_df.loc[index, "event_id"] = None
        data_df = data_df.drop(columns=["events"])
        data_df = data_df.rename(columns = {"imageUrl": "image_url", "newsSite": "news_site", "publishedAt": "published_at", "updatedAt": "updated_at"})

        return data_df

class ConnectToPostgres:

    def __init__(self, table, pg_user, pg_password, pg_host, pg_port, pg_db, engine = None):
        self.pg_user = pg_user
        self.pg_password = pg_password
        self.pg_host = pg_host
        self.pg_port = pg_port
        self.pg_db =  pg_db
        self.table = table
        self.engine = create_engine(f"postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}")

    def get_max_data_id(self):
        max_id_df = pd.read_sql(f"SELECT MAX(id) FROM spacedevs.{self.table}", self.engine)
        max_id = max_id_df.iloc[0]['max']

        return max_id

    def get_max_data_id_dev(self):
        max_id_df = pd.read_sql(f"SELECT MAX(id) FROM spacedevs_dev.{self.table}", self.engine)
        max_id = max_id_df.iloc[0]['max']

        return max_id


union_statement = f'''
SELECT
  concat('articles', '-', a.id) as id
, a.title
, a.url
, CONCAT(DATE(published_at), '-', regexp_replace(lower(a.title), '[^A-Za-z0-9]+', '-', 'g')) as slug
, a.published_at
, a.image_url
, a.news_site
, a.summary
, DATE(a.published_at) as published_date
, 'articles' as api_source
, CASE WHEN l.slug IS NULL OR l.slug = 'null' THEN 'No Related Launch' ELSE l.slug END as launch_slug
, CASE WHEN l.name IS NULL OR l.name = 'null' THEN 'No Related Launch' ELSE l.name END as launch_name
, CASE WHEN e.slug IS NULL OR e.slug = 'null' THEN 'No Related Event' ELSE e.slug END as event_slug
, CASE WHEN e.name IS NULL OR e.name = 'null' THEN 'No Related Event' ELSE e.name END as event_name
FROM spacedevs.articles a
LEFT JOIN spacedevs.launch l
    ON a.launch_id = l.id
LEFT JOIN spacedevs.event e
ON CAST(a.event_id as VARCHAR) = CAST(e.id as VARCHAR)
WHERE concat('articles', '-', a.id) NOT IN (SELECT id FROM spacedevs.posts)
UNION ALL
SELECT
  concat('blogs', '-', a.id) as id
, a.title
, a.url
, CONCAT(DATE(published_at), '-', regexp_replace(lower(a.title), '[^A-Za-z0-9]+', '-', 'g')) as slug
, a.published_at
, a.image_url
, a.news_site
, a.summary
, DATE(published_at) as published_date
, 'blogs' as api_source
, CASE WHEN l.slug IS NULL OR l.slug = 'null' THEN 'No Related Launch' ELSE l.slug END as launch_slug
, CASE WHEN l.name IS NULL OR l.name = 'null' THEN 'No Related Launch' ELSE l.name END as launch_name
, CASE WHEN e.slug IS NULL OR e.slug = 'null' THEN 'No Related Event' ELSE e.slug END as event_slug
, CASE WHEN e.name IS NULL OR e.name = 'null' THEN 'No Related Event' ELSE e.name END as event_name
FROM spacedevs.blogs a
LEFT JOIN spacedevs.launch l
    ON a.launch_id = l.id
LEFT JOIN spacedevs.event e
ON CAST(a.event_id as VARCHAR) = CAST(e.id as VARCHAR)
WHERE concat('blogs', '-', a.id) NOT IN (SELECT id FROM spacedevs.posts)
UNION ALL
SELECT
  concat('reports', '-', a.id) as id
, a.title
, a.url
, CONCAT(DATE(published_at), '-', regexp_replace(lower(a.title), '[^A-Za-z0-9]+', '-', 'g')) as slug
, a.published_at
, a.image_url
, a.news_site
, a.summary
, DATE(published_at) as published_date
, 'reports' as api_source
, CASE WHEN l.slug IS NULL OR l.slug = 'null' THEN 'No Related Launch' ELSE l.slug END as launch_slug
, CASE WHEN l.name IS NULL OR l.name = 'null' THEN 'No Related Launch' ELSE l.name END as launch_name
, CASE WHEN e.slug IS NULL OR e.slug = 'null' THEN 'No Related Event' ELSE e.slug END as event_slug
, CASE WHEN e.name IS NULL OR e.name = 'null' THEN 'No Related Event' ELSE e.name END as event_name
FROM spacedevs.reports a
LEFT JOIN spacedevs.launch l
    ON a.launch_id = l.id
LEFT JOIN spacedevs.event e
ON CAST(a.event_id as VARCHAR) = CAST(e.id as VARCHAR)
WHERE concat('reports', '-', a.id) NOT IN (SELECT id FROM spacedevs.posts)
ORDER BY published_at DESC
'''

dev_union_statement = f'''
SELECT
  concat('articles', '-', a.id) as id
, a.title
, a.url
, CONCAT(DATE(published_at), '-', regexp_replace(lower(a.title), '[^A-Za-z0-9]+', '-', 'g')) as slug
, a.published_at
, a.image_url
, a.news_site
, a.summary
, DATE(a.published_at) as published_date
, 'articles' as api_source
, CASE WHEN l.slug IS NULL OR l.slug = 'null' THEN 'No Related Launch' ELSE l.slug END as launch_slug
, CASE WHEN l.name IS NULL OR l.name = 'null' THEN 'No Related Launch' ELSE l.name END as launch_name
, CASE WHEN e.slug IS NULL OR e.slug = 'null' THEN 'No Related Event' ELSE e.slug END as event_slug
, CASE WHEN e.name IS NULL OR e.name = 'null' THEN 'No Related Event' ELSE e.name END as event_name
FROM spacedevs_dev.articles a
LEFT JOIN spacedevs_dev.launch l
    ON a.launch_id = l.id
LEFT JOIN spacedevs_dev.event e
ON CAST(a.event_id as VARCHAR) = CAST(e.id as VARCHAR)
WHERE concat('articles', '-', a.id) NOT IN (SELECT id FROM spacedevs.posts)
UNION ALL
SELECT
  concat('blogs', '-', a.id) as id
, a.title
, a.url
, CONCAT(DATE(published_at), '-', regexp_replace(lower(a.title), '[^A-Za-z0-9]+', '-', 'g')) as slug
, a.published_at
, a.image_url
, a.news_site
, a.summary
, DATE(published_at) as published_date
, 'blogs' as api_source
, CASE WHEN l.slug IS NULL OR l.slug = 'null' THEN 'No Related Launch' ELSE l.slug END as launch_slug
, CASE WHEN l.name IS NULL OR l.name = 'null' THEN 'No Related Launch' ELSE l.name END as launch_name
, CASE WHEN e.slug IS NULL OR e.slug = 'null' THEN 'No Related Event' ELSE e.slug END as event_slug
, CASE WHEN e.name IS NULL OR e.name = 'null' THEN 'No Related Event' ELSE e.name END as event_name
FROM spacedevs_dev.blogs a
LEFT JOIN spacedevs_dev.launch l
    ON a.launch_id = l.id
LEFT JOIN spacedevs_dev.event e
ON CAST(a.event_id as VARCHAR) = CAST(e.id as VARCHAR)
WHERE concat('blogs', '-', a.id) NOT IN (SELECT id FROM spacedevs.posts)
UNION ALL
SELECT
  concat('reports', '-', a.id) as id
, a.title
, a.url
, CONCAT(DATE(published_at), '-', regexp_replace(lower(a.title), '[^A-Za-z0-9]+', '-', 'g')) as slug
, a.published_at
, a.image_url
, a.news_site
, a.summary
, DATE(published_at) as published_date
, 'reports' as api_source
, CASE WHEN l.slug IS NULL OR l.slug = 'null' THEN 'No Related Launch' ELSE l.slug END as launch_slug
, CASE WHEN l.name IS NULL OR l.name = 'null' THEN 'No Related Launch' ELSE l.name END as launch_name
, CASE WHEN e.slug IS NULL OR e.slug = 'null' THEN 'No Related Event' ELSE e.slug END as event_slug
, CASE WHEN e.name IS NULL OR e.name = 'null' THEN 'No Related Event' ELSE e.name END as event_name
FROM spacedevs_dev.reports a
LEFT JOIN spacedevs_dev.launch l
    ON a.launch_id = l.id
LEFT JOIN spacedevs_dev.event e
ON CAST(a.event_id as VARCHAR) = CAST(e.id as VARCHAR)
WHERE concat('reports', '-', a.id) NOT IN (SELECT id FROM spacedevs.posts)
ORDER BY published_at DESC
'''
