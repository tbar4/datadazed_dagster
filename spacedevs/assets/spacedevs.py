from dagster import asset
from pandas import DataFrame

from api_puller.read_api import *

@asset
def launches() -> DataFrame:
    launch = BaseAPI(library="launch")
    df = launch.get_api_data()
    df = parse_json_col(df=df, index_label="id", column="program", new_col_name="program_id")

    return df