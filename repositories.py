from dagster import repository

from datadazed_dagster.spacedevs.assets.launches import launches
from datadazed_dagster.jobs import launch_asset_job
#from schedules import 

@repository
def dagster_examples():
    """Collection of example jobs, assets, and schedules used by Dagster."""
    return [
        launches,
        launch_asset_job
    ]