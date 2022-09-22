from dagster import job, define_asset_job

from datadazed_dagster.spacedevs.assets.spacedevs import (
    launches
)

launch_asset_job = define_asset_job(name="launch_asset_job", selection="launches")