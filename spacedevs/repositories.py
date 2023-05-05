from dagster import repository

from .assets import spacedevs_dev_assets, spacedevs_prod_assets
from .jobs import spacedevs_dev_asset_job, spacedevs_prod_asset_job
from .schedules import spacedevs_dev_schedule, spacedevs_prod_schedule

@repository
def spacedevs():
    """Collection of example jobs, assets, and schedules used by Dagster."""
    return [
        spacedevs_dev_assets,
        spacedevs_prod_data_assets,
        spacedevs_prod_news_assets,
        spacedevs_dev_asset_job,
        spacedevs_prod_data_asset_job,
        spacedevs_prod_news_asset_job,
        spacedevs_dev_schedule,
        spacedevs_prod_data_schedule,
        spacedevs_prod_news_schedule
    ]