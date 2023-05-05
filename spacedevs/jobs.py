from dagster import job, define_asset_job, AssetSelection

from spacedevs.assets import SPACEDEVS_DEV, SPACEDEVS_DATA_PROD, SPACEDEVS_NEWS_PROD

spacedevs_dev_asset_job = define_asset_job(
    name="launch_dev_asset_job", 
    selection=AssetSelection.groups(SPACEDEVS_DEV)
)

spacedevs_prod_data_asset_job = define_asset_job(
    name="launch_prod_data_asset_job", 
    selection=AssetSelection.groups(SPACEDEVS_DATA_PROD)
)

spacedevs_prod_news_asset_job = define_asset_job(
    name="launch_prod_news_asset_job", 
    selection=AssetSelection.groups(SPACEDEVS_NEWS_PROD)
)