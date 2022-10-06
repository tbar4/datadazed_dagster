from dagster import job, define_asset_job, AssetSelection

from spacedevs.assets import SPACEDEVS_DEV, SPACEDEVS_PROD

spacedevs_dev_asset_job = define_asset_job(
    name="launch_dev_asset_job", 
    selection=AssetSelection.groups(SPACEDEVS_DEV)
)

spacedevs_prod_asset_job = define_asset_job(
    name="launch_prod_asset_job", 
    selection=AssetSelection.groups(SPACEDEVS_PROD)
)