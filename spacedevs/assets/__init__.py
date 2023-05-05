from dagster import load_assets_from_package_module

from . import dev, prod_data, prod_news

SPACEDEVS_DEV = "spacedevs_dev"
SPACEDEVS_DATA_PROD = "spacedevs_data_prod"
SPACEDEVS_NEWS_PROD = "spacedevs_news_prod"

spacedevs_dev_assets = load_assets_from_package_module(package_module=dev, group_name=SPACEDEVS_DEV)
spacedevs_prod_data_assets = load_assets_from_package_module(package_module=prod_data, group_name=SPACEDEVS_DATA_PROD)
spacedevs_prod_news_assets = load_assets_from_package_module(package_module=prod_news, group_name=SPACEDEVS_NEWS_PROD)