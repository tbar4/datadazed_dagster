from dagster import load_assets_from_package_module

from . import dev, prod

SPACEDEVS_DEV = "spacedevs_dev"
SPACEDEVS_PROD = "spacedevs_prod"

spacedevs_dev_assets = load_assets_from_package_module(package_module=dev, group_name=SPACEDEVS_DEV)
spacedevs_prod_assets = load_assets_from_package_module(package_module=prod, group_name=SPACEDEVS_PROD)