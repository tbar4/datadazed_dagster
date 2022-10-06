from dagster import schedule, ScheduleDefinition

from .jobs import spacedevs_dev_asset_job, spacedevs_prod_asset_job

spacedevs_dev_schedule = ScheduleDefinition(
    job=spacedevs_dev_asset_job, 
    cron_schedule="0 1 * * *"
    )

spacedevs_prod_schedule = ScheduleDefinition(
    job=spacedevs_prod_asset_job, 
    cron_schedule="0 0/3 * * *"
    )