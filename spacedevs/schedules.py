from dagster import schedule, ScheduleDefinition

from .jobs import spacedevs_dev_asset_job, spacedevs_prod_data_asset_job, spacedevs_prod_news_asset_job

spacedevs_dev_schedule = ScheduleDefinition(
    job=spacedevs_dev_asset_job, 
    cron_schedule="0 1 * * *"
    )

spacedevs_prod_data_schedule = ScheduleDefinition(
    job=spacedevs_prod_data_asset_job, 
    cron_schedule="0 1 * * *"
    )

spacedevs_prod_news_schedule = ScheduleDefinition(
    job=spacedevs_prod_news_asset_job, 
    cron_schedule="0 * * * *"
    )