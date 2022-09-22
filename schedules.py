from dagster import schedule, ScheduleDefinition

from datadazed_dagster.jobs import launch_job

launch_schedule = ScheduleDefinition(
    job=launch_job, 
    cron_schedule="0 0/3 * * *"
    )