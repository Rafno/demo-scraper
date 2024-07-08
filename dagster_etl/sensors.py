import os
from dagster import sensor, RunRequest, RunConfig

@sensor()
def my_directory_sensor_cursor(context):
    pass