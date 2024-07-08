import os

from dagster import Definitions, load_assets_from_package_module, EnvVar, mem_io_manager,define_asset_job, ScheduleDefinition
from dagster_duckdb_pandas import DuckDBPandasIOManager
from dagster_duckdb import DuckDBResource

from .assets import staging, scraping
from .assets.dbt import (
    DBT_PROJECT_DIR,
    dbt_project_assets,
    dbt_resource,
)
from .resources import AlgoliaAPI, SilverSeaWebSocketClient, StorageAccountIoManager


silversea_assets = load_assets_from_package_module(staging, group_name="staging")

scraping_assets = load_assets_from_package_module(scraping, group_name="scraping")

duckdb_name = "scraped.duckdb"

resources = {
    # this io_manager allows us to load dbt models as pandas dataframes
    "io_manager": DuckDBPandasIOManager(
        database=os.path.join(DBT_PROJECT_DIR, duckdb_name)
    ),
    "memory_manager": mem_io_manager,
    # this resource is used to execute dbt cli commands
    "dbt": dbt_resource,
    # This resource is to store things into Azure Blob
    "storage_account_io_manager": StorageAccountIoManager(
        account_name="homelabaccount",
        account_key=EnvVar("STORAGE_ACCOUNT_KEY"),
        container_name="landing-blob",
    ),
    # This resource is the AlgoliaAPI
    "algolia_api": AlgoliaAPI(
        algolia_api_key=EnvVar("ALGOLIA_API_KEY"),
        algolia_application_id=EnvVar("ALGOLIA_APPLICATION_ID"),
        algolia_url=EnvVar("ALGOLIA_URL"),
    ),
    # This resource is the SilverSea Websocket
    "websocket": SilverSeaWebSocketClient(),
    # This resouces is used to query DuckDB if needed
    "duckdb": DuckDBResource(database=os.path.join(DBT_PROJECT_DIR, duckdb_name))
}

# Load all assets from the scrape module
scrape_assets = load_assets_from_package_module(scraping)

# Define a job that targets all assets
daily_sail_code_job = define_asset_job(name="weekly_sail_code_job", selection=scraping_assets)

weekly_sail_code_schedule = ScheduleDefinition(
    job=daily_sail_code_job,
    cron_schedule="0 1 * * 1",
)

defs = Definitions(
    assets=[*silversea_assets, dbt_project_assets, *scraping_assets],
    resources=resources,
    jobs=[daily_sail_code_job],
    schedules=[weekly_sail_code_schedule]
)
