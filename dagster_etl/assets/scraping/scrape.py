import json
import time
from dagster import (AssetExecutionContext, StaticPartitionMapping, asset, OpExecutionContext,AssetMaterialization, StaticPartitionsDefinition, AssetDep)
from dagster_duckdb import DuckDBResource
import duckdb
import pandas as pd
from ...resources import SilverSeaWebSocketClient, StorageAccountIoManager
from typing import List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

def get_all_full_cabin_categories(duckdb: DuckDBResource):
    query = """
        SELECT ship_code, cabin_category
        FROM scraped.public.cabin_categories
    """

    with duckdb.get_connection() as conn:
        df = pd.read_sql(query, conn)

    return df

def process_received_message(context, action, received_message, fare_code, sail_code, cabin_category=None):
    message = json.loads(received_message)
    if '"PricesErrorResponseV2"' in received_message:
        context.log.warn(f"No such fare_code={fare_code} for sail_code={sail_code} for action={action}, skipping...")
    elif '"BadRequestResponse"' in received_message:
        context.log.error(f"Bad request for {received_message}")
        raise Exception("Bad request received")
    elif action == 'available-suites' and 'suites' in message and not message['suites']:
        context.log.warn(f"No availability for fare_code={fare_code} for sail_code={sail_code} for cabin_category={cabin_category}, skipping...")
    else:
        return received_message
    return ""

def execute_requests(context, socket, action, sail_code, fare_code, currency, cabin_categories, combinations):
    futures = []
    data = ""
    with ThreadPoolExecutor(max_workers=10) as executor:
        if action == 'available-suites':
            for cabin_category in cabin_categories:
                futures.append(executor.submit(socket.send_and_receive, context, action, fare_code, sail_code, 1, 0, currency, cabin_category))
        else:
            for adults, kids in combinations.keys():
                if adults + kids <= 4:
                    futures.append(executor.submit(socket.send_and_receive, context, action, fare_code, sail_code, adults, kids, currency, None))
        
        for future in as_completed(futures):
            received_message = future.result()
            data_piece = process_received_message(context, action, received_message, fare_code, sail_code)
            if data_piece:
                data += data_piece + "\n"
    
    return data

def get_and_store(
    context: OpExecutionContext,
    socket: SilverSeaWebSocketClient,
    action: str,
    sail_code: str,
    storage_account_io_manager: StorageAccountIoManager,
    fare_code: str = "Essential",
    currency: str = "US",
    cabin_categories: Optional[List[str]] = None,
) -> None:
    if action not in ["prices-v2", "available-suites"]:
        context.log.error("Incorrect action set")
        return

    if action == 'available-suites' and cabin_categories is None:
        cabin_categories = [None]

    combinations = {
        (1, 0): "1 adult, 0 kids",
        (1, 1): "1 adult, 1 kid",
        (1, 2): "1 adult, 2 kids",
        (1, 3): "1 adult, 3 kids",
        (2, 0): "2 adults, 0 kids",
        (2, 1): "2 adults, 1 kid",
        (2, 2): "2 adults, 2 kids",
        (3, 0): "3 adults, 0 kids",
        (3, 1): "3 adults, 1 kid",
        (4, 0): "4 adults, 0 kids",
    }

    data = execute_requests(context, socket, action, sail_code, fare_code, currency, cabin_categories, combinations)

    if data:
        blob_name = f"socket/sail_code={sail_code}/action={action}/currency={currency}/fare_code={fare_code}/data"
        context.log.info(f"Blob name will be {blob_name}")
        storage_account_io_manager.upload_blob(context, competitor="silversea", json_data=data, blob_name=blob_name)
    else:
        context.log.warn(f"No data was recorded for fare_code={fare_code} for sail_code={sail_code} for action={action}")

@asset(
    required_resource_keys={"storage_account_io_manager", "algolia_api"},
    description="Fetches sail codes prices for all cabins"
)
def store_sail_codes_file(context: AssetExecutionContext) -> pd.DataFrame:
    storage_account_io_manager = context.resources.storage_account_io_manager
    algolia_api = context.resources.algolia_api
    page = 0
    sail_codes_data = []

    while True:  # Continue until all pages are processed
        time.sleep(1)
        payload = json.dumps(
            {
                "requests": [
                    {
                        "indexName": "prod_cruises_all_languages",
                        "params": f"analytics=false&distinct=true&filters=(countries%3A%22US%22)&hitsPerPage=10&maxValuesPerFacet=100&page={page}&query=&tagFilters=",
                    }
                ]
            }
        )

        response = algolia_api.request("POST", "/1/indexes/*/queries", payload)
        if response.status != 200:
            context.log.error(f"Error: {response.status} - {response.reason}")
            break

        result = json.loads(response.read().decode("utf-8"))
        page_target = result["results"][0]["nbPages"]
        sail_code = ''
        for hit in result["results"][0]["hits"]:
            sail_code = hit["cruiseCode"]
            fare_codes = hit.get("fareCodes", [])
            sail_codes_data.append({
                "sail_code": sail_code,
                "fare_codes": fare_codes
            })

        result_str = json.dumps(result)
        storage_account_io_manager.upload_blob(
            context,
            competitor="silversea",
            json_data=result_str,
            blob_name=f"algolia/{page}",
        )
        # Assume for now they won't have more pages. This prevents the job from running forever.
        if page > 300 or page >= page_target - 1:
            context.log.warn(f"Stopped at page {page} and target was {page_target}")
            break  # Stop the loop when all pages are processed

        page += 1

    # Convert sail_codes_data to a DataFrame
    df = pd.DataFrame(sail_codes_data)
    context.log.info(df)
    return df



@asset(
    required_resource_keys={"storage_account_io_manager", "websocket"},
    description="Processes each combination of sail code and currency",
    #partitions_def=StaticPartitionsDefinition(['GB', 'AS', 'CA', 'IE', 'US']),
)
def process_combination(context: AssetExecutionContext, store_sail_codes_file) -> None:
    storage_account_io_manager = context.resources.storage_account_io_manager
    websocket = context.resources.websocket
    #currency = context.partition_key
    currency = 'US'

    total_sail_codes = len(store_sail_codes_file['sail_code'])
    context.log.info(f"Beginning to iterate through {total_sail_codes}")
    for index, row in store_sail_codes_file.iterrows():
            sail_code = row['sail_code']
            fare_codes = row['fare_codes']
            context.log.info(f"at {index} of {total_sail_codes}")
            for fare_code in fare_codes:
                context.log.info(f"Generating combination for sail code {sail_code} and fare code {fare_code} with {currency}.")
                get_and_store(
                    context=context,
                    socket=websocket,
                    action="prices-v2",
                    sail_code=sail_code,
                    fare_code=fare_code,
                    currency=currency,
                    storage_account_io_manager=storage_account_io_manager,
                )

@asset(
    required_resource_keys={"storage_account_io_manager", "websocket", "duckdb"},
    description="Checks for each cabin category the availability of the cabins"
)
def scrape_availability(context: AssetExecutionContext, store_sail_codes_file) -> None:
    storage_account_io_manager = context.resources.storage_account_io_manager
    websocket = context.resources.websocket
    duckdb = context.resources.duckdb
    cabin_categories = get_all_full_cabin_categories(duckdb) #This is always yesterdays data
    # Extract the first 2 characters of sail_code
    store_sail_codes_file['ship_code'] = store_sail_codes_file['sail_code'].apply(lambda x: x[:2])
    # Perform the merge
    merged_df = pd.merge(cabin_categories, store_sail_codes_file, on='ship_code', how='inner')
    total_sail_codes = len(store_sail_codes_file['sail_code'].unique())
    context.log.info(f"Total unique sail codes: {total_sail_codes}")

    # Iterate through each sail_code
    for index, sail_code in enumerate(store_sail_codes_file['sail_code'].unique(), start=1):
        context.log.info(f"iterating {index}-{sail_code} of {total_sail_codes}")
        sail_code_rows = merged_df[merged_df['sail_code'] == sail_code]
        cabin_categories = sail_code_rows['cabin_category'].unique().tolist()
        fare_codes_list = sail_code_rows['fare_codes'].drop_duplicates()
        for fare_code in fare_codes_list: #TODO, fix this.
            for code in fare_code:
                get_and_store(
                context=context,
                socket=websocket,
                action="available-suites",
                sail_code=sail_code,
                fare_code=code,
                cabin_categories=cabin_categories,
                storage_account_io_manager=storage_account_io_manager,
            )
