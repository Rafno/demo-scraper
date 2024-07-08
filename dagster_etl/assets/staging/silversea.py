from dagster import StaticPartitionsDefinition, asset, DailyPartitionsDefinition, multi_asset, AssetOut
from ...resources import StorageAccountIoManager
from datetime import datetime
import duckdb
import pandas as pd
import json

currency_partition = StaticPartitionsDefinition(['US','AS','CA','GB','IE'])
daily_partition = DailyPartitionsDefinition(start_date="2024-06-14")

def process_sail_code_json_data(json_data, extract_date):
    data = []
    for item in json_data:
        results = item.get("results", [])
        for result in results:
            hits = result.get("hits", [])
            for hit in hits:
                destination_name = hit.get("destinationName", {}).get("en")
                visible = hit.get("visible")
                ship_id = hit.get("content", {}).get("shipId")
                ship_name = hit.get("content", {}).get("shipName")
                special_type = hit.get("specialType")
                cruise_code = hit.get("cruiseCode")
                cruise_id = hit.get("cruiseId")
                available = hit.get("available")
                days = hit.get("days")
                departure_year_month = hit.get("departureYearMonth")
                departure_port = hit.get("departurePort", {}).get("city", {}).get("en")
                arrival_port = hit.get("arrivalPort", {}).get("city", {}).get("en")
                cruise_type = hit.get("cruiseType")
                combo_type = hit.get("comboType")
                port_codes = hit.get("portCodes")
                port_names = hit.get("portNames")
                country_names = hit.get("countryNames", {}).get("en")
                departure_timestamp = hit.get("departureTimestamp")
                top_cruise = hit.get("topCruise")
                day_group = hit.get("dayGroup")
                features = hit.get("features")
                cruise_group = hit.get("cruiseGroup")
                fare_codes = hit.get("fareCodes")
                countries = hit.get("countries")
                currency = hit.get("currency")
                ordering_price = hit.get("orderingPrice")
                ordering_webfare = hit.get("orderingWebFare")
                prices = hit.get("prices")
                promo_codes = hit.get("promoCodes")

                data.append({
                        "destination_name": destination_name,
                        "is_visible": visible,
                        "ship_code": cruise_code[:2],
                        "ship_id": ship_id,
                        "ship_name": ship_name,
                        "special_type": special_type,
                        "sail_code": cruise_code,
                        "cruise_id": cruise_id,
                        "available": available,
                        "days": days,
                        "departure_year_month": departure_year_month,
                        "departure_port": departure_port,
                        "arrival_port": arrival_port,
                        "cruise_type": cruise_type,
                        "combo_type": combo_type,
                        "port_codes": port_codes,
                        "port_names": port_names,
                        "country_names": country_names,
                        "departure_timestamp": departure_timestamp,
                        "top_cruise": top_cruise,
                        "day_group": day_group,
                        "features": features,
                        "cruise_group": cruise_group,
                        "fare_codes": fare_codes,
                        "countries": countries,
                        "currency": currency,
                        "ordering_price": ordering_price,
                        "ordering_webfare": ordering_webfare,
                        "prices": prices,
                        "promo_codes": promo_codes,
                        "extract_date": str(extract_date),
                })

    return pd.DataFrame(data)

@asset(
    compute_kind="python",
    description="Creates sail code table from Algolia",
    tags={"source": "aloglia"},
    partitions_def=daily_partition,
    metadata={"partition_expr": "extract_date"}
    )
def sail_codes(context, storage_account_io_manager: StorageAccountIoManager):
    partition_date = datetime.strptime(context.partition_key, "%Y-%m-%d")
    partition_date_str = partition_date.strftime("%Y/%m/%d")
    json_data = storage_account_io_manager.download_blobs_from_path(context, f'silversea/{partition_date_str}/algolia')

    data = process_sail_code_json_data(json_data, partition_date_str)

    df = pd.DataFrame(data)
    return df

def process_left_json_data(json_data, extract_date):
    left_data = []

    for message in json_data['data']['prices']:
        if "_tag" in message:
            if message["_tag"] == "Left":
                left_request = message['left']['request']
                left_data.append({
                    "sail_code": str(left_request.get("cruiseCode", None)), 
                    "ship_code": str(left_request.get("cruiseCode", None))[:2],
                    "cabin_category": str(left_request.get("suiteCategory", None)),
                    "adults": int(left_request['occupancy'].get("adults", None)),
                    "kids": int(left_request['occupancy'].get("kids", None)),
                    "fare_code": str(left_request.get("fareCode", None)),
                    "air_type": str(left_request['air'].get("type", None)),
                    "extract_date": str(extract_date),
                })

    left_df = pd.DataFrame(left_data)

    return left_df

def process_right_json_data(json_data, extract_date):
    right_data = []

    for message in json_data['data']['prices']:
        if "_tag" in message:
            if message["_tag"] == "Right":
                right = message['right']
                right_data.append({
                    "sail_code": str(right.get("cruiseCode", None)),
                    "ship_code": str(right.get("cruiseCode", None))[:2],
                    "cabin_category": str(right.get("suiteCategory", None)),
                    "adults": int(right['occupancy'].get("adults", None)),
                    "kids": int(right['occupancy'].get("kids", None)),
                    "fare_code": str(right.get("fareCode", None)),
                    "air_type": str(right['air'].get("type", None)),
                    "suite_payment_type": str(right['suitePayment'].get("type", None)),
                    "suite_payment_percentage": int(right['suitePayment'].get("percentage", None)),
                    "suite_payment_amount_value": float(right['suitePayment']['amount'].get("value", None)),
                    "suite_payment_amount_currency": str(right['suitePayment']['amount'].get("currency", None)),
                    "balance_amount_value": float(right['suitePayment']['balanceAmount'].get("value", None)),
                    "balance_amount_currency": str(right['suitePayment']['balanceAmount'].get("currency", None)),
                    "total_amount_value": float(right['suitePayment']['totalAmount'].get("value", None)),
                    "total_amount_currency": str(right['suitePayment']['totalAmount'].get("currency", None)),
                    "final_duedate": str(right['suitePayment'].get("finalDueDate", None)),
                    "inclusions": [str(x.get("inclusionCode", None)) for x in right['price'].get("inclusions", [])],
                    "guest_seqn": [int(x.get("number", None)) for x in right['price'].get("paxPrices", [])],
                    "pax_prices_totalvalue": [float(x['total'].get("value", None)) for x in right['price'].get("paxPrices", [])],
                    "pax_prices_totalcurrency": [str(x['total'].get("currency", None)) for x in right['price'].get("paxPrices", [])],
                    "price_total_value": float(right['price']['total'].get("value", None)),
                    "price_total_currency": str(right['price']['total'].get("currency", None)),
                    "extract_date": str(extract_date),
                })

    right_df = pd.DataFrame(right_data)
    return right_df

@multi_asset(
    compute_kind="python",
    description="Prices of each availability",
    partitions_def=daily_partition,
    outs={
        "unavailable_cabins": AssetOut(metadata={"partition_expr": "extract_date"}),
        "available_cabins": AssetOut(metadata={"partition_expr": "extract_date"}),
    }
)
def silversea_cabin_prices(context, storage_account_io_manager: StorageAccountIoManager):
    partition_date = datetime.strptime(context.partition_key, "%Y-%m-%d")
    partition_date_str = partition_date.strftime("%Y/%m/%d")

    # Download combined JSON data from the specified path
    json_data = storage_account_io_manager.download_blobs_from_path(context=context, path=f'silversea/{partition_date_str}/socket', target_action='prices-v2')
    df_left_list = []  # List to store left data frames
    df_right_list = []  # List to store right data frames

    if json_data:
        # Normalize the JSON data to flatten it into a table
        for line in json_data:
            context.log.info(f"Trying to normalize {line}")
            df_left = process_left_json_data(line, partition_date_str)
            df_right = process_right_json_data(line, partition_date_str)
            
            # Append data frames to respective lists
            df_left_list.append(df_left)
            df_right_list.append(df_right)

    else:
        context.log.error("Failed to download and combine JSON data.")

    # Concatenate all data frames in the list into one big data frame
    df_left = pd.concat(df_left_list, ignore_index=True)
    df_right = pd.concat(df_right_list, ignore_index=True)

    return df_left, df_right