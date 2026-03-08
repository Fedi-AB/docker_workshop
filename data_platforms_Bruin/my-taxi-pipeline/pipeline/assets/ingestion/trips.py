"""@bruin

name: ingestion.trips

type: python
image: python:3.11
connection: my-taxi-pipeline

materialization:
  type: table
  strategy: delete+insert
  incremental_key: pickup_datetime

@bruin"""

import os
import json
import pandas as pd
import pyarrow.dataset as ds
from datetime import datetime, timezone


def read_parquet_stream(url):

    dataset = ds.dataset(url, format="parquet")

    table = dataset.to_table()

    return table.to_pandas()


def materialize():

    print("Starting ingestion job...")

    start_date = pd.to_datetime(os.environ["BRUIN_START_DATE"])

    year = start_date.year
    month = start_date.month

    print(f"Ingestion window: {year}-{month:02d}")

    vars_dict = json.loads(os.environ.get("BRUIN_VARS", "{}"))

    taxi_types = vars_dict.get("taxi_types", ["green"])

    print("Taxi types:", taxi_types)

    dataframes = []

    canonical_columns = [
        "vendor_id",
        "pickup_datetime",
        "dropoff_datetime",
        "pickup_location_id",
        "dropoff_location_id",
        "passenger_count",
        "trip_distance",
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "improvement_surcharge",
        "total_amount",
        "payment_type",
        "trip_type",
        "congestion_surcharge",
        "store_and_fwd_flag",
        "ratecode_id",
        "dispatching_base_num",
        "affiliated_base_number",
        "sr_flag",
        "taxi_type",
        "extracted_at",
    ]

    for taxi_type in taxi_types:

        url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{year}-{month:02d}.parquet"

        print(f"Reading dataset: {url}")

        try:

            df = read_parquet_stream(url)

        except Exception as e:

            print("FAILED loading dataset")
            print(e)

            continue

        if taxi_type == "green":

            df = df.rename(columns={
                "VendorID": "vendor_id",
                "lpep_pickup_datetime": "pickup_datetime",
                "lpep_dropoff_datetime": "dropoff_datetime",
                "PULocationID": "pickup_location_id",
                "DOLocationID": "dropoff_location_id",
                "RatecodeID": "ratecode_id"
            })

        elif taxi_type == "yellow":

            df = df.rename(columns={
                "VendorID": "vendor_id",
                "tpep_pickup_datetime": "pickup_datetime",
                "tpep_dropoff_datetime": "dropoff_datetime",
                "PULocationID": "pickup_location_id",
                "DOLocationID": "dropoff_location_id",
                "RatecodeID": "ratecode_id"
            })

        elif taxi_type == "fhv":

            df = df.rename(columns={
                "PUlocationID": "pickup_location_id",
                "DOlocationID": "dropoff_location_id",
                "dispatching_base_num": "dispatching_base_num",
                "Affiliated_base_number": "affiliated_base_number",
                "SR_Flag": "sr_flag"
            })

        for col in canonical_columns:
            if col not in df.columns:
                df[col] = None

        df["taxi_type"] = taxi_type
        df["extracted_at"] = datetime.now(timezone.utc)

        df = df[canonical_columns]

        df["pickup_datetime"] = pd.to_datetime(df["pickup_datetime"], errors="coerce")

        df = df[df["pickup_datetime"].notna()]

        dataframes.append(df)

        print(f"{taxi_type} rows loaded:", len(df))

    if not dataframes:
        raise ValueError("No dataset downloaded.")

    result = pd.concat(dataframes, ignore_index=True)

    print("TOTAL ROWS:", len(result))

    return result