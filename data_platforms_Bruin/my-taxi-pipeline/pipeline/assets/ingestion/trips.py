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
from datetime import datetime, timezone


def materialize():

    start_date = pd.to_datetime(os.environ["BRUIN_START_DATE"])

    year = start_date.year
    month = start_date.month

    vars_dict = json.loads(os.environ.get("BRUIN_VARS", "{}"))
    taxi_types = vars_dict.get("taxi_types", ["green"])

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
    ]

    for taxi_type in taxi_types:

        url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{year}-{month:02d}.parquet"
        print(f"Fetching: {url}")

        try:
            df = pd.read_parquet(url)
        except Exception as e:
            print(f"Error loading {url}")
            print(e)
            continue

        # -----------------------
        # Rename columns
        # -----------------------

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
                "pickup_datetime": "pickup_datetime",
                "dropOff_datetime": "dropoff_datetime",
                "PUlocationID": "pickup_location_id",
                "DOlocationID": "dropoff_location_id",
                "dispatching_base_num": "dispatching_base_num",
                "Affiliated_base_number": "affiliated_base_number",
                "SR_Flag": "sr_flag"
            })

        # -----------------------
        # Add missing columns
        # -----------------------

        for col in canonical_columns:
            if col not in df.columns:
                df[col] = None

        # -----------------------
        # Select canonical schema
        # -----------------------

        df = df[canonical_columns]

        # -----------------------
        # Type normalization
        # -----------------------

        df["pickup_datetime"] = pd.to_datetime(df["pickup_datetime"], errors="coerce")
        df["dropoff_datetime"] = pd.to_datetime(df["dropoff_datetime"], errors="coerce")

        df["pickup_location_id"] = pd.to_numeric(df["pickup_location_id"], errors="coerce").astype("Int64")
        df["dropoff_location_id"] = pd.to_numeric(df["dropoff_location_id"], errors="coerce").astype("Int64")

        df["vendor_id"] = pd.to_numeric(df["vendor_id"], errors="coerce").astype("Int64")
        df["ratecode_id"] = pd.to_numeric(df["ratecode_id"], errors="coerce").astype("Int64")
        df["passenger_count"] = pd.to_numeric(df["passenger_count"], errors="coerce").astype("Int64")
        df["payment_type"] = pd.to_numeric(df["payment_type"], errors="coerce").astype("Int64")

        numeric_cols = [
            "trip_distance",
            "fare_amount",
            "extra",
            "mta_tax",
            "tip_amount",
            "tolls_amount",
            "improvement_surcharge",
            "total_amount",
            "congestion_surcharge"
        ]

        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        # -----------------------
        # DATA QUALITY FILTER
        # -----------------------

        month_start = pd.Timestamp(year=year, month=month, day=1)

        if month == 12:
            month_end = pd.Timestamp(year=year + 1, month=1, day=1)
        else:
            month_end = pd.Timestamp(year=year, month=month + 1, day=1)

        df = df[
            (df["pickup_datetime"] >= month_start) &
            (df["pickup_datetime"] < month_end)
        ]

        df = df[df["dropoff_datetime"] >= df["pickup_datetime"]]

        if "trip_distance" in df.columns:
            df = df[df["trip_distance"].isna() | (df["trip_distance"] >= 0)]

        # -----------------------
        # Metadata
        # -----------------------

        df["taxi_type"] = taxi_type
        df["extracted_at"] = datetime.now(timezone.utc)

        dataframes.append(df)

    if not dataframes:
        return pd.DataFrame()

    return pd.concat(dataframes, ignore_index=True)