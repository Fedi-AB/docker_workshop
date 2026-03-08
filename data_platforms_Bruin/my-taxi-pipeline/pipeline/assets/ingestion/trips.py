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
import requests
from io import BytesIO
from datetime import datetime, timezone


def materialize():

    # -----------------------------------------------------------
    # Determine ingestion window using Bruin runtime context
    # -----------------------------------------------------------

    start_date = pd.to_datetime(os.environ["BRUIN_START_DATE"])

    year = start_date.year
    month = start_date.month

    # -----------------------------------------------------------
    # Retrieve pipeline variables (taxi types to ingest)
    # -----------------------------------------------------------

    vars_dict = json.loads(os.environ.get("BRUIN_VARS", "{}"))
    taxi_types = vars_dict.get("taxi_types", ["green"])

    dataframes = []

    # -----------------------------------------------------------
    # Canonical schema used to standardize all taxi datasets
    # -----------------------------------------------------------

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

    # -----------------------------------------------------------
    # HTTP headers to prevent CloudFront 403 errors
    # -----------------------------------------------------------

    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; BruinPipeline/1.0)",
        "Accept": "*/*"
    }

    # -----------------------------------------------------------
    # Iterate through each taxi dataset (green, yellow, fhv)
    # -----------------------------------------------------------

    for taxi_type in taxi_types:

        url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{year}-{month:02d}.parquet"
        print(f"Fetching: {url}")

        # -----------------------------------------------------------
        # Download dataset using requests
        # This avoids CloudFront restrictions in cloud environments
        # -----------------------------------------------------------

        try:

            response = requests.get(url, headers=headers, timeout=120)
            response.raise_for_status()

            df = pd.read_parquet(BytesIO(response.content))

        except Exception as e:

            print(f"Failed loading {url}")
            print(e)
            continue

        # -----------------------------------------------------------
        # Normalize column names depending on taxi dataset format
        # -----------------------------------------------------------

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

        # -----------------------------------------------------------
        # Ensure all canonical columns exist
        # Missing columns are added as NULL
        # -----------------------------------------------------------

        for col in canonical_columns:
            if col not in df.columns:
                df[col] = None

        # -----------------------------------------------------------
        # Add ingestion metadata
        # -----------------------------------------------------------

        df["taxi_type"] = taxi_type
        df["extracted_at"] = datetime.now(timezone.utc)

        # -----------------------------------------------------------
        # Reorder columns according to canonical schema
        # -----------------------------------------------------------

        df = df[canonical_columns]

        # -----------------------------------------------------------
        # Normalize datatypes for analytics compatibility
        # -----------------------------------------------------------

        df["pickup_datetime"] = pd.to_datetime(df["pickup_datetime"], errors="coerce")
        df["dropoff_datetime"] = pd.to_datetime(df["dropoff_datetime"], errors="coerce")

        df["pickup_location_id"] = pd.to_numeric(
            df["pickup_location_id"], errors="coerce"
        ).astype("Int64")

        df["dropoff_location_id"] = pd.to_numeric(
            df["dropoff_location_id"], errors="coerce"
        ).astype("Int64")

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
            "congestion_surcharge",
        ]

        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        # -----------------------------------------------------------
        # Remove rows where the incremental key is null
        # Required for delete+insert incremental strategy
        # -----------------------------------------------------------

        df = df[df["pickup_datetime"].notna()]

        # -----------------------------------------------------------
        # Data quality filters
        # Keep only trips within the ingestion month
        # -----------------------------------------------------------

        month_start = pd.Timestamp(year=year, month=month, day=1)

        if month == 12:
            month_end = pd.Timestamp(year=year + 1, month=1, day=1)
        else:
            month_end = pd.Timestamp(year=year, month=month + 1, day=1)

        df = df[
            (df["pickup_datetime"] >= month_start)
            & (df["pickup_datetime"] < month_end)
        ]

        # Remove trips with invalid timestamps
        df = df[df["dropoff_datetime"] >= df["pickup_datetime"]]

        # Remove negative trip distances
        if "trip_distance" in df.columns:
            df = df[df["trip_distance"].isna() | (df["trip_distance"] >= 0)]

        dataframes.append(df)

    # -----------------------------------------------------------
    # Safety check if no dataset could be downloaded
    # -----------------------------------------------------------

    if not dataframes:

        raise ValueError(
            "No dataset could be downloaded. Check dataset availability or network access."
        )

    # -----------------------------------------------------------
    # Concatenate all datasets into one dataframe
    # -----------------------------------------------------------

    result = pd.concat(dataframes, ignore_index=True)

    print(f"Rows loaded: {len(result)}")

    return result