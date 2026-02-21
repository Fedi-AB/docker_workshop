#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import click
from sqlalchemy import create_engine
from sqlalchemy.types import BigInteger, Integer, String

# -----------------------------
# CSV schema
# -----------------------------
dtype = {
    "LocationID": "Int64",
    "Borough": "string",
    "Zone": "string",
    "service_zone": "string"
}

# -----------------------------
# CLI
# -----------------------------
@click.command()
@click.option('--pg-user', default='root', show_default=True)
@click.option('--pg-password', default='root', show_default=True)
@click.option('--pg-host', default='localhost', show_default=True)
@click.option('--pg-port', default=5432, show_default=True, type=int)
@click.option('--pg-database', default='ny_taxi', show_default=True)
@click.option('--table-name', default='taxi_zone_lookup', show_default=True)
def run(pg_user, pg_password, pg_host, pg_port, pg_database, table_name):

    url = (
        "https://raw.githubusercontent.com/"
        "Fedi-AB/docker_workshop/main/CSV_files/taxi_zone_lookup.csv"
    )

    print(f"📥 Downloading data from {url}")

    df = pd.read_csv(url, dtype=dtype)

    # -----------------------------
    # Create index column (0,1,2,...) as FIRST column
    # -----------------------------
    df.reset_index(inplace=True)   # index → column
    df.rename(columns={"index": "index"}, inplace=True)

    engine = create_engine(
        f"postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_database}"
    )

    print("🗄️ Writing data to PostgreSQL...")

    df.to_sql(
        name=table_name,
        con=engine,
        if_exists="replace",
        index=False,
        dtype={
            "index": BigInteger(),
            "LocationID": Integer(),
            "Borough": String(),
            "Zone": String(),
            "service_zone": String()
        }
    )

    print(f"✅ Table `{table_name}` successfully ingested ({len(df)} rows)")

# -----------------------------
if __name__ == '__main__':
    run()
