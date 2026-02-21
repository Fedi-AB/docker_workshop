#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import click

# ## Install SQLAlchemy
from sqlalchemy import create_engine
# ## Adding Progress Bar
from tqdm.auto import tqdm


# ## Handling Data Types

dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]

# ## Ingesting Data in Chunks : ##
# ** We don't want to insert all the data at once. Let's do it in batches (size of 100000) and use an iterator for that **

@click.command()
@click.option('--pg-user', default='root', show_default=True, help='Postgres user')
@click.option('--pg-password', default='root', show_default=True, help='Postgres password')
@click.option('--pg-host', default='localhost', show_default=True, help='Postgres host')
@click.option('--pg-port', default=5432, show_default=True, type=int, help='Postgres port')
@click.option('--pg-database', default='ny_taxi', show_default=True, help='Postgres database name')
@click.option('--year', default=2021, show_default=True, type=int, help='Year of the dataset')
@click.option('--month', default=1, show_default=True, type=int, help='Month of the dataset (1-12)')
@click.option('--table_name', default='yellow_taxi_data', show_default=True, help='table name')
@click.option('--chunksize', default=100000, show_default=True, type=int, help='CSV chunksize for ingestion')
def run(pg_user, pg_password, pg_host, pg_port, pg_database, year, month, table_name, chunksize):
    if not (1 <= month <= 12):
        raise click.BadParameter('month doit être entre 1 et 12')

    # ## Create Database Connection
    prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/' 
    url = f'{prefix}/yellow_tripdata_{year}-{month:02d}.csv.gz'
    
    # Create the connection engine to the Postgres database
    engine = create_engine(f'postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_database}')
   
    # Create an iterator to read the CSV file in chunks of specified size
    df_iter = pd.read_csv(
        url,
        dtype=dtype,
        parse_dates=parse_dates,
        iterator=True,
        chunksize = chunksize,
    )

    first = True

    #Iterate Over Chunks
    for df_chunk in tqdm(df_iter):
        if first:
            # Create table schema (no data)
            # ** head(n=0) makes sure we only create the table, we don't add any data yet. **
            df_chunk.head(0).to_sql(
                name=table_name, 
                con=engine, 
                if_exists='replace'
            )
            first = False

        # ## Create the Table by appending the first chunk:
        df_chunk.to_sql(
            name=table_name, 
            con=engine, 
            if_exists='append'
            )

# Run the ingestion process 
if __name__ == '__main__':
    run()