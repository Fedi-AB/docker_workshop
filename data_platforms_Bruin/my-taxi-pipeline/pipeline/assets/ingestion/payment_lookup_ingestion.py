"""@bruin

name: ingestion.payment_lookup

type: python

image: python:3.11

connection: my-taxi-pipeline

materialization:
  type: table

columns:
  - name: payment_type_id
    type: bigint
    primary_key: true

  - name: payment_type_name
    type: varchar

@bruin"""

import pandas as pd
import os

def materialize():

    current_dir = os.path.dirname(__file__)
    csv_path = os.path.join(current_dir, "payment_lookup.csv")

    df = pd.read_csv(csv_path)

    return df