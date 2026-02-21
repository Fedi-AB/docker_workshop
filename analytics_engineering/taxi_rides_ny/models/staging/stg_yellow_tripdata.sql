
SELECT 

    -- Identifiers (standardized naming for consistency across yellow and green datasets)
    CAST(vendorid AS INTEGER) AS vendor_id,

    -- SAFE_CAST is used here to handle any potential non-integer values in the 'ratecodeid' column, which could cause errors during casting. If a value cannot be cast to INTEGER, it will return NULL instead of throwing an error. The "double curly braces" is not SQL native syntax but is used in dbt to indicate that this is a Jinja expression, allowing us to use the SAFE_CAST function provided by dbt for safer type casting.

    -- The macro 'safe_cast' is defined in the 'safe_cast.sql' file within the 'macros' directory of the dbt project, and it abstracts away the differences between SQL dialects (like BigQuery and DuckDB) for safe casting operations.
    {{ safe_cast('ratecodeid', 'INTEGER') }} AS rate_code_id,
    CAST(pulocationid AS INTEGER) AS pickup_location_id,
    CAST(dolocationid AS INTEGER) AS dropoff_location_id,
    'yellow' AS service_type,

    -- Timestamps
    CAST(tpep_pickup_datetime AS TIMESTAMP) AS pickup_datetime,
    CAST(tpep_dropoff_datetime AS TIMESTAMP) AS dropoff_datetime,

    -- Trip details/infos
    CAST(store_and_fwd_flag AS STRING) AS store_and_fwd_flag,
    CAST(passenger_count AS INTEGER) AS passenger_count,
    CAST(trip_distance AS NUMERIC) AS trip_distance,

    -- yellow table don't have the 'trip_type' column. We can add it with a default value since all trips in the yellow dataset are street-hail trips (which corresponds to a trip_type of 1 in the green dataset).
    -- So we can set this to 1 for all records in this dataset
    cast(1 as integer) as trip_type,  

    -- Payement infos /financial details
    CAST(fare_amount AS NUMERIC) AS fare_amount,
    CAST(extra AS NUMERIC) AS extra,
    CAST(mta_tax AS NUMERIC) AS mta_tax,
    CAST(tip_amount AS NUMERIC) AS tip_amount,

    -- yellow table dont't have the 'ehail_fee' column.
    -- We can add it with a default value of 0, since this fee is only applicable to e-hail trips (which are not present in the yellow dataset).
    CAST(0 AS NUMERIC) AS ehail_fee, 
    CAST(tolls_amount AS NUMERIC) AS tolls_amount,
    CAST(improvement_surcharge AS NUMERIC) AS improvement_surcharge,
    CAST(total_amount AS NUMERIC) AS total_amount,

    -- SAFE_CAST is used here to handle any potential non-integer values in the 'payment_type' column, which could cause errors during casting. If a value cannot be cast to INTEGER, it will return NULL instead of throwing an error. The "double curly braces" is not SQL native syntax but is used in dbt to indicate that this is a Jinja expression, allowing us to use the SAFE_CAST function provided by dbt for safer type casting.

    -- The macro 'safe_cast' is defined in the 'safe_cast.sql' file within the 'macros' directory of the dbt project, and it abstracts away the differences between SQL dialects (like BigQuery and DuckDB) for safe casting operations.
    {{ safe_cast('payment_type', 'INTEGER') }} AS payment_type
    
FROM {{ source('raw_data', 'yellow_tripdata')}}

-- Exclude nulls IDs for vendorID, as they are required for analysis
WHERE vendorID IS NOT NULL AND (tpep_pickup_datetime >= '2019-01-01'
  AND tpep_pickup_datetime < '2021-01-01')