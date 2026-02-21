/*
Incremental model configuration:

- materialized='incremental'
  Builds the table incrementally instead of recreating it on every run.
  Only new or updated records are processed, which improves performance
  and reduces warehouse compute costs for large datasets.

- unique_key='trip_id'
  Defines the primary key used to identify unique records.
  Required for merge-based incremental models to prevent duplicates
  and ensure correct updates of existing rows.

- incremental_strategy='merge'
  Uses a MERGE statement to:
      • Insert new records
      • Update existing records when the unique_key already exists
  This provides idempotent behavior and ensures data consistency.

- on_schema_change='append_new_columns'
  Automatically adds new columns to the target table if the model schema evolves.
  Prevents failures when new fields are introduced upstream.
*/


{{
  config(
    materialized='incremental',
    unique_key='trip_id',
    incremental_strategy='merge',
    on_schema_change='append_new_columns'  )
}}


-- Fact table containing all taxi trips enriched with zone information
-- Materialized incrementally to handle large datasets efficiently
SELECT 
    -- Identifiers
    it.trip_id,
    it.vendor_id,
    it.service_type,
    it.rate_code_id,

    -- Location IDs
    it.pickup_location_id,
    pdl.borough AS pickup_borough,
    pdl.zone AS pickup_zone,
    it.dropoff_location_id,
    ddl.borough AS dropoff_borough,
    ddl.zone AS dropoff_zone,

    -- Trip timing
    it.pickup_datetime,
    it.dropoff_datetime,
    it.store_and_fwd_flag,

    -- Trip details/metrics
    it.passenger_count,
    it.trip_distance,
    it.trip_type,
    {{ get_trip_duration_minutes ('it.pickup_datetime', 'it.dropoff_datetime') }} AS trip_duration_minutes,

    -- Payement infos /financial details
    it.fare_amount,
    it.extra,
    it.mta_tax,
    it.tip_amount,
    it.ehail_fee,
    it.tolls_amount,
    it.improvement_surcharge,
    it.total_amount,
    it.payment_type,
    it.payment_type_description

    
FROM 
    {{ ref ('int_trips') }} AS it

LEFT JOIN {{ ref ('dim_locations') }} AS pdl
    ON it.pickup_location_id = pdl.location_id
LEFT JOIN {{ ref('dim_locations') }} AS ddl
    ON it.dropoff_location_id = ddl.location_id


/*
Incremental filtering logic:

This condition is executed only when the model runs in incremental mode.
On the first run (when the target table does not yet exist),
the filter is ignored and the full dataset is loaded.

On subsequent runs, the model processes only new records
by comparing the source pickup_datetime with the maximum
pickup_datetime already stored in the target table ({{ this }}).

This approach:
  • Prevents reprocessing historical data
  • Reduces compute cost and execution time
  • Enables scalable loading for large datasets

Note:
This strategy assumes that pickup_datetime is strictly increasing.
If late-arriving data is possible, a lookback window strategy
may be required to avoid missing records.
*/
{% if is_incremental() %}
  -- Only process new trips based on pickup datetime
  where it.pickup_datetime > (select max(pickup_datetime) from {{ this }})
{% endif %}