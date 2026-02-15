-- Enrich and deduplicate trip data
-- Demonstrates enrichment and surrogate key generation

WITH trips_unioned AS (
    SELECT
        *
    FROM {{ ref('int_trips_unioned')}}
),

-- Create a CTE for the payment type lookup to enrich the trip data with descriptive payment type information. This will allow us to have more meaningful insights when analyzing the trip data based on payment types.
payment_type AS (
    SELECT
       *
    FROM {{ ref('payment_type_lookup') }}
),

-- Create a CTE for cleaning and adding surrogate keys to the trip data
cleaned_trips AS (
    SELECT
        --Create surrogate key by concatenating the pickup datetime and the unique trip identifier (e.g., trip_id) to ensure uniqueness across both datasets. Using the generate_surrogate_key macro.
        {{ generate_surrogate_key(['tu.vendor_id', 'tu.pickup_datetime', 'tu.pickup_location_id', 'tu.service_type']) }} as trip_id,

        -- Identifiers
        tu.vendor_id,
        tu.service_type,
        tu.rate_code_id,

        -- Location IDs
        tu.pickup_location_id,
        tu.dropoff_location_id,

        -- Timestamps
        tu.pickup_datetime,
        tu.dropoff_datetime,

        -- Trip details/infos
        tu.store_and_fwd_flag,
        tu.passenger_count,
        tu.trip_distance,
        tu.trip_type,

        -- Payement infos /financial details
        tu.fare_amount,
        tu.extra,
        tu.mta_tax,
        tu.tip_amount,
        tu.ehail_fee,
        tu.tolls_amount,
        tu.improvement_surcharge,
        tu.total_amount,

        -- Hundle potential nulls in payment_type
        COALESCE(pt.payment_type, 0) AS payment_type,

        -- Create a new column from payment type lookup "description" and hundle the potential nulls.
        COALESCE(pt.description, 'Unknown') AS payment_type_description

    FROM trips_unioned AS tu
    LEFT JOIN payment_type AS pt
        ON COALESCE(tu.payment_type, 0) = pt.payment_type
)

SELECT
    *
FROM cleaned_trips