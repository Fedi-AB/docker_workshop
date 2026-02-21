SELECT *
FROM {{ ref('stg_green_tripdata') }}
WHERE dropoff_datetime < pickup_datetime
