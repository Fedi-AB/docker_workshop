SELECT *
FROM {{ ref('stg_yellow_tripdata') }}
WHERE dropoff_datetime < pickup_datetime
