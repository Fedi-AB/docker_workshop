SELECT *
FROM {{ ref('stg_yellow_tripdata') }}
WHERE pickup_datetime < '2019-01-01'
   OR pickup_datetime >= '2021-01-01'
