--------------------------------------------------------------------------------------------------------------------------
-- Creating a CTE for each of the two datasets (green and yellow) to select all columns from the respective staging models. We will then union these two CTEs together to create a single dataset that contains all trips from both datasets.
--------------------------------------------------------------------------------------------------------------------------

WITH green_tripdata AS (
    SELECT
        *
    FROM {{ ref('stg_green_tripdata')}}
),

yellow_tripdata AS (
    SELECT
        *
    FROM {{ ref('stg_yellow_tripdata')}}
),

-- Now we can union the two datasets together. Since we have standardized the column names and data types in the staging models, we can simply use a UNION ALL to combine the two datasets without worrying about any mismatches in schema.
trips_unioned AS (
    SELECT * FROM green_tripdata
    UNION ALL
    SELECT * FROM yellow_tripdata
)

SELECT * FROM trips_unioned

