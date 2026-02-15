-- Creation of a CTE for the dimension table of locations, which will be used in the fact table of taxi rides. This table will contain information about the different locations in New York City, including the borough, zone, and service zone.

WITH dim_locations AS (
    SELECT 
        LocationID AS location_id,
        Borough AS borough,
        Zone AS zone,
        service_zone AS service_zone
    FROM 
        {{ ref('taxi_zone_lookup')}}
)

SELECT 
    * 
FROM 
    dim_locations

