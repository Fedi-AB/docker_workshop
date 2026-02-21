
SELECT 
    *   
FROM {{ source('raw_data', 'fhv_tripdata')}}

-- Exclude nulls IDs for vendorID, as they are required for analysis
WHERE dispatching_base_num IS NOT NULL
