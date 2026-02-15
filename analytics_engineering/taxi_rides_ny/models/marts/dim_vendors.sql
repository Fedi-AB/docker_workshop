-- For vendors dimension table we will use two ways. The first with classic CTE and CASE WHEN statement. The second ways we xill use macro with if statement.
-- 1) With CTE and CASE WHEN statement
--WITH dim_vendors AS (
--    SELECT 
--        DISTINCT vendor_id,
--        CASE
--            WHEN vendor_id = 1 THEN 'Creative Mobile Technologies, LLC'
--            WHEN vendor_id = 2 THEN 'VeriFone Inc.'
--            ELSE 'Unknown'
--        END AS vendor_name

--    FROM 
--        {{ ref('int_trips_unioned')}}
--)

--SELECT
--    *
--FROM
--    dim_vendors

---------------------------------------------------------------------------------------------------------------------
-- 2) With macro and if statement
-- The macro will be defined in the macros directory, and it will take the vendor_id as an argument and return the corresponding vendor name.

WITH dim_vendors AS (
    SELECT
        DISTINCT vendor_id,
        {{ get_vendor_name('vendor_id') }} AS vendor_name

    FROM
        {{ ref('int_trips_unioned')}}   
)

SELECT
    *
FROM
    dim_vendors