/* @bruin
name: staging.trips
type: bq.sql
connection: my-taxi-pipeline

depends:
  - ingestion.trips
  - ingestion.payment_lookup

materialization:
  type: table
  
columns:
  - name: pickup_datetime
    type: timestamp
    checks:
      - name: not_null


@bruin */

SELECT
    t.pickup_datetime,
    t.dropoff_datetime,
    t.pickup_location_id,
    t.dropoff_location_id,
    t.fare_amount,
    t.taxi_type,
    p.payment_type_name
FROM `data-engineering-project-dtc.ingestion.trips` t
LEFT JOIN `data-engineering-project-dtc.ingestion.payment_lookup` p
    ON t.payment_type = p.payment_type_id

-- Filter records to the current execution window.
-- This ensures incremental processing by selecting only trips within the defined start and end datetime boundaries.
WHERE t.pickup_datetime >= TIMESTAMP('{{ start_datetime }}')
  AND t.pickup_datetime < TIMESTAMP('{{ end_datetime }}')

-- Deduplicate records by keeping only the first occurrence of each logical trip (based on key trip attributes).
-- This removes potential duplicates introduced during ingestion.
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY t.pickup_datetime, t.dropoff_datetime,
                 t.pickup_location_id, t.dropoff_location_id, CAST(t.fare_amount AS NUMERIC)
    ORDER BY t.pickup_datetime
) = 1