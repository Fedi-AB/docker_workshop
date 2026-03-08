/* @bruin

# Docs:
# - SQL assets: https://getbruin.com/docs/bruin/assets/sql
# - Materialization: https://getbruin.com/docs/bruin/assets/materialization
# - Quality checks: https://getbruin.com/docs/bruin/quality/available_checks

# Set the asset name (recommended: reports.trips_report).
name: reports.trips_report

# Set platform type.
# Docs: https://getbruin.com/docs/bruin/assets/sql
# suggested type: duckdb.sql
type: bq.sql
connection: my-taxi-pipeline
location: EU

# Declare dependency on the staging asset(s) this report reads from.
depends:
  - staging.trips

# Choose materialization strategy.
# For reports, `time_interval` is a good choice to rebuild only the relevant time window.
# Important: Use the same `incremental_key` as staging (e.g., pickup_datetime) for consistency.
materialization:
  type: table
  # suggested strategy: time_interval
  strategy: time_interval
  # set to your report's date column
  incremental_key: trip_date
  # set to `date` or `timestamp`
  time_granularity: date

# Define report columns + primary key(s) at your chosen level of aggregation.
columns:
  - name: trip_date
    type: date
    description: The date of the trip (derived from pickup_datetime).
    primary_key: true
  - name: taxi_type
    type: string
    description: The type of taxi (e.g., yellow, green).
    primary_key: true
  - name: payment_type
    type: string
    description: The method of payment (e.g., credit card, cash).
    primary_key: true
  - name: trip_count
    type: bigint
    description: The total number of trips for the given date, taxi 
    checks:
      - name: non_negative
  - name: total_fare
    type: float
  - name: avg_fare
    type: float

@bruin */

-- Purpose of reports:
-- - Aggregate staging data for dashboards and analytics
-- Required Bruin concepts:
-- - Filter using `{{ start_datetime }}` / `{{ end_datetime }}` for incremental runs
-- - GROUP BY your dimension + date columns

SELECT
    CAST(pickup_datetime AS DATE) AS trip_date,
    taxi_type,
    payment_type_name AS payment_type,
    COUNT(*) AS trip_count,
    SUM(fare_amount) AS total_fare,
    AVG(fare_amount) AS avg_fare

FROM `data-engineering-project-dtc.staging.trips`
WHERE pickup_datetime >= '{{ start_datetime }}'
  AND pickup_datetime < '{{ end_datetime }}'

GROUP BY 1, 2, 3
ORDER BY trip_date DESC;