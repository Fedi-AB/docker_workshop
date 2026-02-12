--===========================================
-- Query public available table in bigquery
--===========================================
SELECT 
    station_id, 
    name 
FROM
    bigquery-public-data.new_york_citibike.citibike_stations
LIMIT 100;

--==============================================================
-- Creating external table referring to gcs path with csv files
--==============================================================
CREATE OR REPLACE EXTERNAL TABLE `data-engineering-project-dtc.de_gcp_dataset_dtc.external_yellow_tripdata`
-- OPTIONS is used to specify the format and location of the data files to be loaded into the external table. In this case, we are specifying that the data files are in CSV format and providing the URIs of the files located in a Google Cloud Storage bucket.
OPTIONS (
  format = 'CSV',
  uris = ['gs://de_bucket_dtc/yellow_tripdata_2019-*.csv', 'gs://de_bucket_dtc/yellow_tripdata_2020-*.csv']
);


--============================================================================================
-- Check yellow trip data external table is created successfully and data is loaded correctly
--============================================================================================
SELECT 
    * 
FROM 
    data-engineering-project-dtc.de_gcp_dataset_dtc.external_yellow_tripdata 
LIMIT 100;


--===========================================================
-- Create a non partitioned table from external table
--===========================================================
CREATE OR REPLACE TABLE `data-engineering-project-dtc.de_gcp_dataset_dtc.yellow_tripdata_non_partioned`
    AS 
    SELECT
        *
    FROM `data-engineering-project-dtc.de_gcp_dataset_dtc.external_yellow_tripdata`;



--===========================================================
-- Create a partitioned table from external table
--===========================================================
CREATE OR REPLACE TABLE `data-engineering-project-dtc.de_gcp_dataset_dtc.yellow_tripdata_partioned` 
PARTITION BY DATE(tpep_pickup_datetime) AS 
    SELECT
        *
    FROM `data-engineering-project-dtc.de_gcp_dataset_dtc.external_yellow_tripdata`;



--===========================================================
--Ipoact of partitioning on query performance
--===========================================================
--1) Querying non-partitioned table
--==> Will scan the entire table : 1,62 Go of data is scanned
SELECT
    DISTINCT(VendorID)
FROM
    `data-engineering-project-dtc.de_gcp_dataset_dtc.yellow_tripdata_non_partioned`

WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2019-06-30';

--2) Querying the partitioned table 
--==> Will scan only the relevant partitions : 106 Mo of data is scanned 
SELECT 
    DISTINCT(VendorID) 
FROM 
    `data-engineering-project-dtc.de_gcp_dataset_dtc.yellow_tripdata_partioned` 
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2019-06-30';



--======================================================
-- Check the number of rows in each partition 
--======================================================
SELECT table_name, partition_id, total_rows
FROM `data-engineering-project-dtc.de_gcp_dataset_dtc.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = 'yellow_tripdata_partioned'
ORDER BY total_rows DESC;



--======================================================
-- Creating a partition and cluster table
--======================================================
CREATE OR REPLACE TABLE `data-engineering-project-dtc.de_gcp_dataset_dtc.yellow_tripdata_partition_cluster`
 PARTITION BY DATE(tpep_pickup_datetime)
 CLUSTER BY VendorID
 AS 
    SELECT 
        * 
    FROM `data-engineering-project-dtc.de_gcp_dataset_dtc.external_yellow_tripdata`;




--===========================================================
--Ipoact of partitioning and clustering on query performance
--===========================================================
--1) Querying partitioned table
--==> Will scan the entire table : 1,06 Go of data is scanned
SELECT 
    COUNT(*) as trips
FROM 
    `data-engineering-project-dtc.de_gcp_dataset_dtc.yellow_tripdata_partioned`
WHERE 
    DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2020-12-31'
    AND VendorID=1;


--2) Querying partitioned & clustered table
--==> Will scan the entire table : 871 Mo of data is scanned
SELECT 
    COUNT(*) as trips
FROM 
    `data-engineering-project-dtc.de_gcp_dataset_dtc.yellow_tripdata_partition_cluster`
WHERE 
    DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2020-12-31'
    AND VendorID=1;