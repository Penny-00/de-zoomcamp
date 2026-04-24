-- Query public available table
SELECT station_id, name FROM
    bigquery-public-data.new_york_citibike.citibike_stations
LIMIT 100;


-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `taxi-rides-ny.nytaxi.external_yellow_tripdata`
OPTIONS (
  format = 'CSV',
  uris = ['gs://nyc-tl-data/trip data/yellow_tripdata_2019-*.csv', 'gs://nyc-tl-data/trip data/yellow_tripdata_2020-*.csv']
);

-- Check yellow trip data
SELECT * FROM taxi-rides-ny.nytaxi.external_yellow_tripdata limit 10;

-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE taxi-rides-ny.nytaxi.yellow_tripdata_non_partitioned AS
SELECT * FROM taxi-rides-ny.nytaxi.external_yellow_tripdata;


-- Create a partitioned table from external table
CREATE OR REPLACE TABLE taxi-rides-ny.nytaxi.yellow_tripdata_partitioned
PARTITION BY
  DATE(tpep_pickup_datetime) AS
SELECT * FROM taxi-rides-ny.nytaxi.external_yellow_tripdata;

-- Impact of partition
-- Scanning 1.6GB of data
SELECT DISTINCT(VendorID)
FROM taxi-rides-ny.nytaxi.yellow_tripdata_non_partitioned
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2019-06-30';

-- Scanning ~106 MB of DATA
SELECT DISTINCT(VendorID)
FROM taxi-rides-ny.nytaxi.yellow_tripdata_partitioned
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2019-06-30';

-- Let's look into the partitions
SELECT table_name, partition_id, total_rows
FROM `nytaxi.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = 'yellow_tripdata_partitioned'
ORDER BY total_rows DESC;

-- Creating a partition and cluster table
CREATE OR REPLACE TABLE taxi-rides-ny.nytaxi.yellow_tripdata_partitioned_clustered
PARTITION BY DATE(tpep_pickup_datetime)
CLUSTER BY VendorID AS
SELECT * FROM taxi-rides-ny.nytaxi.external_yellow_tripdata;

-- Query scans 1.1 GB
SELECT count(*) as trips
FROM taxi-rides-ny.nytaxi.yellow_tripdata_partitioned
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2020-12-31'
  AND VendorID=1;

-- Query scans 864.5 MB
SELECT count(*) as trips
FROM taxi-rides-ny.nytaxi.yellow_tripdata_partitioned_clustered
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2020-12-31'
  AND VendorID=1;


-- Answers  to the assignment questions

-- before creating external table, we need to create a dataset (schema) to hold the table
CREATE SCHEMA `kestra-sandbox-493016.zoomcamp_dataset`;

-- Create an external table referring to the parquet files in gcs
CREATE OR REPLACE EXTERNAL TABLE `kestra-sandbox-493016.zoomcamp_dataset.yellow_taxi_external`
OPTIONS (
  format = 'PARQUET',
  uris = [
    'gs://penny-zoomcamp-taxi-data_0/yellow_tripdata_2024-01.parquet',
    'gs://penny-zoomcamp-taxi-data_0/yellow_tripdata_2024-02.parquet',
    'gs://penny-zoomcamp-taxi-data_0/yellow_tripdata_2024-03.parquet',
    'gs://penny-zoomcamp-taxi-data_0/yellow_tripdata_2024-04.parquet',
    'gs://penny-zoomcamp-taxi-data_0/yellow_tripdata_2024-05.parquet',
    'gs://penny-zoomcamp-taxi-data_0/yellow_tripdata_2024-06.parquet'
  ]
);


-- Check the count of records in the external table
SELECT COUNT(*) FROM `kestra-sandbox-493016.zoomcamp_dataset.yellow_taxi_external`;


-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE `kestra-sandbox-493016.zoomcamp_dataset.yellow_taxi_regular` AS
SELECT *
FROM `kestra-sandbox-493016.zoomcamp_dataset.yellow_taxi_external`;


-- Check the count of records in the non partitioned table
SELECT COUNT(*) AS record_count
FROM zoomcamp_dataset.yellow_taxi_regular;
  

-- Check the count of distinct PULocationID in both tables to understand the power of columnar storage
SELECT COUNT(DISTINCT PULocationID) AS distinct_pulocationids
FROM zoomcamp_dataset.yellow_taxi_external;

SELECT COUNT(DISTINCT PULocationID) AS distinct_pulocationids
FROM zoomcamp_dataset.yellow_taxi_regular;


-- Check the distinct PULocationID values in the non partitioned table
SELECT PULocationID
FROM zoomcamp_dataset.yellow_taxi_regular;

-- Check the distinct PULocationID and DOLocationID values to understand BQ query's cost optimization
SELECT PULocationID, DOLocationID
FROM zoomcamp_dataset.yellow_taxi_regular;


-- Check the count of zero fare trips in the non partitioned table
SELECT COUNT(*) AS zero_fare_trips
FROM zoomcamp_dataset.yellow_taxi_regular
WHERE fare_amount = 0;

-- Create a partitioned and clustered table from non partitioned table
CREATE OR REPLACE TABLE `kestra-sandbox-493016.zoomcamp_dataset.yellow_taxi_partitioned_clustered`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT *
FROM `kestra-sandbox-493016.zoomcamp_dataset.yellow_taxi_regular`;


-- Costing for Original (non-partitioned) table
SELECT DISTINCT VendorID
FROM kestra-sandbox-493016.zoomcamp_dataset.yellow_taxi_regular
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15'
ORDER BY VendorID;

-- Costing for Partitioned + clustered table
SELECT DISTINCT VendorID
FROM kestra-sandbox-493016.zoomcamp_dataset.yellow_taxi_partitioned_clustered
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15'
ORDER BY VendorID;


SELECT COUNT(*) AS total_records
FROM kestra-sandbox-493016.zoomcamp_dataset.yellow_taxi_regular;
