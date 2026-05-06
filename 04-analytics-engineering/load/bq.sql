LOAD DATA INTO `kestra-sandbox-493016.nytaxi.green_tripdata`
FROM FILES (
  format = 'CSV',
  uris = ['gs://penny-zoomcamp-taxi-data_0/green/2019/*.csv.gz']
);

LOAD DATA INTO `kestra-sandbox-493016.nytaxi.green_tripdata`
FROM FILES (
  format = 'CSV',
  uris = ['gs://penny-zoomcamp-taxi-data_0/green/2020/*.csv.gz']
);

LOAD DATA INTO `kestra-sandbox-493016.nytaxi.yellow_tripdata`
FROM FILES (
  format = 'CSV',
  uris = ['gs://penny-zoomcamp-taxi-data_0/yellow/2020/*.csv.gz']
);

LOAD DATA INTO `kestra-sandbox-493016.nytaxi.yellow_tripdata`
FROM FILES (
  format = 'CSV',
  uris = ['gs://penny-zoomcamp-taxi-data_0/yellow/2019/*.csv.gz']
);

-- Create a partitioned (by pickup datetime) and clustered table for Yellow taxi
CREATE OR REPLACE TABLE `kestra-sandbox-493016.nytaxi.yellow_tripdata_partitioned`
PARTITION BY DATE(tpep_pickup_datetime)
CLUSTER BY PULocationID, VendorID AS
SELECT *
FROM `kestra-sandbox-493016.nytaxi.yellow_tripdata`;

-- Create a partitioned (by pickup datetime) and clustered table for Green taxi
-- Note: green trip data uses `lpep_pickup_datetime` as the pickup timestamp.
CREATE OR REPLACE TABLE `kestra-sandbox-493016.nytaxi.green_tripdata_partitioned`
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY PULocationID, VendorID AS
SELECT *
FROM `kestra-sandbox-493016.nytaxi.green_tripdata`;

-- Quick checks
SELECT COUNT(*) AS yellow_partitioned_count
FROM `kestra-sandbox-493016.nytaxi.yellow_tripdata_partitioned`;

SELECT COUNT(*) AS green_partitioned_count
FROM `kestra-sandbox-493016.nytaxi.green_tripdata_partitioned`;