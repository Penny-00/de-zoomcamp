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