# Taxi Ingestion Runbook

## 1) Sanity-check source URL

```bash
wget --spider -S "https://ghfast.top/https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2020-03.csv.gz"
```

## 2) Run modular loader (default targets)

Defaults in `load_taxi_data_v1.py`:
- yellow: every month in 2020
- green: every month in 2019 and 2020

```bash
python 04-analytics-engineering/load/load_taxi_data_v1.py
```

## 3) Run specific subsets

Green only, first quarter of 2020:

```bash
python 04-analytics-engineering/load/load_taxi_data_v1.py \
	--taxi-types green \
	--years 2020 \
	--months 01 02 03
```

Yellow only, all months in 2019:

```bash
python 04-analytics-engineering/load/load_taxi_data_v1.py \
	--taxi-types yellow \
	--years 2019 \
	--months 01 02 03 04 05 06 07 08 09 10 11 12
```

## 4) Upload-only mode

Use this if files already exist locally in the download directory:

```bash
python 04-analytics-engineering/load/load_taxi_data_v1.py --upload-only
```

## 5) Override bucket or download directory

```bash
python 04-analytics-engineering/load/load_taxi_data_v1.py \
	--bucket-name penny-zoomcamp-taxi-data_0 \
	--download-dir data
```

## Notes

- URL examples:
	- yellow: https://ghfast.top/https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2020-03.csv.gz
	- green: https://ghfast.top/https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz
- The script deletes local files after successful upload.

