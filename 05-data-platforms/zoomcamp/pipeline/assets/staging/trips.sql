/* @bruin
name: staging.trips
type: duckdb.sql

depends:
  - ingestion.trips
  - ingestion.payment_lookup

materialization:
  type: table
  strategy: time_interval
  incremental_key: pickup_datetime
  time_granularity: timestamp

columns:
  - name: vendor_id
    type: integer
    primary_key: true
    checks:
      - name: not_null
  - name: pickup_datetime
    type: timestamp
    primary_key: true
    checks:
      - name: not_null
  - name: dropoff_datetime
    type: timestamp
    primary_key: true
    checks:
      - name: not_null
  - name: pu_location_id
    type: integer
    primary_key: true
    checks:
      - name: not_null
  - name: do_location_id
    type: integer
    primary_key: true
    checks:
      - name: not_null
  - name: fare_amount
    type: float
    primary_key: true
    checks:
      - name: not_null

custom_checks:
  - name: row_count_positive
    description: Ensures the table is not empty
    query: SELECT COUNT(*) > 0 FROM staging.trips
    value: 1
@bruin */

WITH raw_trips AS (
    SELECT 
        vendor_id,
        pickup_datetime,
        dropoff_datetime,
        passenger_count,
        trip_distance,
        ratecode_id,
        store_and_fwd_flag,
        pu_location_id,
        do_location_id,
        payment_type,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_amount,
        congestion_surcharge,
        airport_fee,
        ehail_fee,
        trip_type,
        taxi_type,
        extracted_at,
        ROW_NUMBER() OVER (
            PARTITION BY vendor_id, pickup_datetime, dropoff_datetime, pu_location_id, do_location_id, fare_amount
            ORDER BY extracted_at DESC
        ) as rn
    FROM ingestion.trips
    WHERE pickup_datetime >= '{{ start_datetime }}'
      AND pickup_datetime < '{{ end_datetime }}'
      AND vendor_id IS NOT NULL
      AND pickup_datetime IS NOT NULL
      AND dropoff_datetime IS NOT NULL
      AND pu_location_id IS NOT NULL
      AND do_location_id IS NOT NULL
      AND fare_amount >= 0
      AND total_amount >= 0
)
SELECT 
    t.vendor_id,
    t.pickup_datetime,
    t.dropoff_datetime,
    t.passenger_count,
    t.trip_distance,
    t.ratecode_id,
    t.store_and_fwd_flag,
    t.pu_location_id,
    t.do_location_id,
    t.payment_type,
    pl.payment_type_name,
    t.fare_amount,
    t.extra,
    t.mta_tax,
    t.tip_amount,
    t.tolls_amount,
    t.improvement_surcharge,
    t.total_amount,
    t.congestion_surcharge,
    t.airport_fee,
    t.ehail_fee,
    t.trip_type,
    t.taxi_type,
    t.extracted_at
FROM raw_trips t
LEFT JOIN ingestion.payment_lookup pl 
  ON CAST(t.payment_type AS INTEGER) = pl.payment_type_id
WHERE t.rn = 1
