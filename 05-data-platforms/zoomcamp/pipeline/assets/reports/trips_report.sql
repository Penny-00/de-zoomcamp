/* @bruin
name: reports.trips_report
type: duckdb.sql

depends:
  - staging.trips

materialization:
  type: table
  strategy: time_interval
  incremental_key: pickup_date
  time_granularity: date

columns:
  - name: pickup_date
    type: date
    primary_key: true
    checks:
      - name: not_null
  - name: taxi_type
    type: string
    primary_key: true
    checks:
      - name: not_null
  - name: payment_type
    type: string
    primary_key: true
    checks:
      - name: not_null
  - name: trip_count
    type: integer
    checks:
      - name: non_negative
  - name: total_amount
    type: float
    checks:
      - name: non_negative
  - name: fare_amount
    type: float
    checks:
      - name: non_negative

@bruin */

SELECT 
    CAST(pickup_datetime AS DATE) AS pickup_date,
    taxi_type,
    COALESCE(payment_type_name, 'unknown') AS payment_type,
    COUNT(*) AS trip_count,
    SUM(total_amount) AS total_amount,
    SUM(fare_amount) AS fare_amount,
    SUM(tip_amount) AS tip_amount,
    SUM(tolls_amount) AS tolls_amount,
    SUM(passenger_count) AS passenger_count,
    SUM(trip_distance) AS trip_distance
FROM staging.trips
WHERE pickup_datetime >= '{{ start_datetime }}'
  AND pickup_datetime < '{{ end_datetime }}'
GROUP BY 
    CAST(pickup_datetime AS DATE),
    taxi_type,
    COALESCE(payment_type_name, 'unknown')
