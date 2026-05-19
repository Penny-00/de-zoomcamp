select 
    -- identifiers
    cast(vendorid as int64) as vendor_id,
    cast(ratecodeid as int64) as rate_code_id,
    cast(pulocationid as int64) as pickup_location_id,
    cast(dolocationid as int64) as dropoff_location_id,

    -- timestamps
    cast(tpep_pickup_datetime as timestamp) as pickup_datetime,
    cast(tpep_dropoff_datetime as timestamp) as dropoff_datetime,

    -- trip info
    store_and_fwd_flag,
    cast(passenger_count as int64) as passenger_count,
    cast(trip_distance as float64) as trip_distance,
    1 as trip_type, -- yellow taxi trip type is always 1

    -- payment info
    cast(fare_amount as numeric) as fare_amount,
    cast(extra as numeric) as extra,
    cast(mta_tax as numeric) as mta_tax,
    cast(tip_amount as numeric) as tip_amount,
    cast(tolls_amount as numeric) as tolls_amount,
    cast(improvement_surcharge as numeric) as improvement_surcharge,
    0 as ehail_fee, -- yellow taxis do not have ehail fee
    cast(total_amount as numeric) as total_amount,
    cast(payment_type as int64) as payment_type

from {{ source('raw_data', 'yellow_tripdata_partitioned') }}

-- filter out records with null vendor_id (data quality requirement)
where vendorid is not null
