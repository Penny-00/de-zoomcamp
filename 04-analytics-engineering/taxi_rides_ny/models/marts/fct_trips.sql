/*
To do: 
- One row per trip doesnt matter if yellow or Green
- Add a primary key (trip_id). It has to be unique
- Find all the duplicates, understand why they happen and fix them 
- Find a way to enrich the column payment_type you'll use provided seeds
*/



with trips_unioned as (
    select * from {{ ref('int_trips_unioned') }}
),
trips as (
    select
        trip_id,
        vendor_id,
        pickup_datetime,
        dropoff_datetime,
        passenger_count,
        trip_distance,
        pickup_location_id,
        dropoff_location_id,
        payment_type,
        fare_amount,
        extra,
        mta_tax,
        tip_amount, 
        tolls_amount,
        improvement_surcharge,
        total_amount
    from trips_unioned
)
select * from trips


