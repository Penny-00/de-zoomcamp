{{ config(materialized='table') }}

with source as (
    select * from {{ source('raw_fhv', 'fhv_trips') }}
),

renamed as (
    select
        cast(pickup_datetime as timestamp) as pickup_datetime,
        cast(drop_off_datetime as timestamp) as dropoff_datetime,
        cast(dispatching_base_num as string) as dispatching_base_num,
        cast(p_ulocation_id as integer) as pickup_location_id,
        cast(d_olocation_id as integer) as dropoff_location_id,
        cast(sr_flag as string) as sr_flag,
        cast(affiliated_base_number as string) as affiliated_base_number
    from source
    where dispatching_base_num is not null
)

select * from renamed

-- Sample records for dev environment using deterministic date filter
{% if target.name == 'dev' %}
where pickup_datetime >= '2019-02-01' and pickup_datetime < '2019-03-01'
{% endif %}