{{
    config(
        materialized='table'
    )
}}

with green_trips as (
    select
        vendor_id,
        rate_code_id,
        pickup_location_id,
        dropoff_location_id,
        pickup_datetime,
        dropoff_datetime,
        store_and_fwd_flag,
        passenger_count,
        trip_distance,
        trip_type,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        ehail_fee,
        improvement_surcharge,
        total_amount,
        payment_type,
        'Green' as service_type
    from {{ ref('stg_green_tripdata') }}
),

yellow_trips as (
    select
        vendor_id,
        rate_code_id,
        pickup_location_id,
        dropoff_location_id,
        pickup_datetime,
        dropoff_datetime,
        store_and_fwd_flag,
        passenger_count,
        trip_distance,
        trip_type,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        ehail_fee,
        improvement_surcharge,
        total_amount,
        payment_type,
        'Yellow' as service_type
    from {{ ref('stg_yellow_tripdata') }}
),

trips_unioned as (
    select * from green_trips
    union all
    select * from yellow_trips
),

dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)

select
    trips_unioned.vendor_id,
    trips_unioned.rate_code_id,
    trips_unioned.pickup_location_id,
    trips_unioned.dropoff_location_id,
    trips_unioned.pickup_datetime,
    trips_unioned.dropoff_datetime,
    trips_unioned.store_and_fwd_flag,
    trips_unioned.passenger_count,
    trips_unioned.trip_distance,
    trips_unioned.trip_type,
    trips_unioned.fare_amount,
    trips_unioned.extra,
    trips_unioned.mta_tax,
    trips_unioned.tip_amount,
    trips_unioned.tolls_amount,
    trips_unioned.ehail_fee,
    trips_unioned.improvement_surcharge,
    trips_unioned.total_amount,
    trips_unioned.payment_type,
    trips_unioned.service_type,
    pickup_zone.borough as pickup_borough,
    pickup_zone.zone as pickup_zone,
    dropoff_zone.borough as dropoff_borough,
    dropoff_zone.zone as dropoff_zone
from trips_unioned
inner join dim_zones as pickup_zone
on trips_unioned.pickup_location_id = pickup_zone.location_id
inner join dim_zones as dropoff_zone
on trips_unioned.dropoff_location_id = dropoff_zone.location_id