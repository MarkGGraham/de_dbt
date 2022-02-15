{{ config(materialized='table') }}

with dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select 
    fhv_trips.tripid,
    fhv_trips.dispatch_base,
    fhv_trips.affiliated_base,
    fhv_trips.pickup_locationid,
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    fhv_trips.dropoff_locationid,
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone,  
    fhv_trips.pickup_datetime, 
    fhv_trips.dropoff_datetime, 
    fhv_trips.shared_ride_indicator
from {{ ref('stg_fhv_tripdata') }} as fhv_trips
inner join dim_zones as pickup_zone
on fhv_trips.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on fhv_trips.dropoff_locationid = dropoff_zone.locationid