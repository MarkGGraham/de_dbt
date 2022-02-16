{{ config(materialized='table') }}

with dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select 
    fhv_trips.tripid,
    fhv_trips.dispatch_base,
    -- fhv_trips.affiliated_base,
    coalesce(fhv_trips.pickup_locationid, 256) as pickup_locationid,
    coalesce(pickup_zone.borough, 'Unknown') as pickup_borough, 
    coalesce(pickup_zone.zone, 'Unknown') as pickup_zone, 
    coalesce(fhv_trips.dropoff_locationid, 256) as dropoff_locationid,
    coalesce(dropoff_zone.borough, 'Unknown') as dropoff_borough, 
    coalesce(dropoff_zone.zone, 'Unknown') as dropoff_zone,  
    fhv_trips.pickup_datetime, 
    fhv_trips.dropoff_datetime, 
    fhv_trips.shared_ride_indicator
from {{ ref('stg_fhv_tripdata') }} as fhv_trips
inner join dim_zones as pickup_zone
on fhv_trips.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on fhv_trips.dropoff_locationid = dropoff_zone.locationid