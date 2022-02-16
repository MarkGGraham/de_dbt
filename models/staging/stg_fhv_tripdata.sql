{{ config(materialized='view') }}

with tripdata as 
(
  select *,
    row_number() over(partition by dispatching_base_num, pickup_datetime) as rn
  from {{ source('staging','fhv_tripdata') }}
)
select  
    -- Identifiers
    {{ dbt_utils.surrogate_key(['dispatching_base_num', 'pickup_datetime']) }} as tripid,
    dispatching_base_num as dispatch_base,
    -- Affiliated_base_number as affiliated_base,	
    cast (PULocationID as Integer) as pickup_locationid,
    cast (DOLocationID as Integer) as dropoff_locationid,

    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,

    -- trip info
    case when SR_Flag = 1 then 'Yes' else 'No' end as shared_ride_indicator

from tripdata
-- where rn=1

{% if var('is_test_run', default=true) %}
    limit 100
{% endif %}