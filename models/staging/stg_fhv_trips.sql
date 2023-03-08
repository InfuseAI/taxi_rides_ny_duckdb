with tripdata as 
(
  select *,
    row_number() over(partition by pickup_datetime) as rn
  from {{ source('staging','fhv_tripdata_2019') }}
)
select
   -- identifiers
    {{ dbt_utils.generate_surrogate_key(['pickup_datetime']) }} as tripid,
    dispatching_base_num as dispatching_base_num,
    Affiliated_base_number as Affiliated_base_number,
    cast(PUlocationid as integer) as  pickup_locationid,
    cast(DOlocationid as integer) as dropoff_locationid,
    
    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropOff_datetime as timestamp) as dropoff_datetime,
    
    -- trip info
    SR_Flag,
    
from tripdata
