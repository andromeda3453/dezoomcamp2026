-- Unioning yellow and green datasets to create one single trips fact table 
with green_tripdata as (
    select * from {{ ref('stg_kestra_dataset__greentrip_data') }}
),
yellow_tripdata as (
    select * from {{ ref('stg_kestra_dataset__yellowtrip_data') }}
),
trips_unioned as (
    select *, "green" as taxi_type from green_tripdata
    union all
    select *, "yellow" as taxi_type from yellow_tripdata
)

select * from trips_unioned 