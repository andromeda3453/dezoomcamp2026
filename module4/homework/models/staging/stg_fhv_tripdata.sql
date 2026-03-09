with source as (
    select * from {{source('raw', 'fhv_tripdata')}}
),
filtered as (
    select * from source
    where dispatching_base_num is not null
)
select count(1) from filtered