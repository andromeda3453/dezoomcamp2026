select
    -- IDs
    cast(vendorid as int) as vendor_id,
    cast(pulocationid as int) as pickup_location_id,
    cast(dolocationid as int) as dropoff_location_id,
    cast(ratecodeid as int) as ratecode_id,
    -- timestamps
    cast(tpep_pickup_datetime as timestamp) as pickup_datetime,
    cast(tpep_dropoff_datetime as timestamp) as dropoff_datetime,
    -- trip info
    store_and_fwd_flag,
    cast(passenger_count as int) as passenger_count,
    1 as trip_type, -- yellow taxis can only be street-hail (trip_type=1). this column doesnt exist in the yellow taxi dataset. we have to add it to match the columns of the green dataset
    trip_distance,
    -- payment info
    cast(payment_type as int) as payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    0 as ehail_fee, -- yellow taxis cant have an ehail fee
    total_amount,
    congestion_surcharge,
-- got rid of airport fee column for the sake of matching columns
from {{ source("raw_data", "yellowtrip_data") }}
where vendorid is not null -- data quality req