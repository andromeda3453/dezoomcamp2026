-- Do all kinds of cosmetic cleanups that you need to do here (renaming columns,
-- changing order of columns,
-- adding aliases,
-- casting to new dtypes)
select

    -- put identifiers at the beginning 
    cast(vendorid as int) as vendor_id,
    cast(pulocationid as int) as pickup_location_id,
    cast(dolocationid as int) as dropoff_location_id,
    cast(ratecodeid as int) as ratecode_id,
    -- timestamps
    cast(lpep_pickup_datetime as timestamp) as pickup_datetime,
    cast(lpep_dropoff_datetime as timestamp) as dropoff_datetime,
    -- trip info
    store_and_fwd_flag,
    cast(passenger_count as int) as passenger_count,
    cast(trip_type as int) as trip_type,
    trip_distance,
    -- payment info
    cast(payment_type as int) as payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    ehail_fee,
    total_amount,
    congestion_surcharge

from {{ source("raw_data", "greentrip_data") }}
where vendorid is not null -- usually done in intermedite tables. staging tables are usually 1 to 1 with source tables(same no of columns/rows)
