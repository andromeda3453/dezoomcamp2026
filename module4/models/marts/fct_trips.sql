with
    final as (
        select
            trip_id,
            vendor_id,
            pickup_location_id,
            dropoff_location_id,
            ratecode_id,
            pickup_datetime,
            dropoff_datetime,
            passenger_count,
            trip_type,
            trip_distance,
            payment_type,
            fare_amount,
            extra,
            mta_tax
            tip_amount,
            tolls_amount,
            improvement_surcharge,
            ehail_fee,
            total_amount,
            congestion_surcharge,
            taxi_type
        from {{ ref("int_trips_filtered") }}
    )

select *
from final
