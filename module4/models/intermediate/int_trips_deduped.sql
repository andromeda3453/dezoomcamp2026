{{ config(materialized='table') }}

with
    deduped as (

        select
            {{
                dbt_utils.generate_surrogate_key(
                    [
                        "vendor_id",
                        "pickup_location_id",
                        "dropoff_location_id",
                        "pickup_datetime",
                        "dropoff_datetime",
                        "passenger_count",
                        "trip_distance"

                    ]
                )
            }} as trip_id, *
        from {{ ref("int_trips_unioned") }}

    ),
    partitioned as (

        select row_number() over (partition by trip_id) as row_num,
        *
        from deduped
    ),
    final as (

        {{
            dbt_utils.deduplicate(
                relation="deduped",
                partition_by="trip_id",
                order_by="pickup_location_id desc",
            )
        }}
    )

select *
from partitioned where row_num = 2

