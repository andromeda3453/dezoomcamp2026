{{ config(materialized="ephemeral") }}

with
    filtered as (
        select
            * except (
                fare_amount,
                extra,
                mta_tax,
                tip_amount,
                tolls_amount,
                improvement_surcharge,
                ehail_fee,
                total_amount,
                congestion_surcharge
            ),
            abs(fare_amount) as fare_amount,
            abs(extra) as extra,
            abs(mta_tax) as mta_tax,
            abs(tip_amount) as tip_amount,
            abs(tolls_amount) as tolls_amount,
            abs(improvement_surcharge) as improvement_surcharge,
            abs(ehail_fee) as ehail_fee,
            abs(total_amount) as total_amount,
            abs(congestion_surcharge) as congestion_surcharge

        from {{ ref("int_trips_deduped") }}
    )

select * from filtered
