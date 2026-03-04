with
    trips_unioned as (select * from {{ ref("int_trips_deduped") }}),
    ratecodes as (
        select distinct ratecode_id, {{ get_ratecodes("ratecode_id") }} as ratecode
        from trips_unioned
    )

select *
from ratecodes