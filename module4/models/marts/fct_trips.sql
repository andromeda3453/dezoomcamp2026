with final as (
    select * from {{ref('int_trips_unioned')}}
)

select * from final where vendor_id is null