with deduped as (

    select {{dbt_utils.generate_surrogate_key(['vendor_id','pickup_location_id', 'dropoff_location_id', 'pickup_datetime','dropoff_datetime'])}} as trip_id
    from {{ref('int_trips_unioned')}}

)

select * from deduped