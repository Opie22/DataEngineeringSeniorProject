with

source as (

    select * from {{ source('raw_ext', 'web_analytics_raw') }}

),

cleaned as (

    select
        customer_id::varchar        as customer_id,
        product_id::int             as product_id,
        session_id::varchar         as session_id,
        page_url::varchar           as page_url,
        event_type::varchar         as event_type,
        event_timestamp::timestamp_ntz as event_timestamp,
        _loaded_at::timestamp_ntz   as _loaded_at,
        _file_name::varchar         as _file_name,
        current_timestamp()         as dbt_loaded_at

    from source

)

select * from cleaned
