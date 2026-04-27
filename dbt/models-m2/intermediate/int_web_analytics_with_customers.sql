with

web_analytics as (

    select * from {{ ref('stg_web_analytics') }}

),

customers as (

    select * from {{ ref('stg_adventure_db__customers') }}

),

joined as (

    select
        -- Event identifiers
        wa.session_id,
        wa.event_timestamp,
        wa.event_type,
        wa.page_url,

        -- Product
        wa.product_id,

        -- Customer identity
        wa.customer_id,
        c.full_name,
        c.first_name,
        c.last_name,
        c.email_address,

        -- Customer geography (useful for slicing browsing behavior by region)
        c.city,
        c.state_province,
        c.country_region,
        c.postal_code,

        -- Metadata
        wa._loaded_at,
        wa.dbt_loaded_at

    from web_analytics wa
    left join customers c
        on wa.customer_id = c.customer_id::varchar

)

select * from joined
