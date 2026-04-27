with

sales_orders as (
    select * from {{ ref('stg_ecom__sales_orders') }}
),

customers as (
    select * from {{ ref('stg_adventure_db__customers') }}
),

joined as (

    select
        -- Order identifiers
        so.sales_order_id,
        so.sales_order_number,
        so.order_date,
        so.due_date,
        so.ship_date,

        -- Order metrics
        so.status,
        so.online_order_flag,
        so.sub_total,
        so.tax_amt,
        so.freight,
        so.total_due,
        so.delivery_estimate_days,
        so.shipping_method,

        -- Line items (nested variant — used by downstream models)
        so.order_details,

        -- Customer identity
        c.customer_id,
        c.full_name,
        c.first_name,
        c.last_name,
        c.email_address,

        -- Customer location (used for dashboard geo breakdown)
        c.city,
        c.state_province,
        c.country_region,
        c.postal_code,
        c.territory_id

    from sales_orders so
    left join customers c
        on so.customer_id = c.customer_id

)

select * from joined
