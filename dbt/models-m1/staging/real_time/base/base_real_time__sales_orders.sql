with

orders as (
    select * from {{ source('raw_ext', 'orders_raw') }}
),

order_details as (
    select * from {{ source('raw_ext', 'order_details_raw') }}
),

orders_with_details as (
    select
        o.sales_order_id::string                    as sales_order_id,
        o.customer_id::string                       as customer_id,
        o.account_number::string                    as account_number,
        o.bill_to_address_id::number                as bill_to_address_id,
        o.comment::string                           as comment,
        o.credit_card_approval_code::string         as credit_card_approval_code,
        o.credit_card_id::number                    as credit_card_id,
        o.currency_rate_id::number                  as currency_rate_id,
        null::string                                as delivery_estimate,
        o.due_date::timestamp                       as due_date,
        o.freight::float                            as freight,
        o.last_modified::date                       as modified_date,
        o.online_order_flag::integer                as online_order_flag,
        o.order_date::timestamp                     as order_date,
        array_agg(object_construct(
            'SalesOrderDetailID', od.sales_order_detail_id,
            'CarrierTrackingNumber', od.carrier_tracking_number,
            'OrderQty', od.order_qty,
            'ProductID', od.product_id,
            'SpecialOfferID', od.special_offer_id,
            'UnitPrice', od.unit_price,
            'UnitPriceDiscount', od.unit_price_discount,
            'LineTotal', od.line_total,
            'ModifiedDate', od.last_modified
        ))                                          as order_details,
        o.purchase_order_number::string             as purchase_order_number,
        o.revision_number::number                   as revision_number,
        o.sales_order_number::string                as sales_order_number,
        o.sales_person_id::number                   as sales_person_id,
        o.ship_date::timestamp                      as ship_date,
        o.ship_method_id::number                    as ship_method_id,
        o.ship_to_address_id::number                as ship_to_address_id,
        o.status::number                            as status,
        o.sub_total::float                          as sub_total,
        o.tax_amt::float                            as tax_amt,
        o.territory_id::number                      as territory_id,
        o.total_due::float                          as total_due
    from orders o
    left join order_details od
        on o.sales_order_id = od.sales_order_id
    group by all
)

select * from orders_with_details
