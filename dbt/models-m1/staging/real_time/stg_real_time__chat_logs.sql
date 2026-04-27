with

source as (
    select * from {{ source('raw_ext', 'chat_logs_raw') }}
),

parsed as (
    select
        raw:_id::string              as chat_id,
        raw:session_id::string       as session_id,
        raw:customer_id::string      as customer_id,
        raw:agent_id::string         as agent_id,
        raw:message::string          as message,
        raw:channel::string          as channel,
        raw:sentiment::string        as sentiment,
        raw:timestamp::timestamp     as chat_timestamp,
        raw:last_modified::timestamp as last_modified
    from source
)

select * from parsed
