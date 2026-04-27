{#
    Custom test: verify that the most recent event in the web analytics
    staging model is no more than 24 hours old. This is a data freshness
    check that works independently of dbt's built-in source freshness.

    The test fails (returns a row) if the most recent event_timestamp
    is more than 24 hours old.
#}

select 1
from (
    select max(event_timestamp) as most_recent
    from {{ ref('stg_web_analytics') }}
) freshness
where datediff('hour', freshness.most_recent, current_timestamp()) > 24
