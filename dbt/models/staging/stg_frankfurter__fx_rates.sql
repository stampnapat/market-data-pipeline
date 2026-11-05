-- One row per currency pair per ECB business day.
with source as (
    select * from {{ source('ingested', 'fx_rates') }}
)

select
    symbol,
    base_currency,
    quote_currency,
    rate_date,
    rate,
    _loaded_at as loaded_at
from source
