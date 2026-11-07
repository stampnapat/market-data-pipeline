-- One row per spot gold daily bar (USD per troy ounce).
with source as (
    select * from {{ source('ingested', 'gold_spot_prices') }}
)

select
    symbol,
    replace(symbol, 'USD', '')  as asset_code,   -- XAUUSD -> XAU
    price_date,
    open                        as open_price,
    high                        as high_price,
    low                         as low_price,
    close                       as close_price,
    _loaded_at                  as loaded_at
from source
