-- One row per spot gold trading day (USD per troy ounce).
-- Twelve Data also emits Saturday/Sunday bars while the market is closed (flat
-- placeholders). They would add fake ~0% returns to volatility and correlation,
-- so weekend bars stop here; raw and the staging table keep them as delivered.
with source as (
    select * from {{ source('ingested', 'gold_spot_prices') }}
    where extract(isodow from price_date) < 6
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
