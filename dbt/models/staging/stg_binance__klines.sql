-- One row per hourly candle. Renames columns to warehouse conventions and
-- splits the trading pair (BTCUSDT -> asset BTC, quote USDT).
with source as (
    select * from {{ source('ingested', 'binance_klines') }}
)

select
    symbol,
    replace(symbol, 'USDT', '')      as asset_code,
    'USDT'                           as quote_currency,
    open_time                        as opened_at,
    close_time                       as closed_at,
    open                             as open_price,
    high                             as high_price,
    low                              as low_price,
    close                            as close_price,
    volume                           as base_volume,
    quote_volume,
    trade_count,
    _loaded_at                       as loaded_at
from source
