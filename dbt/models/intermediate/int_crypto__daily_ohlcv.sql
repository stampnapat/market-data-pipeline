-- Roll hourly candles up to one row per asset per UTC day.
--   open  = open of the first candle, close = close of the last candle
--   high  = highest high,             low   = lowest low,   volume = sum
with candles as (
    select * from {{ ref('stg_binance__klines') }}
)

select
    asset_code,
    quote_currency,
    opened_at::date                                         as price_date,
    (array_agg(open_price  order by opened_at))[1]          as open_price,
    max(high_price)                                         as high_price,
    min(low_price)                                          as low_price,
    (array_agg(close_price order by opened_at desc))[1]     as close_price,
    sum(base_volume)                                        as volume,
    count(*)                                                as observation_count,
    count(*) = 24                                           as is_complete_day,
    max(loaded_at)                                          as loaded_at
from candles
group by asset_code, quote_currency, opened_at::date
