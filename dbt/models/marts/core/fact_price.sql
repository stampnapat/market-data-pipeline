{{
    config(
        materialized='incremental',
        unique_key=['asset_id', 'date_key'],
        on_schema_change='fail'
    )
}}

-- Daily price fact. Grain: one row per asset per day.
-- Every source is normalised to OHLC (+ optional volume / dealer bid-ask):
--   crypto     : real daily OHLCV rolled up from hourly candles
--   fx         : one ECB fixing per business day -> O = H = L = C = rate
--   spot gold  : daily OHLC bar (USD per troy ounce), no volume
--   Thai gold  : day's last announcement; close = dealer sell price (what a buyer pays)
-- Incremental: each run re-processes every (asset, day) whose source rows were loaded
-- after the previous build, however old the day is (a backfill of last quarter is
-- picked up), plus a short lookback by date. Rows are replaced by delete + insert
-- on the unique key.

with crypto as (
    select
        asset_code, price_date, open_price, high_price, low_price, close_price,
        volume, null::numeric as bid_price, null::numeric as ask_price,
        observation_count, is_complete_day, loaded_at
    from {{ ref('int_crypto__daily_ohlcv') }}
),

fx as (
    select
        symbol as asset_code, rate_date as price_date,
        rate as open_price, rate as high_price, rate as low_price, rate as close_price,
        null::numeric as volume, null::numeric as bid_price, null::numeric as ask_price,
        1 as observation_count, true as is_complete_day, loaded_at
    from {{ ref('stg_frankfurter__fx_rates') }}
),

gold_spot as (
    select
        asset_code, price_date, open_price, high_price, low_price, close_price,
        null::numeric as volume, null::numeric as bid_price, null::numeric as ask_price,
        1 as observation_count, true as is_complete_day, loaded_at
    from {{ ref('stg_twelvedata__gold_spot') }}
),

thai_gold as (
    select
        asset_code, price_date,
        sell_price_thb as open_price, sell_price_thb as high_price,
        sell_price_thb as low_price, sell_price_thb as close_price,
        null::numeric as volume, buy_price_thb as bid_price, sell_price_thb as ask_price,
        1 as observation_count, true as is_complete_day, loaded_at
    from {{ ref('int_thai_gold__daily_close') }}
),

unioned as (
    select * from crypto
    union all
    select * from fx
    union all
    select * from gold_spot
    union all
    select * from thai_gold
)

select
    d.date_key,
    a.asset_id,
    u.price_date,
    u.open_price,
    u.high_price,
    u.low_price,
    u.close_price,
    u.volume,
    u.bid_price,
    u.ask_price,
    u.observation_count,
    u.is_complete_day,
    u.loaded_at  -- latest load of the source rows behind this value (drives incremental)
from unioned u
join {{ ref('dim_asset') }} a on a.asset_code = u.asset_code
join {{ ref('dim_date') }}  d on d.date_day = u.price_date
{% if is_incremental() %}
where u.loaded_at > (select max(loaded_at) from {{ this }})
   or u.price_date >= (select max(price_date) - {{ var('fact_lookback_days') }} from {{ this }})
{% endif %}
