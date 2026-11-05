-- Daily returns and volatility per asset.
--   daily_return_pct   : close-to-close change vs the previous observation
--   intraday_range_pct : (high - low) / open, how far price swung within the day
--   volatility_30_pct  : standard deviation of daily log returns over 30 observations
--   annualized_vol_pct : volatility_30 * sqrt(observations per year)
--                        (crypto trades 365 days, FX/gold ~252 business days)
with prices as (
    select
        f.asset_id,
        a.asset_code,
        a.asset_class,
        f.price_date,
        f.open_price,
        f.high_price,
        f.low_price,
        f.close_price,
        lag(f.close_price) over (partition by f.asset_id order by f.price_date) as prev_close
    from {{ ref('fact_price') }} f
    join {{ ref('dim_asset') }} a using (asset_id)
),

returns as (
    select
        *,
        close_price / prev_close - 1   as daily_return,
        ln(close_price / prev_close)   as log_return
    from prices
),

rolling as (
    select
        *,
        stddev_samp(log_return) over w30  as vol_30,
        count(log_return)       over w30  as n_30
    from returns
    window w30 as (partition by asset_id order by price_date rows between 29 preceding and current row)
)

select
    asset_id,
    asset_code,
    asset_class,
    price_date,
    close_price,
    round(100 * daily_return, 4)                                   as daily_return_pct,
    round(100 * (high_price - low_price) / nullif(open_price, 0), 4) as intraday_range_pct,
    case when n_30 = 30 then round(100 * vol_30, 4) end            as volatility_30_pct,
    case when n_30 = 30 then round(
        100 * vol_30 * sqrt(case when asset_class = 'crypto' then 365 else 252 end)::numeric, 2
    ) end                                                          as annualized_vol_pct
from rolling
