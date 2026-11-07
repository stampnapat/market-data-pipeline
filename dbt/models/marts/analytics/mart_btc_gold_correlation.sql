-- Rolling 30-observation correlation of daily log returns: BTC vs spot gold.
-- Only days on which BOTH have a close are used (gold doesn't trade on weekends),
-- so a Monday return spans the weekend for both assets alike.
with closes as (
    select
        f.price_date,
        max(f.close_price) filter (where a.asset_code = 'BTC') as btc_close,
        max(f.close_price) filter (where a.asset_code = 'XAU') as xau_close
    from {{ ref('fact_price') }} f
    join {{ ref('dim_asset') }} a using (asset_id)
    where a.asset_code in ('BTC', 'XAU')
    group by f.price_date
),

common_days as (
    select * from closes where btc_close is not null and xau_close is not null
),

returns as (
    select
        price_date,
        btc_close,
        xau_close,
        ln(btc_close / lag(btc_close) over (order by price_date)) as btc_log_return,
        ln(xau_close / lag(xau_close) over (order by price_date)) as xau_log_return
    from common_days
),

rolling as (
    select
        *,
        corr(btc_log_return, xau_log_return) over w30  as corr_30,
        count(btc_log_return)                over w30  as n_30
    from returns
    window w30 as (order by price_date rows between 29 preceding and current row)
)

select
    price_date,
    btc_close,
    xau_close,
    round(100 * btc_log_return, 4)                        as btc_return_pct,
    round(100 * xau_log_return, 4)                        as xau_return_pct,
    case when n_30 = 30 then round(corr_30::numeric, 4) end as correlation_30
from rolling
