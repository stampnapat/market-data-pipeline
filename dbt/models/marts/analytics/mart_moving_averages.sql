-- 7- and 30-observation simple moving averages of the daily close, per asset.
-- Windows count price observations: crypto trades every day (= calendar days),
-- FX and gold only on business days. A window that isn't full yet returns NULL
-- instead of a misleading average of fewer points.
with prices as (
    select
        f.asset_id,
        a.asset_code,
        a.asset_class,
        f.price_date,
        f.close_price
    from {{ ref('fact_price') }} f
    join {{ ref('dim_asset') }} a using (asset_id)
),

windowed as (
    select
        *,
        avg(close_price) over w7   as avg_7,
        count(*)         over w7   as n_7,
        avg(close_price) over w30  as avg_30,
        count(*)         over w30  as n_30
    from prices
    window
        w7  as (partition by asset_id order by price_date rows between 6 preceding and current row),
        w30 as (partition by asset_id order by price_date rows between 29 preceding and current row)
)

select
    asset_id,
    asset_code,
    asset_class,
    price_date,
    close_price,
    case when n_7  = 7  then round(avg_7, 8)  end   as ma_7,
    case when n_30 = 30 then round(avg_30, 8) end   as ma_30,
    case
        when n_30 < 30 then null
        when avg_7 > avg_30 then 'up'
        else 'down'
    end                                             as trend  -- short MA above long MA = uptrend
from windowed
