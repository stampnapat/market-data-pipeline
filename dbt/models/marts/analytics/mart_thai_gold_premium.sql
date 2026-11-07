-- Thai 96.5% gold bar price vs. what the same gold is worth at spot, in THB.
--   implied THB per baht-weight = XAU (USD/oz) * USDTHB * 15.244 g * 0.965 / 31.1034768 g/oz
--   premium = Thai dealer sell price - implied spot value
-- Spot and FX use the latest value on or before the Thai date (no FX/spot on weekends),
-- and only if it is at most 4 days old.
{% set grams_per_baht_weight = 15.244 %}
{% set purity = 0.965 %}
{% set grams_per_troy_ounce = 31.1034768 %}

with thai as (
    select f.price_date, f.bid_price as dealer_buy_thb, f.ask_price as dealer_sell_thb
    from {{ ref('fact_price') }} f
    join {{ ref('dim_asset') }} a using (asset_id)
    where a.asset_code = 'TH_GOLD_BAR_965'
),

priced as (
    select
        t.*,
        spot.price_date     as spot_date,
        spot.close_price    as xau_usd_per_oz,
        fx.price_date       as fx_date,
        fx.close_price      as thb_per_usd
    from thai t
    cross join lateral (
        select f.price_date, f.close_price
        from {{ ref('fact_price') }} f
        join {{ ref('dim_asset') }} a using (asset_id)
        where a.asset_code = 'XAU' and f.price_date <= t.price_date
        order by f.price_date desc
        limit 1
    ) spot
    cross join lateral (
        select f.price_date, f.close_price
        from {{ ref('fact_price') }} f
        join {{ ref('dim_asset') }} a using (asset_id)
        where a.asset_code = 'USDTHB' and f.price_date <= t.price_date
        order by f.price_date desc
        limit 1
    ) fx
    where spot.price_date >= t.price_date - 4
      and fx.price_date >= t.price_date - 4
),

implied as (
    select
        *,
        xau_usd_per_oz * thb_per_usd
            * {{ grams_per_baht_weight }} * {{ purity }} / {{ grams_per_troy_ounce }}
            as spot_value_thb
    from priced
)

select
    price_date,
    dealer_buy_thb,
    dealer_sell_thb,
    xau_usd_per_oz,
    thb_per_usd,
    spot_date,
    fx_date,
    round(spot_value_thb, 2)                                  as spot_value_thb,
    round(dealer_sell_thb - spot_value_thb, 2)                as premium_thb,
    round(100 * (dealer_sell_thb / spot_value_thb - 1), 3)    as premium_pct
from implied
