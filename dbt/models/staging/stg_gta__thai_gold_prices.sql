-- One row per Thai gold product per captured announcement.
-- Prices are THB per baht-weight (15.244 g) of 96.5% gold.
with source as (
    select * from {{ source('ingested', 'thai_gold_prices') }}
)

select
    symbol,
    case symbol
        when 'TH_GOLD_BAR_965' then 'bar'
        when 'TH_GOLD_ORNAMENT_965' then 'ornament'
    end                                                  as product,
    announced_at,
    (announced_at at time zone 'Asia/Bangkok')::date     as announced_date_bkk,
    announcement_no,
    buy_price                                            as buy_price_thb,
    sell_price                                           as sell_price_thb,
    sell_price - buy_price                               as spread_thb,
    _loaded_at                                           as loaded_at
from source
