-- Keep the last announcement of each Bangkok-local day per product
-- (the "closing" Thai gold price of that day).
with prices as (
    select
        *,
        row_number() over (
            partition by symbol, announced_date_bkk
            order by announced_at desc
        ) as rn
    from {{ ref('stg_gta__thai_gold_prices') }}
)

select
    symbol                as asset_code,
    announced_date_bkk    as price_date,
    announced_at,
    announcement_no,
    buy_price_thb,
    sell_price_thb
from prices
where rn = 1
