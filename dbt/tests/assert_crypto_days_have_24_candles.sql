{{ config(severity='warn') }}
-- A crypto day rolled up from fewer than 24 hourly candles is incomplete.
-- Warn (not error): a partial day can be repaired by re-running that day's ingestion.
select f.asset_id, f.price_date, f.observation_count
from {{ ref('fact_price') }} f
join {{ ref('dim_asset') }} a using (asset_id)
where a.asset_class = 'crypto' and not f.is_complete_day
