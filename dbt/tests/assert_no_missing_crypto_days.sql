-- Every crypto asset must have a row for every calendar day between its first and last day.
-- Build the full calendar first and LEFT JOIN onto it: grouping only existing rows
-- can never reveal a day that is missing entirely (we hit exactly this in episode 11).
with bounds as (
    select asset_id, min(price_date) as first_day, max(price_date) as last_day
    from {{ ref('fact_price') }} f
    join {{ ref('dim_asset') }} a using (asset_id)
    where a.asset_class = 'crypto'
    group by asset_id
),

expected as (
    select b.asset_id, d.date_day
    from bounds b
    join {{ ref('dim_date') }} d on d.date_day between b.first_day and b.last_day
)

select e.asset_id, e.date_day as missing_day
from expected e
left join {{ ref('fact_price') }} f
    on f.asset_id = e.asset_id and f.price_date = e.date_day
where f.asset_id is null
