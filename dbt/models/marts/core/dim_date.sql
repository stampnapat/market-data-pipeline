-- Calendar dimension: one row per day. Range set by vars in dbt_project.yml.
with days as (
    select generate_series(
        '{{ var("dim_date_start") }}'::date,
        '{{ var("dim_date_end") }}'::date,
        interval '1 day'
    )::date as date_day
)

select
    to_char(date_day, 'YYYYMMDD')::int          as date_key,
    date_day,
    extract(year from date_day)::int            as year,
    extract(quarter from date_day)::int         as quarter,
    extract(month from date_day)::int           as month,
    to_char(date_day, 'Mon')                    as month_name,
    extract(day from date_day)::int             as day_of_month,
    extract(isodow from date_day)::int          as day_of_week,   -- 1 = Monday
    to_char(date_day, 'Dy')                     as day_name,
    extract(week from date_day)::int            as iso_week,
    extract(isodow from date_day) in (6, 7)     as is_weekend
from days
