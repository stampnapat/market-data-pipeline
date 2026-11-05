-- Completeness check for staging.binance_klines.
-- Lists every (day, symbol) in the range that does NOT have exactly 24 hourly candles.
-- Empty result = no gaps.
--
-- Run:  docker compose exec -T postgres psql -U loader -d warehouse \
--         -v from="'2026-08-07'" -v to="'2026-11-04'" < scripts/check_completeness.sql
--
-- Note: build the full calendar first (generate_series) and LEFT JOIN onto it.
-- Grouping only the rows that exist can never reveal a day that is missing entirely.

WITH days AS (
    SELECT generate_series(:from::date, :to::date, interval '1 day')::date AS day
),
symbols AS (
    SELECT DISTINCT symbol FROM staging.binance_klines
)
SELECT d.day, s.symbol, count(k.open_time) AS candles
FROM days d
CROSS JOIN symbols s
LEFT JOIN staging.binance_klines k
    ON k.symbol = s.symbol
   AND k.open_time >= d.day
   AND k.open_time < d.day + 1
GROUP BY d.day, s.symbol
HAVING count(k.open_time) <> 24
ORDER BY d.day, s.symbol;
