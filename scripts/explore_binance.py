"""Explore the Binance klines API: fetch one UTC day of 1h candles and print them."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import requests

BASE_URL = "https://api.binance.com/api/v3/klines"


def to_ms(dt: datetime) -> int:
    """Binance timestamps are Unix time in milliseconds."""
    return int(dt.timestamp() * 1000)


def fetch_day(symbol: str, day: datetime) -> list[list]:
    """Return all 1h candles whose open time falls on `day` (UTC)."""
    start = day.replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=1)
    params = {
        "symbol": symbol,
        "interval": "1h",
        "startTime": to_ms(start),
        "endTime": to_ms(end) - 1,  # endTime is inclusive, so stop 1 ms before midnight
        "limit": 1000,
    }
    response = requests.get(BASE_URL, params=params, timeout=10)
    response.raise_for_status()
    print("Rate-limit weight used this minute:", response.headers.get("x-mbx-used-weight-1m"))
    return response.json()


def main() -> None:
    day = datetime(2026, 11, 1, tzinfo=UTC)
    rows = fetch_day("BTCUSDT", day)

    print(f"Got {len(rows)} candles for {day.date()}")
    print("First raw row:", rows[0])
    print()
    for row in rows[:5]:
        open_time = datetime.fromtimestamp(row[0] / 1000, tz=UTC)
        print(
            f"{open_time:%Y-%m-%d %H:%M}  open={row[1]}  high={row[2]}  "
            f"low={row[3]}  close={row[4]}  volume={row[5]}"
        )


if __name__ == "__main__":
    main()
