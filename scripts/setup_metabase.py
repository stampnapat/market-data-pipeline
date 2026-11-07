"""Create (or update) the "Market Overview" Metabase dashboard from code.

Dashboard as code: every question is native SQL over the `marts` schema, so the
dashboard can be rebuilt identically on any machine. Re-running updates the same
cards and dashboard in place (matched by name) instead of creating duplicates.

Prerequisites: Metabase setup done once in the browser, with the warehouse added as a
database named "Warehouse" (user metabase_reader). Then add to .env:
    METABASE_ADMIN_EMAIL=...
    METABASE_ADMIN_PASSWORD=...

Run:
    set -a; source .env; set +a
    python scripts/setup_metabase.py
"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass, field
from typing import Any

import requests

METABASE_URL = os.environ.get("METABASE_URL", "http://localhost:3000")
DATABASE_NAME = "Warehouse"
COLLECTION_NAME = "Market Data Pipeline"
DASHBOARD_NAME = "Market Overview"

# Validated categorical slots (blue, orange, aqua); see docs/metabase.md
BLUE, ORANGE, AQUA = "#2a78d6", "#eb6834", "#1baf7a"


@dataclass
class Card:
    name: str
    sql: str
    display: str  # "table" | "line" | "scalar" ...
    settings: dict[str, Any] = field(default_factory=dict)
    description: str = ""


def price_with_ma(asset: str, unit: str = "USDT") -> Card:
    return Card(
        name=f"{asset}: daily close with 7/30-day moving averages",
        description=f"{asset} daily close ({unit}) and its 7- and 30-day simple moving averages.",
        sql=f"""
SELECT price_date, close_price, ma_7, ma_30
FROM marts.mart_moving_averages
WHERE asset_code = '{asset}'
ORDER BY price_date""",
        display="line",
        settings={
            "graph.dimensions": ["price_date"],
            "graph.metrics": ["close_price", "ma_7", "ma_30"],
            "graph.y_axis.title_text": unit,
            "graph.y_axis.unpin_from_zero": True,  # price level matters, not distance from 0
            "graph.x_axis.title_text": "Date (UTC)",
            "series_settings": {
                "close_price": {"title": "Close", "color": BLUE},
                "ma_7": {"title": "7-day MA", "color": ORANGE},
                "ma_30": {"title": "30-day MA", "color": AQUA},
            },
        },
    )


CARDS: list[Card] = [
    Card(
        name="Latest snapshot",
        description="Most recent day per asset: price, trend and risk at a glance.",
        sql="""
SELECT DISTINCT ON (m.asset_code)
    m.asset_code                    AS asset,
    m.price_date                    AS as_of,
    m.close_price                   AS close,
    m.ma_7,
    m.ma_30,
    m.trend,
    v.daily_return_pct              AS "daily return %",
    v.annualized_vol_pct            AS "annualized vol %"
FROM marts.mart_moving_averages m
JOIN marts.mart_daily_volatility v USING (asset_id, price_date)
ORDER BY m.asset_code, m.price_date DESC""",
        display="table",
    ),
    price_with_ma("BTC"),
    price_with_ma("ETH"),
    price_with_ma("XAU", unit="USD per troy ounce"),
    Card(
        name="BTC vs gold: 30-day correlation of daily returns",
        description=(
            "Rolling correlation of daily log returns on days both trade. "
            "+1 = move together, 0 = unrelated, -1 = opposite."
        ),
        sql="""
SELECT price_date, correlation_30
FROM marts.mart_btc_gold_correlation
WHERE correlation_30 IS NOT NULL
ORDER BY price_date""",
        display="line",
        settings={
            "graph.dimensions": ["price_date"],
            "graph.metrics": ["correlation_30"],
            "graph.y_axis.title_text": "Correlation",
            "graph.x_axis.title_text": "Date (UTC)",
            # Correlation lives in [-1, 1]: a fixed scale makes its size readable
            "graph.y_axis.auto_range": False,
            "graph.y_axis.min": -1,
            "graph.y_axis.max": 1,
            "series_settings": {"correlation_30": {"title": "30-day correlation", "color": BLUE}},
        },
    ),
    Card(
        name="Annualized volatility (30-day rolling)",
        description=(
            "Std. dev. of daily log returns over 30 observations, annualized. "
            "Same % unit for all assets."
        ),
        sql="""
SELECT price_date, asset_code, annualized_vol_pct
FROM marts.mart_daily_volatility
WHERE annualized_vol_pct IS NOT NULL AND asset_code IN ('BTC', 'ETH', 'XAU')
ORDER BY price_date""",
        display="line",
        settings={
            "graph.dimensions": ["price_date", "asset_code"],
            "graph.metrics": ["annualized_vol_pct"],
            "graph.y_axis.title_text": "Annualized volatility (%)",
            "graph.x_axis.title_text": "Date (UTC)",
            "series_settings": {
                "BTC": {"color": BLUE},
                "ETH": {"color": ORANGE},
                "XAU": {"title": "Gold (XAU)", "color": AQUA},
            },
        },
    ),
    Card(
        name="Daily return: BTC vs ETH",
        description="Close-to-close daily change in percent.",
        sql="""
SELECT price_date, asset_code, daily_return_pct
FROM marts.mart_daily_volatility
WHERE asset_code IN ('BTC', 'ETH') AND daily_return_pct IS NOT NULL
ORDER BY price_date""",
        display="line",
        settings={
            "graph.dimensions": ["price_date", "asset_code"],
            "graph.metrics": ["daily_return_pct"],
            "graph.y_axis.title_text": "Daily return (%)",
            "graph.x_axis.title_text": "Date (UTC)",
            "series_settings": {"BTC": {"color": BLUE}, "ETH": {"color": ORANGE}},
        },
    ),
    Card(
        name="USD/THB reference rate",
        description="ECB reference rate, business days only (THB per 1 USD).",
        sql="""
SELECT f.price_date, f.close_price AS thb_per_usd
FROM marts.fact_price f
JOIN marts.dim_asset a USING (asset_id)
WHERE a.asset_code = 'USDTHB'
ORDER BY f.price_date""",
        display="line",
        settings={
            "graph.dimensions": ["price_date"],
            "graph.metrics": ["thb_per_usd"],
            "graph.y_axis.title_text": "THB per USD",
            "graph.y_axis.unpin_from_zero": True,
            "graph.x_axis.title_text": "Date (UTC)",
            "series_settings": {"thb_per_usd": {"title": "USD/THB", "color": BLUE}},
        },
    ),
    Card(
        name="Thai gold bar 96.5%: dealer buy / sell",
        description=(
            "Last announcement of each Bangkok day, THB per baht-weight. "
            "Collected daily since ingestion began (no history)."
        ),
        sql="""
SELECT f.price_date, f.bid_price AS dealer_buys, f.ask_price AS dealer_sells
FROM marts.fact_price f
JOIN marts.dim_asset a USING (asset_id)
WHERE a.asset_code = 'TH_GOLD_BAR_965'
ORDER BY f.price_date""",
        display="line",
        settings={
            "graph.dimensions": ["price_date"],
            "graph.metrics": ["dealer_buys", "dealer_sells"],
            "graph.y_axis.title_text": "THB per baht-weight",
            "graph.y_axis.unpin_from_zero": True,  # buy/sell differ by ~0.3%: visible only unpinned
            "graph.x_axis.title_text": "Date (Bangkok)",
            "series_settings": {
                "dealer_buys": {"title": "Dealer buys", "color": BLUE},
                "dealer_sells": {"title": "Dealer sells", "color": ORANGE},
            },
        },
    ),
    Card(
        name="Thai gold premium over spot (%)",
        description=(
            "Thai 96.5% bar dealer sell price vs. its spot value in THB "
            "(XAU x USDTHB x 15.244 g x 0.965 / 31.1035 g/oz)."
        ),
        sql="""
SELECT price_date, premium_pct
FROM marts.mart_thai_gold_premium
ORDER BY price_date""",
        display="line",
        settings={
            "graph.dimensions": ["price_date"],
            "graph.metrics": ["premium_pct"],
            "graph.y_axis.title_text": "Premium over spot (%)",
            "graph.x_axis.title_text": "Date (Bangkok)",
            "graph.y_axis.unpin_from_zero": True,
            "series_settings": {"premium_pct": {"title": "Premium %", "color": BLUE}},
        },
    ),
    Card(
        name="Data freshness",
        description=(
            "Last day loaded per asset. A growing 'days behind' means an ingestion DAG stopped."
        ),
        sql="""
SELECT a.asset_code AS asset, a.source_system AS source,
       max(f.price_date) AS last_day,
       current_date - max(f.price_date) AS days_behind,
       count(*) AS days_loaded
FROM marts.dim_asset a
LEFT JOIN marts.fact_price f USING (asset_id)
GROUP BY a.asset_code, a.source_system
ORDER BY a.asset_code""",
        display="table",
    ),
]

# (card name, col, row, width, height) on Metabase's 24-column grid
LAYOUT = [
    ("Latest snapshot", 0, 0, 24, 8),  # tall enough for all 7 asset rows
    ("BTC: daily close with 7/30-day moving averages", 0, 8, 12, 7),
    ("ETH: daily close with 7/30-day moving averages", 12, 8, 12, 7),
    ("XAU: daily close with 7/30-day moving averages", 0, 15, 12, 7),
    ("BTC vs gold: 30-day correlation of daily returns", 12, 15, 12, 7),
    ("Annualized volatility (30-day rolling)", 0, 22, 12, 6),
    ("Daily return: BTC vs ETH", 12, 22, 12, 6),
    ("USD/THB reference rate", 0, 28, 12, 6),
    ("Thai gold bar 96.5%: dealer buy / sell", 12, 28, 12, 6),
    ("Thai gold premium over spot (%)", 0, 34, 24, 6),
    ("Data freshness", 0, 40, 24, 8),
]


class Metabase:
    def __init__(self, url: str, email: str, password: str) -> None:
        self.url = url.rstrip("/")
        self.http = requests.Session()
        token = self._call("POST", "/api/session", json={"username": email, "password": password})
        self.http.headers["X-Metabase-Session"] = token["id"]

    def _call(self, method: str, path: str, **kwargs: Any) -> Any:
        response = self.http.request(method, self.url + path, timeout=30, **kwargs)
        if not response.ok:
            sys.exit(f"{method} {path} failed: {response.status_code} {response.text[:300]}")
        return response.json() if response.content else None

    def get(self, path: str) -> Any:
        return self._call("GET", path)

    def post(self, path: str, body: dict) -> Any:
        return self._call("POST", path, json=body)

    def put(self, path: str, body: dict) -> Any:
        return self._call("PUT", path, json=body)


def find_database_id(mb: Metabase) -> int:
    databases = mb.get("/api/database")
    databases = databases.get("data", databases)  # newer versions wrap the list
    for db in databases:
        if db["name"] == DATABASE_NAME:
            return db["id"]
    sys.exit(f'No Metabase database named "{DATABASE_NAME}". Add it in Admin > Databases first.')


def ensure_collection(mb: Metabase) -> int:
    for collection in mb.get("/api/collection"):
        if collection.get("name") == COLLECTION_NAME and not collection.get("archived"):
            return collection["id"]
    return mb.post("/api/collection", {"name": COLLECTION_NAME, "color": BLUE})["id"]


def existing_items(mb: Metabase, collection_id: int, model: str) -> dict[str, int]:
    items = mb.get(f"/api/collection/{collection_id}/items?models={model}")
    items = items.get("data", items)
    return {item["name"]: item["id"] for item in items}


def upsert_card(
    mb: Metabase, card: Card, database_id: int, collection_id: int, existing: dict
) -> int:
    body = {
        "name": card.name,
        "description": card.description,
        "display": card.display,
        "collection_id": collection_id,
        "visualization_settings": card.settings,
        "dataset_query": {
            "type": "native",
            "native": {"query": card.sql.strip()},
            "database": database_id,
        },
    }
    if card.name in existing:
        mb.put(f"/api/card/{existing[card.name]}", body)
        print(f"  updated card  {card.name}")
        return existing[card.name]
    card_id = mb.post("/api/card", body)["id"]
    print(f"  created card  {card.name}")
    return card_id


def upsert_dashboard(mb: Metabase, collection_id: int, card_ids: dict[str, int]) -> int:
    existing = existing_items(mb, collection_id, "dashboard")
    if DASHBOARD_NAME in existing:
        dashboard_id = existing[DASHBOARD_NAME]
    else:
        dashboard_id = mb.post(
            "/api/dashboard", {"name": DASHBOARD_NAME, "collection_id": collection_id}
        )["id"]
    dashcards = [
        # Negative ids = new dashcards; sending the full list replaces the old layout
        {
            "id": -(i + 1),
            "card_id": card_ids[name],
            "col": col,
            "row": row,
            "size_x": width,
            "size_y": height,
        }
        for i, (name, col, row, width, height) in enumerate(LAYOUT)
    ]
    mb.put(
        f"/api/dashboard/{dashboard_id}",
        {
            "description": "Crypto, FX and Thai gold from the market-data pipeline (marts schema).",
            "dashcards": dashcards,
        },
    )
    return dashboard_id


def main() -> None:
    email = os.environ.get("METABASE_ADMIN_EMAIL")
    password = os.environ.get("METABASE_ADMIN_PASSWORD")
    if not email or not password:
        sys.exit("Set METABASE_ADMIN_EMAIL and METABASE_ADMIN_PASSWORD (see .env.example).")

    mb = Metabase(METABASE_URL, email, password)
    database_id = find_database_id(mb)
    collection_id = ensure_collection(mb)
    existing = existing_items(mb, collection_id, "card")

    print(f'Collection "{COLLECTION_NAME}":')
    card_ids = {c.name: upsert_card(mb, c, database_id, collection_id, existing) for c in CARDS}
    dashboard_id = upsert_dashboard(mb, collection_id, card_ids)
    print(f"Dashboard ready: {METABASE_URL}/dashboard/{dashboard_id}")


if __name__ == "__main__":
    main()
