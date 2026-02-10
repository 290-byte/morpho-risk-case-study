"""
Fetch historical DEX prices from GeckoTerminal API.

GeckoTerminal is free, no API key required, and gives actual on-chain
DEX trading prices — perfect for showing oracle vs market reality.

Usage:
    python queries/fetch_dex_prices.py
    python queries/fetch_dex_prices.py --data-dir ./data

Output: dex_prices_hourly.csv
"""

import time
import argparse
import requests
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone

GECKOTERMINAL_BASE = "https://api.geckoterminal.com/api/v2"

# Known DEX pools for our tokens
# Format: (network, pool_address, token_symbol, base_or_quote)
# Find pools at: https://www.geckoterminal.com/
POOLS = [
    # xUSD pools (Ethereum)
    {
        "network": "eth",
        "pool": "0x5e35c9d9a3dc66a5a3243cedaa1478df9547a5f3",  # xUSD/USDC on Curve
        "symbol": "xUSD",
        "chain": "ethereum",
        "note": "Curve xUSD/USDC",
    },
    # deUSD pools (Ethereum)
    {
        "network": "eth",
        "pool": "0x9c08c7a7a722749b3a80e22d83e52a0555b25b03",  # deUSD/USDC
        "symbol": "deUSD",
        "chain": "ethereum",
        "note": "deUSD/USDC",
    },
    # sdeUSD pools (Ethereum) — may be wrapped / vault token
    {
        "network": "eth",
        "pool": "0x12a5deed87a0a48f153030b7a4ad00a29a94e29b",  # sdeUSD pool
        "symbol": "sdeUSD",
        "chain": "ethereum",
        "note": "sdeUSD pool",
    },
]

# Timeframes for OHLCV
# GeckoTerminal supports: day, hour, minute (4h, 12h, 1h, 15m, 5m, 1m)
TIMEFRAMES = {
    "hour": "hour",
    "day": "day",
    "4h": "4hour",
}


def fetch_ohlcv(network: str, pool_address: str, timeframe: str = "hour",
                 aggregate: int = 1, limit: int = 1000,
                 before_timestamp: int = None) -> list:
    """
    Fetch OHLCV candle data from GeckoTerminal.
    
    Returns list of dicts with: datetime, open, high, low, close, volume
    """
    url = f"{GECKOTERMINAL_BASE}/networks/{network}/pools/{pool_address}/ohlcv/{timeframe}"
    params = {
        "aggregate": aggregate,
        "limit": min(limit, 1000),  # API max is 1000
        "currency": "usd",
    }
    if before_timestamp:
        params["before_timestamp"] = before_timestamp

    headers = {"Accept": "application/json;version=20230302"}

    try:
        resp = requests.get(url, params=params, headers=headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        candles = data.get("data", {}).get("attributes", {}).get("ohlcv_list", [])
        rows = []
        for c in candles:
            if len(c) >= 6:
                rows.append({
                    "timestamp": int(c[0]),
                    "datetime": datetime.fromtimestamp(int(c[0]), tz=timezone.utc).isoformat(),
                    "open": float(c[1]),
                    "high": float(c[2]),
                    "low": float(c[3]),
                    "close": float(c[4]),
                    "volume_usd": float(c[5]),
                })
        return rows

    except requests.exceptions.RequestException as e:
        print(f"   ❌ API error: {e}")
        return []


def fetch_pool_info(network: str, pool_address: str) -> dict:
    """Get pool metadata (name, base/quote tokens, etc.)."""
    url = f"{GECKOTERMINAL_BASE}/networks/{network}/pools/{pool_address}"
    headers = {"Accept": "application/json;version=20230302"}

    try:
        resp = requests.get(url, headers=headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        attrs = data.get("data", {}).get("attributes", {})
        return {
            "name": attrs.get("name", ""),
            "base_token_price_usd": attrs.get("base_token_price_usd"),
            "quote_token_price_usd": attrs.get("quote_token_price_usd"),
            "reserve_usd": attrs.get("reserve_in_usd"),
            "volume_24h": attrs.get("volume_usd", {}).get("h24"),
        }
    except Exception as e:
        print(f"   ❌ Pool info error: {e}")
        return {}


def fetch_full_history(network: str, pool_address: str, symbol: str,
                        start_date: str = "2025-10-01",
                        end_date: str = "2025-12-31",
                        timeframe: str = "hour") -> pd.DataFrame:
    """
    Fetch complete hourly price history by paginating backwards.
    GeckoTerminal returns newest-first, so we paginate with before_timestamp.
    """
    start_ts = int(datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())
    end_ts = int(datetime.strptime(end_date, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())

    all_rows = []
    before_ts = end_ts
    page = 0

    while before_ts > start_ts:
        page += 1
        print(f"   Page {page}: fetching before {datetime.fromtimestamp(before_ts, tz=timezone.utc).strftime('%Y-%m-%d %H:%M')}...")

        rows = fetch_ohlcv(network, pool_address, timeframe=timeframe,
                            limit=1000, before_timestamp=before_ts)

        if not rows:
            break

        # Filter to our date range
        rows = [r for r in rows if start_ts <= r["timestamp"] <= end_ts]
        all_rows.extend(rows)

        # Next page: use oldest timestamp from this batch
        oldest = min(r["timestamp"] for r in rows) if rows else before_ts
        if oldest >= before_ts:
            break  # No progress, stop
        before_ts = oldest

        time.sleep(1.0)  # Rate limit: ~30 req/min for free tier

    if not all_rows:
        return pd.DataFrame()

    df = pd.DataFrame(all_rows)
    df["symbol"] = symbol
    df = df.drop_duplicates(subset=["timestamp"]).sort_values("timestamp")
    return df


def main(data_dir: Path = None):
    if data_dir is None:
        data_dir = Path(__file__).parent.parent / "data"
    data_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 70)
    print("DEX Price Fetcher — GeckoTerminal")
    print("=" * 70)

    all_prices = []

    for pool_cfg in POOLS:
        network = pool_cfg["network"]
        pool_addr = pool_cfg["pool"]
        symbol = pool_cfg["symbol"]
        note = pool_cfg["note"]

        print(f"\n{'─' * 50}")
        print(f"🔍 {symbol} — {note}")
        print(f"   Network: {network}  Pool: {pool_addr[:20]}...")

        # Get current pool info
        info = fetch_pool_info(network, pool_addr)
        if info:
            print(f"   Pool: {info.get('name', 'N/A')}")
            print(f"   Base price: ${info.get('base_token_price_usd', 'N/A')}")
            print(f"   Reserves: ${info.get('reserve_usd', 'N/A')}")
        else:
            print(f"   ⚠️  Pool not found or API error — skipping")
            continue

        time.sleep(0.5)

        # Fetch historical prices
        df = fetch_full_history(
            network, pool_addr, symbol,
            start_date="2025-10-01",
            end_date="2025-12-31",
            timeframe="hour",
        )

        if df.empty:
            print(f"   ⚠️  No historical data returned")
            continue

        df["chain"] = pool_cfg["chain"]
        df["pool_address"] = pool_addr
        df["pool_note"] = note
        all_prices.append(df)

        print(f"   ✅ {len(df)} candles fetched ({df['datetime'].min()} → {df['datetime'].max()})")

    if all_prices:
        combined = pd.concat(all_prices, ignore_index=True)
        output_path = data_dir / "dex_prices_hourly.csv"
        combined.to_csv(output_path, index=False)
        print(f"\n✅ Saved {len(combined)} rows to {output_path}")

        # Summary
        for symbol in combined["symbol"].unique():
            subset = combined[combined["symbol"] == symbol]
            print(f"   {symbol}: {len(subset)} candles, "
                  f"price range ${subset['close'].min():.4f} – ${subset['close'].max():.4f}")
    else:
        print("\n⚠️  No price data fetched.")

    print(f"\n{'=' * 70}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", type=Path, default=None)
    args = parser.parse_args()
    main(data_dir=args.data_dir)
