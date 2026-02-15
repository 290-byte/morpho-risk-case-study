#!/usr/bin/env python3
"""
Block 8: Plume sdeUSD/pUSD Deep Dive
=====================================
Answers: Why did $4.9M in borrows disappear from the Plume sdeUSD/pUSD market
between Nov 3-5, 2025? Was it voluntary repayment or liquidation?
Also fetches the Ethereum sdeUSD/USDC market for comparison (oracle contrast).

Outputs:
  block8_plume_transactions.csv       - All market events (repay, liquidate, supply, withdraw)
  block8_plume_market_history.csv     - Hourly supply/borrow/collateral/utilization
  block8_plume_borrower_positions.csv - Current borrower snapshot
  block8_eth_transactions.csv         - Ethereum sdeUSD/USDC transactions (comparison)
  block8_eth_market_history.csv       - Ethereum sdeUSD/USDC hourly history
  block8_oracle_comparison.csv        - Oracle config side-by-side

Requires: requests (pip install requests)
"""

import requests
import time
import csv
import json
import os
import sys
from datetime import datetime, timezone

# ─── Config ──────────────────────────────────────────────────────
API_URL = "https://blue-api.morpho.org/graphql"  # same as all other block scripts
DATA_DIR = os.environ.get("DATA_DIR", os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data"))
os.makedirs(DATA_DIR, exist_ok=True)

# Markets
PLUME_SDEUSD_PUSD = {
    "key": "0x8d009383866dffaac5fe25af684e93f8dd5a98fed1991c298624ecc3a860f39f",
    "chain_id": 98866,
    "label": "sdeUSD/pUSD(91.5%) Plume",
}
ETH_SDEUSD_USDC = {
    "key": "0x0f9563442d64ab3bd3bcb27058db0b0d4046a4c46f0acd811dacae9551d2b129",
    "chain_id": 1,
    "label": "sdeUSD/USDC(91.5%) Ethereum",
}

# Time window: Oct 15 - Nov 20, 2025 (captures pre-depeg + aftermath)
TS_START = 1760486400   # Oct 15 2025
TS_END   = 1763596800   # Nov 20 2025
TS_DEPEG = 1762214400   # Nov 4  2025

DELAY = 0.3  # seconds between API calls


# ─── GraphQL helpers ─────────────────────────────────────────────

def query_graphql(query: str, variables: dict = None, retries: int = 3) -> dict:
    """Send GraphQL query with retry."""
    payload = {"query": query}
    if variables:
        payload["variables"] = variables

    for attempt in range(retries):
        try:
            resp = requests.post(API_URL, json=payload, timeout=60)
            if resp.status_code == 400:
                print(f"  [ERR] 400 Bad Request. Response body:")
                print(f"  {resp.text[:500]}")
                return {}
            resp.raise_for_status()
            data = resp.json()
            if "errors" in data:
                errs = data["errors"]
                print(f"  [WARN] GraphQL errors: {json.dumps(errs[:2], indent=2)}")
                # Some errors are partial (data still returned)
                if "data" in data and data["data"]:
                    return data["data"]
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)
                    continue
                return {}
            return data.get("data", {})
        except Exception as e:
            print(f"  [ERR] Attempt {attempt+1}: {e}")
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
    return {}


def ts_to_date(ts):
    """Unix timestamp to date string."""
    try:
        return datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime("%Y-%m-%d")
    except:
        return ""

def ts_to_datetime(ts):
    """Unix timestamp to datetime string."""
    try:
        return datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
    except:
        return ""


# ─── Query 1: Transaction history ────────────────────────────────

TRANSACTIONS_QUERY = """
query MarketTransactions($marketKey: [String!]!, $types: [TransactionType!]!,
                          $chainId: [Int!]!, $tsGte: Int, $tsLte: Int,
                          $first: Int!, $skip: Int!) {
  transactions(
    first: $first
    skip: $skip
    orderBy: Timestamp
    orderDirection: Asc
    where: {
      marketUniqueKey_in: $marketKey
      type_in: $types
      chainId_in: $chainId
      timestamp_gte: $tsGte
      timestamp_lte: $tsLte
    }
  ) {
    items {
      hash
      timestamp
      blockNumber
      type
      user { address }
      data {
        ... on MarketTransferTransactionData {
          shares
          assets
          assetsUsd
          market { uniqueKey }
        }
        ... on MarketLiquidationTransactionData {
          seizedAssets
          seizedAssetsUsd
          repaidAssets
          repaidAssetsUsd
          badDebtAssets
          badDebtAssetsUsd
          liquidator
          market { uniqueKey }
        }
        ... on MarketCollateralTransferTransactionData {
          assets
          assetsUsd
          market { uniqueKey }
        }
      }
    }
    pageInfo { countTotal }
  }
}
"""

ALL_MARKET_TX_TYPES = [
    "MarketBorrow",
    "MarketRepay",
    "MarketSupply",
    "MarketWithdraw",
    "MarketLiquidation",
    "MarketSupplyCollateral",
    "MarketWithdrawCollateral",
]


def fetch_transactions(market: dict, output_file: str):
    """Fetch all transaction types for a market, paginated."""
    print(f"\n{'='*70}")
    print(f"QUERY 1: Transactions for {market['label']}")
    print(f"{'='*70}")

    all_rows = []
    page_size = 100
    skip = 0
    total = None

    while True:
        variables = {
            "marketKey": [market["key"]],
            "types": ALL_MARKET_TX_TYPES,
            "chainId": [market["chain_id"]],
            "tsGte": TS_START,
            "tsLte": TS_END,
            "first": page_size,
            "skip": skip,
        }

        data = query_graphql(TRANSACTIONS_QUERY, variables)
        txs = data.get("transactions", {})
        items = txs.get("items", [])

        if total is None:
            total = txs.get("pageInfo", {}).get("countTotal", "?")
            print(f"  Total transactions: {total}")

        if not items:
            break

        for tx in items:
            tx_type = tx.get("type", "")
            ts = int(tx.get("timestamp", 0))
            user = tx.get("user", {}).get("address", "")
            td = tx.get("data", {})

            row = {
                "hash": tx.get("hash", ""),
                "timestamp": ts,
                "date": ts_to_date(ts),
                "datetime": ts_to_datetime(ts),
                "block_number": tx.get("blockNumber", ""),
                "type": tx_type,
                "user_address": user,
                "market_unique_key": market["key"],
            }

            if tx_type == "MarketLiquidation":
                row["assets"] = td.get("repaidAssets", "")
                row["assets_usd"] = td.get("repaidAssetsUsd", "")
                row["seized_assets"] = td.get("seizedAssets", "")
                row["seized_assets_usd"] = td.get("seizedAssetsUsd", "")
                row["bad_debt_assets"] = td.get("badDebtAssets", "")
                row["bad_debt_assets_usd"] = td.get("badDebtAssetsUsd", "")
                row["liquidator"] = td.get("liquidator", "")
            elif tx_type in ("MarketSupplyCollateral", "MarketWithdrawCollateral"):
                row["assets"] = td.get("assets", "")
                row["assets_usd"] = td.get("assetsUsd", "")
            else:
                # MarketBorrow, MarketRepay, MarketSupply, MarketWithdraw
                row["assets"] = td.get("assets", "")
                row["assets_usd"] = td.get("assetsUsd", "")
                row["shares"] = td.get("shares", "")

            all_rows.append(row)

        print(f"  Fetched {skip + len(items)} / {total}")
        skip += page_size
        time.sleep(DELAY)

        if len(items) < page_size:
            break

    # Write CSV
    if all_rows:
        fieldnames = [
            "hash", "timestamp", "date", "datetime", "block_number", "type",
            "user_address", "market_unique_key", "assets", "assets_usd",
            "shares", "seized_assets", "seized_assets_usd",
            "bad_debt_assets", "bad_debt_assets_usd", "liquidator",
        ]
        path = os.path.join(DATA_DIR, output_file)
        with open(path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            w.writeheader()
            w.writerows(all_rows)
        print(f"  Wrote {len(all_rows)} rows to {output_file}")
    else:
        print(f"  No transactions found.")

    return all_rows


# ─── Query 2: Market historical state ────────────────────────────

MARKET_HISTORY_QUERY = """
query MarketHistory($uniqueKey: String!, $chainId: Int!, $options: TimeseriesOptions!) {
  marketByUniqueKey(uniqueKey: $uniqueKey, chainId: $chainId) {
    uniqueKey
    lltv
    oracleAddress
    oracle {
      type
      data {
        ... on MorphoChainlinkOracleV2Data {
          baseFeedOne { address }
          baseFeedTwo { address }
          baseOracleVault { address }
          baseVaultConversionSample
          quoteFeedOne { address }
          quoteFeedTwo { address }
          quoteOracleVault { address }
          quoteVaultConversionSample
          scaleFactor
        }
        ... on MorphoChainlinkOracleData {
          baseFeedOne { address }
          baseFeedTwo { address }
          baseOracleVault { address }
          quoteFeedOne { address }
          quoteFeedTwo { address }
          scaleFactor
          vaultConversionSample
        }
      }
    }
    loanAsset { symbol, decimals, address }
    collateralAsset { symbol, decimals, address }
    historicalState {
      supplyAssets(options: $options) { x y }
      supplyAssetsUsd(options: $options) { x y }
      borrowAssets(options: $options) { x y }
      borrowAssetsUsd(options: $options) { x y }
      collateralAssets(options: $options) { x y }
      collateralAssetsUsd(options: $options) { x y }
      liquidityAssets(options: $options) { x y }
      liquidityAssetsUsd(options: $options) { x y }
      utilization(options: $options) { x y }
    }
  }
}
"""


def fetch_market_history(market: dict, output_file: str):
    """Fetch hourly market state timeseries."""
    print(f"\n{'='*70}")
    print(f"QUERY 2: Market history for {market['label']}")
    print(f"{'='*70}")

    variables = {
        "uniqueKey": market["key"],
        "chainId": market["chain_id"],
        "options": {
            "startTimestamp": TS_START,
            "endTimestamp": TS_END,
            "interval": "HOUR",
        },
    }

    data = query_graphql(MARKET_HISTORY_QUERY, variables)
    mkt = data.get("marketByUniqueKey", {})
    if not mkt:
        print("  No data returned.")
        return {}, []

    hs = mkt.get("historicalState", {})

    # Extract oracle config for later
    oracle_info = {
        "oracle_type": mkt.get("oracle", {}).get("type", ""),
        "oracle_address": mkt.get("oracleAddress", ""),
        "oracle_data": mkt.get("oracle", {}).get("data", {}),
        "lltv": mkt.get("lltv", ""),
        "collateral_symbol": mkt.get("collateralAsset", {}).get("symbol", ""),
        "collateral_address": mkt.get("collateralAsset", {}).get("address", ""),
        "collateral_decimals": mkt.get("collateralAsset", {}).get("decimals", ""),
        "loan_symbol": mkt.get("loanAsset", {}).get("symbol", ""),
        "loan_address": mkt.get("loanAsset", {}).get("address", ""),
        "loan_decimals": mkt.get("loanAsset", {}).get("decimals", ""),
    }

    # Build timeseries by timestamp
    fields = {
        "supplyAssets": "supply_assets",
        "supplyAssetsUsd": "supply_usd",
        "borrowAssets": "borrow_assets",
        "borrowAssetsUsd": "borrow_usd",
        "collateralAssets": "collateral_assets",
        "collateralAssetsUsd": "collateral_usd",
        "liquidityAssets": "liquidity_assets",
        "liquidityAssetsUsd": "liquidity_usd",
        "utilization": "utilization",
    }

    by_ts = {}
    for gql_field, csv_col in fields.items():
        points = hs.get(gql_field, [])
        for pt in points:
            ts = pt.get("x", 0)
            val = pt.get("y", 0)
            if ts not in by_ts:
                by_ts[ts] = {"timestamp": ts, "date": ts_to_date(ts), "datetime": ts_to_datetime(ts)}
            by_ts[ts][csv_col] = val

    rows = sorted(by_ts.values(), key=lambda r: r["timestamp"])
    print(f"  Got {len(rows)} hourly snapshots")

    if rows:
        fieldnames = ["timestamp", "date", "datetime", "supply_assets", "supply_usd",
                       "borrow_assets", "borrow_usd", "collateral_assets", "collateral_usd",
                       "liquidity_assets", "liquidity_usd", "utilization"]
        path = os.path.join(DATA_DIR, output_file)
        with open(path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            w.writeheader()
            w.writerows(rows)
        print(f"  Wrote {len(rows)} rows to {output_file}")

        # Quick summary around depeg
        print(f"\n  Snapshot around depeg (Nov 3-5):")
        for r in rows:
            if r["date"] in ("2025-11-03", "2025-11-04", "2025-11-05") and r["datetime"].endswith("00:00"):
                u = float(r.get("utilization", 0))
                s = float(r.get("supply_usd", 0))
                b = float(r.get("borrow_usd", 0))
                liq = float(r.get("liquidity_usd", 0))
                print(f"    {r['datetime']}: supply=${s:,.0f}  borrow=${b:,.0f}  liq=${liq:,.0f}  util={u:.2%}")

    return oracle_info, rows


# ─── Query 3: Borrower positions ─────────────────────────────────

POSITIONS_QUERY = """
query Positions($marketKey: [String!]!, $first: Int!, $skip: Int!) {
  marketPositions(
    first: $first
    skip: $skip
    orderBy: BorrowShares
    orderDirection: Desc
    where: {
      marketUniqueKey_in: $marketKey
    }
  ) {
    items {
      user { address }
      market {
        uniqueKey
        loanAsset { symbol decimals }
        collateralAsset { symbol decimals }
      }
      state {
        supplyShares
        supplyAssets
        supplyAssetsUsd
        borrowShares
        borrowAssets
        borrowAssetsUsd
        collateral
        collateralUsd
      }
    }
    pageInfo { countTotal }
  }
}
"""


def fetch_borrower_positions(market: dict, output_file: str):
    """Fetch current borrower positions for a market."""
    print(f"\n{'='*70}")
    print(f"QUERY 3: Borrower positions for {market['label']}")
    print(f"{'='*70}")

    all_rows = []
    skip = 0
    page_size = 100

    while True:
        variables = {
            "marketKey": [market["key"]],
            "first": page_size,
            "skip": skip,
        }
        data = query_graphql(POSITIONS_QUERY, variables)
        items = data.get("marketPositions", {}).get("items", [])
        total = data.get("marketPositions", {}).get("pageInfo", {}).get("countTotal", "?")

        if skip == 0:
            print(f"  Total positions: {total}")

        if not items:
            break

        for pos in items:
            st = pos.get("state", {})
            user = pos.get("user", {}).get("address", "")
            mkt_info = pos.get("market", {})
            loan_dec = int(mkt_info.get("loanAsset", {}).get("decimals", 6))
            coll_dec = int(mkt_info.get("collateralAsset", {}).get("decimals", 18))

            borrow_raw = int(st.get("borrowAssets", 0) or 0)
            collateral_raw = int(st.get("collateral", 0) or 0)
            supply_raw = int(st.get("supplyAssets", 0) or 0)

            row = {
                "user_address": user,
                "market_unique_key": market["key"],
                "chain_id": market["chain_id"],
                "supply_shares": st.get("supplyShares", 0),
                "supply_assets": supply_raw,
                "supply_assets_human": supply_raw / (10 ** loan_dec) if supply_raw else 0,
                "supply_assets_usd": st.get("supplyAssetsUsd", 0),
                "borrow_shares": st.get("borrowShares", 0),
                "borrow_assets": borrow_raw,
                "borrow_assets_human": borrow_raw / (10 ** loan_dec) if borrow_raw else 0,
                "borrow_assets_usd": st.get("borrowAssetsUsd", 0),
                "collateral": collateral_raw,
                "collateral_human": collateral_raw / (10 ** coll_dec) if collateral_raw else 0,
                "collateral_usd": st.get("collateralUsd", 0),
            }
            # Only include non-zero positions
            if borrow_raw > 0 or collateral_raw > 0 or supply_raw > 0:
                all_rows.append(row)

        skip += page_size
        time.sleep(DELAY)
        if len(items) < page_size:
            break

    if all_rows:
        fieldnames = [
            "user_address", "market_unique_key", "chain_id",
            "supply_shares", "supply_assets", "supply_assets_human", "supply_assets_usd",
            "borrow_shares", "borrow_assets", "borrow_assets_human", "borrow_assets_usd",
            "collateral", "collateral_human", "collateral_usd",
        ]
        path = os.path.join(DATA_DIR, output_file)
        with open(path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            w.writeheader()
            w.writerows(all_rows)
        print(f"  Wrote {len(all_rows)} positions to {output_file}")

        # Print borrower summary
        for r in sorted(all_rows, key=lambda x: -float(x.get("borrow_assets_human", 0))):
            ba = float(r.get("borrow_assets_human", 0))
            ch = float(r.get("collateral_human", 0))
            if ba > 1 or ch > 1:
                print(f"    {r['user_address'][:10]}...  borrow={ba:,.2f}  collateral={ch:,.2f}")
    else:
        print("  No active positions found.")

    return all_rows


# ─── Query 4: Oracle comparison ──────────────────────────────────

def write_oracle_comparison(plume_oracle: dict, eth_oracle: dict):
    """Write oracle config comparison CSV."""
    print(f"\n{'='*70}")
    print(f"QUERY 4: Oracle comparison")
    print(f"{'='*70}")

    def flatten_oracle(oracle_info: dict, label: str) -> dict:
        od = oracle_info.get("oracle_data", {}) or {}
        return {
            "market": label,
            "oracle_type": oracle_info.get("oracle_type", ""),
            "oracle_address": oracle_info.get("oracle_address", ""),
            "lltv": oracle_info.get("lltv", ""),
            "collateral": oracle_info.get("collateral_symbol", ""),
            "collateral_address": oracle_info.get("collateral_address", ""),
            "loan": oracle_info.get("loan_symbol", ""),
            "loan_address": oracle_info.get("loan_address", ""),
            "base_feed_one": (od.get("baseFeedOne") or {}).get("address", ""),
            "base_feed_two": (od.get("baseFeedTwo") or {}).get("address", ""),
            "base_oracle_vault": (od.get("baseOracleVault") or {}).get("address", ""),
            "base_vault_conversion_sample": od.get("baseVaultConversionSample", od.get("vaultConversionSample", "")),
            "quote_feed_one": (od.get("quoteFeedOne") or {}).get("address", ""),
            "quote_feed_two": (od.get("quoteFeedTwo") or {}).get("address", ""),
            "quote_oracle_vault": (od.get("quoteOracleVault") or {}).get("address", ""),
            "quote_vault_conversion_sample": od.get("quoteVaultConversionSample", ""),
            "scale_factor": od.get("scaleFactor", ""),
        }

    rows = [
        flatten_oracle(plume_oracle, "sdeUSD/pUSD Plume"),
        flatten_oracle(eth_oracle, "sdeUSD/USDC Ethereum"),
    ]

    fieldnames = list(rows[0].keys())
    path = os.path.join(DATA_DIR, "block8_oracle_comparison.csv")
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)
    print(f"  Wrote oracle comparison to block8_oracle_comparison.csv")

    # Print key difference
    for r in rows:
        vault = r.get("base_oracle_vault", "")
        is_vault = bool(vault and vault != "0x0000000000000000000000000000000000000000")
        print(f"\n  {r['market']}:")
        print(f"    Oracle type: {r['oracle_type']}")
        print(f"    Uses vault-based pricing: {is_vault}")
        if is_vault:
            print(f"    Base vault: {vault}")
            print(f"    -> Oracle tracks sdeUSD internal exchange rate")
            print(f"    -> Depeg VISIBLE to liquidation engine")
        else:
            bfo = r.get("base_feed_one", "")
            print(f"    Base feed: {bfo}")
            print(f"    -> If hardcoded, depeg INVISIBLE to liquidation engine")


# ─── Main ────────────────────────────────────────────────────────

def main():
    print("=" * 70)
    print("BLOCK 8: PLUME sdeUSD/pUSD DEEP DIVE")
    print(f"Time window: {ts_to_date(TS_START)} to {ts_to_date(TS_END)}")
    print("=" * 70)

    # ── 1. Plume transactions ──
    plume_txs = fetch_transactions(PLUME_SDEUSD_PUSD, "block8_plume_transactions.csv")

    # Quick analysis
    if plume_txs:
        print(f"\n  Transaction breakdown:")
        by_type = {}
        for tx in plume_txs:
            t = tx["type"]
            by_type[t] = by_type.get(t, 0) + 1
        for t, c in sorted(by_type.items()):
            print(f"    {t}: {c}")

        # Nov 3-5 detail
        print(f"\n  Nov 3-5 events (the withdrawal window):")
        for tx in plume_txs:
            if tx["date"] in ("2025-11-03", "2025-11-04", "2025-11-05"):
                usd = float(tx.get("assets_usd", 0) or 0)
                if usd > 10 or tx["type"] == "MarketLiquidation":
                    seized = float(tx.get("seized_assets_usd", 0) or 0)
                    bd = float(tx.get("bad_debt_assets_usd", 0) or 0)
                    extra = ""
                    if tx["type"] == "MarketLiquidation":
                        extra = f" seized=${seized:,.0f} bad_debt=${bd:,.0f}"
                    print(f"    {tx['datetime']}  {tx['type']:<25} ${usd:>12,.2f}  {tx['user_address'][:12]}...{extra}")
    time.sleep(DELAY)

    # ── 2. Plume market history ──
    plume_oracle, _ = fetch_market_history(PLUME_SDEUSD_PUSD, "block8_plume_market_history.csv")
    time.sleep(DELAY)

    # ── 3. Plume borrower positions ──
    fetch_borrower_positions(PLUME_SDEUSD_PUSD, "block8_plume_borrower_positions.csv")
    time.sleep(DELAY)

    # ── 4. Ethereum comparison ──
    eth_txs = fetch_transactions(ETH_SDEUSD_USDC, "block8_eth_transactions.csv")
    if eth_txs:
        print(f"\n  Ethereum transaction breakdown:")
        by_type = {}
        for tx in eth_txs:
            t = tx["type"]
            by_type[t] = by_type.get(t, 0) + 1
        for t, c in sorted(by_type.items()):
            print(f"    {t}: {c}")
    time.sleep(DELAY)

    eth_oracle, _ = fetch_market_history(ETH_SDEUSD_USDC, "block8_eth_market_history.csv")
    time.sleep(DELAY)

    # ── 5. Oracle comparison ──
    if plume_oracle or eth_oracle:
        write_oracle_comparison(plume_oracle, eth_oracle)

    # ── Summary ──
    print(f"\n{'='*70}")
    print("BLOCK 8 COMPLETE")
    print(f"{'='*70}")
    print(f"Files written to: {DATA_DIR}/")
    for f in sorted(os.listdir(DATA_DIR)):
        if f.startswith("block8_"):
            size = os.path.getsize(os.path.join(DATA_DIR, f))
            print(f"  {f} ({size:,} bytes)")

    print(f"\nKey question: check block8_plume_transactions.csv for Nov 3-5.")
    print(f"If you see MarketRepay events -> borrower voluntarily repaid.")
    print(f"If you see MarketLiquidation events -> oracle triggered liquidation.")
    print(f"Compare with block8_eth_transactions.csv to see if Ethereum had any.")


if __name__ == "__main__":
    main()
