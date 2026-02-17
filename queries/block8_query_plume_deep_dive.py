#!/usr/bin/env python3
"""
Block 8: Full Market Deep Dive — All Affected Markets
======================================================
Hourly timeseries, transaction history, borrower positions, and oracle
comparison for EVERY significant market affected by the Nov 2025 depeg.

The xUSD/USDC Plume market (0x82e7) is tagged "unrecognized_loan_asset",
so USD fields return empty. Both tokens are 6-decimal, so raw values
divided by 1e6 = dollar amounts.

Outputs:
  block8_plume_transactions.csv              - sdeUSD/pUSD Plume events
  block8_plume_market_history.csv            - sdeUSD/pUSD Plume hourly
  block8_plume_borrower_positions.csv        - sdeUSD/pUSD Plume positions
  block8_elixir_stream_transactions.csv      - xUSD/USDC Plume (0x82e7) events
  block8_elixir_stream_market_history.csv    - xUSD/USDC Plume (0x82e7) hourly
  block8_elixir_stream_positions.csv         - xUSD/USDC Plume (0x82e7) positions
  block8_eth_transactions.csv                - sdeUSD/USDC Ethereum events
  block8_eth_market_history.csv              - sdeUSD/USDC Ethereum hourly
  block8_arb_transactions.csv                - xUSD/USDC Arbitrum events
  block8_arb_market_history.csv              - xUSD/USDC Arbitrum hourly
  block8_arb_positions.csv                   - xUSD/USDC Arbitrum positions
  block8_eth_deusd_transactions.csv          - deUSD/USDC Ethereum events
  block8_eth_deusd_market_history.csv        - deUSD/USDC Ethereum hourly
  block8_oracle_comparison.csv               - Oracle config side-by-side (all markets)
  block8_vault_tvl_hourly.csv                - All 33 vaults: TVL per hour
  block8_vault_allocation_hourly.csv         - All 33 vaults: per-market allocation per hour

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
PLUME_XUSD_USDC = {
    "key": "0x82e7ab8ccabaac59b5f397507ed031ebf19a9a5b2657c00c93bc2423cd0a890d",
    "chain_id": 98866,
    "label": "xUSD/USDC(86%) Plume [Elixir-Stream private]",
}
ETH_SDEUSD_USDC = {
    "key": "0x0f9563442d64ab3bd3bcb27058db0b0d4046a4c46f0acd811dacae9551d2b129",
    "chain_id": 1,
    "label": "sdeUSD/USDC(91.5%) Ethereum",
}
ARB_XUSD_USDC = {
    "key": "0x9e90aec7d768403dacc9dd0d8320307fda3f980eed4df43e3e52168a1c667709",
    "chain_id": 42161,
    "label": "xUSD/USDC(86%) Arbitrum",
}
ETH_DEUSD_USDC = {
    "key": "0xbd1ad3b968f5f0552dbd8cf1989a62881407c5cccf9e49fb3657c8731caf0c1f",
    "chain_id": 1,
    "label": "deUSD/USDC(86%) Ethereum",
}

# Time window: Sep 24 - Nov 20, 2025 (captures 0x82e7 creation Sep 25 + aftermath)
TS_START = 1758672000   # Sep 24 2025
TS_END   = 1763596800   # Nov 20 2025
TS_DEPEG = 1762214400   # Nov 4  2025

# All vaults with any allocation to affected markets (from block1_vaults_graphql.csv)
VAULTS = [
    # Ethereum (chain_id=1)
    {"address": "0x55555815a5595991C3A0Ff119B59AEF6C8B55555", "chain_id": 1, "name": "Adpend USDC"},
    {"address": "0x94643e86aa5E38DDAc6c7791C1297f4E40cD96c1", "chain_id": 1, "name": "1337 USDC"},
    {"address": "0x1265a81d42d513Df40d0031f8f2e1346954d665a", "chain_id": 1, "name": "MEV Capital Elixir USDC"},
    {"address": "0x38248d715336d4CAd20d3e12E8dc34DA596f9d6c", "chain_id": 1, "name": "RIMU USDC"},
    {"address": "0xBEeFFF209270748ddd194831b3fa287a5386f5bC", "chain_id": 1, "name": "Smokehouse USDC"},
    {"address": "0xc582F04d8a82795aa2Ff9c8bb4c1c889fe7b754e", "chain_id": 1, "name": "Gauntlet USDC Frontier"},
    {"address": "0x777791C4d6DC2CE140D00D2828a7C93503c67777", "chain_id": 1, "name": "Hyperithm USDC Degen"},
    {"address": "0x8eB67A509616cd6A7c1B3c8C21D48FF57df3d458", "chain_id": 1, "name": "Gauntlet USDC Core"},
    {"address": "0x5b56F90340dBAa6a8693DADb141D620f0e154fE6", "chain_id": 1, "name": "Avantgarde USDC Core"},
    {"address": "0x974c8FBf4fd795F66B85B73ebC988A51F1A040a9", "chain_id": 1, "name": "Hakutora USDC"},
    {"address": "0xd63070114470f685b75B74D60EEc7c1113d33a3D", "chain_id": 1, "name": "MEV Capital USDC"},
    {"address": "0xB8C9Ae462626Ec6162645F3E7e3441E7918c89F0", "chain_id": 1, "name": "Bandit USDC"},
    {"address": "0x0404fD1a77756EB029F06b5CDea88B2B2ddC2fEE", "chain_id": 1, "name": "Elixir USDC"},
    {"address": "0xca7c196f00e04A5e1c71B91476d0d58f82499734", "chain_id": 1, "name": "Tanken USDC"},
    {"address": "0x0B6C8ef0DE1Be5ed1B59E6e7a67fB9442FB9E49C", "chain_id": 1, "name": "Duplicated Key"},
    {"address": "0x0F359FD18BDa75e9c49bC027E7da59a4b01BF32a", "chain_id": 1, "name": "Relend USDC"},
    {"address": "0xbdbF0494Af9826ba25bcb810e23C36Bc6D8378fA", "chain_id": 1, "name": "Dream Charity Vault"},
    {"address": "0x889dd190bB6b0662DB3fE0AAc29eEDCa3D2ba22A", "chain_id": 1, "name": "MEV Elixir sUSDS"},
    {"address": "0xD50B9Bbf136D1BD5CD5AC6ed9b3F26c458a6d4A6", "chain_id": 1, "name": "(Deployer)"},
    # Arbitrum (chain_id=42161)
    {"address": "0xa60643c90A542A95026C0F1dbdB0615fF42019Cf", "chain_id": 42161, "name": "MEV Capital USDC"},
    {"address": "0x3014ED70B39be395e1a5Eb8ab4c4b8a5378E6522", "chain_id": 42161, "name": "Not Gauntlet"},
    {"address": "0x64CA76e2525fc6Ab2179300c15e343d73e42f958", "chain_id": 42161, "name": "Clearstar High Yield USDC"},
    {"address": "0x4B6F1C9E5d470b97181786b26da0d0945A7cf027", "chain_id": 42161, "name": "Hyperithm USDC"},
    {"address": "0x76B2406D29F1A2Be4DB638aBBD5c7Cab2eE2D8FE", "chain_id": 42161, "name": "Vaultik USDC"},
    {"address": "0x64A651D825FC70Ebba88f2E1BAD90be9A496C4b9", "chain_id": 42161, "name": "Avantgarde USDC Core Arbitrum"},
    {"address": "0xB34e17733245c7371E87E9DF64E705000B91D169", "chain_id": 42161, "name": "Duplicated Key Arbitrum"},
    {"address": "0xa53Cf822FE93002aEaE16d395CD823Ece161a6AC", "chain_id": 42161, "name": "Clearstar USDC Reactor"},
    {"address": "0x6ed386Ee386199D0E1D9D712Ef2071bE433844F4", "chain_id": 42161, "name": "ABRC"},
    {"address": "0x4C289073eb6679393890b85eEC9D604271c18c28", "chain_id": 42161, "name": "Alpha USDC Core"},
    # Plume (chain_id=98866)
    {"address": "0x3DE7AB60745f57Fdea1205E2379261D3C81C7bA6", "chain_id": 98866, "name": "USDC Test"},
    {"address": "0xBA465FCb8D32B875Aed5ae9e2FB53ea303c2D312", "chain_id": 98866, "name": "Elixir Levered RWA vault"},
    {"address": "0xc0Df5784f28046D11813356919B869dDA5815B16", "chain_id": 98866, "name": "Re7 pUSD"},
    {"address": "0x0b14D0bdAf647c541d3887c5b1A4bd64068fCDA7", "chain_id": 98866, "name": "Mystic MEV Capital pUSD"},
]

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
                u = float(r.get("utilization", 0) or 0)
                s = float(r.get("supply_usd", 0) or 0)
                b = float(r.get("borrow_usd", 0) or 0)
                liq = float(r.get("liquidity_usd", 0) or 0)
                # If USD fields are empty (unpriced market), fall back to raw assets
                if s == 0 and b == 0:
                    s_raw = float(r.get("supply_assets", 0) or 0)
                    b_raw = float(r.get("borrow_assets", 0) or 0)
                    liq_raw = float(r.get("liquidity_assets", 0) or 0)
                    if s_raw > 0:
                        print(f"    {r['datetime']}: supply_raw={s_raw:,.0f}  borrow_raw={b_raw:,.0f}  liq_raw={liq_raw:,.0f}  util={u:.2%}")
                        continue
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

def write_oracle_comparison(plume_oracle: dict, eth_oracle: dict, es_oracle: dict = None):
    """Legacy wrapper - redirects to multi."""
    oracles = [
        (plume_oracle, "sdeUSD/pUSD Plume"),
        (eth_oracle, "sdeUSD/USDC Ethereum"),
    ]
    if es_oracle:
        oracles.append((es_oracle, "xUSD/USDC Plume [Elixir-Stream]"))
    write_oracle_comparison_multi(oracles)


def write_oracle_comparison_multi(oracle_pairs: list):
    """Write oracle config comparison CSV for any number of markets.
    oracle_pairs: list of (oracle_info_dict, label_string)
    """
    print(f"\n{'='*70}")
    print(f"ORACLE COMPARISON")
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

    rows = [flatten_oracle(info, label) for info, label in oracle_pairs if info]

    if not rows:
        print("  No oracle data collected.")
        return

    fieldnames = list(rows[0].keys())
    path = os.path.join(DATA_DIR, "block8_oracle_comparison.csv")
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)
    print(f"  Wrote oracle comparison to block8_oracle_comparison.csv")

    for r in rows:
        vault = r.get("base_oracle_vault", "")
        is_vault = bool(vault and vault != "0x0000000000000000000000000000000000000000")
        print(f"\n  {r['market']}:")
        print(f"    Oracle type: {r['oracle_type']}")
        print(f"    Uses vault-based pricing: {is_vault}")
        if is_vault:
            print(f"    Base vault: {vault}")
            print(f"    -> Depeg VISIBLE to liquidation engine")
        else:
            bfo = r.get("base_feed_one", "")
            print(f"    Base feed: {bfo}")
            print(f"    -> If hardcoded, depeg INVISIBLE to liquidation engine")


# ─── Query 5: Vault TVL + allocation hourly ─────────────────────

VAULT_HISTORY_QUERY = """
query VaultHistory($address: String!, $chainId: Int!, $options: TimeseriesOptions!) {
  vaultByAddress(address: $address, chainId: $chainId) {
    address
    name
    historicalState {
      totalAssetsUsd(options: $options) { x y }
      allocation {
        market {
          uniqueKey
          collateralAsset { symbol }
          loanAsset { symbol }
        }
        supplyAssetsUsd(options: $options) { x y }
      }
    }
  }
}
"""


def fetch_vault_histories():
    """Fetch hourly TVL + per-market allocation for all affected vaults.
    Outputs:
      block8_vault_tvl_hourly.csv          - vault-level TVL per hour
      block8_vault_allocation_hourly.csv   - vault->market allocation per hour
    """
    print(f"\n{'='*70}")
    print("VAULT TVL + ALLOCATION HOURLY")
    print(f"Querying {len(VAULTS)} vaults...")
    print(f"{'='*70}")

    options = {
        "startTimestamp": TS_START,
        "endTimestamp": TS_END,
        "interval": "HOUR",
    }

    tvl_rows = []
    alloc_rows = []
    depeg_summary = []

    for i, vault in enumerate(VAULTS):
        addr = vault["address"]
        chain = vault["chain_id"]
        name = vault["name"]
        print(f"\n  [{i+1}/{len(VAULTS)}] {name} ({addr[:14]}...) chain={chain}")

        try:
            data = query_graphql(VAULT_HISTORY_QUERY, {
                "address": addr,
                "chainId": chain,
                "options": options,
            })

            vdata = data.get("vaultByAddress")
            if not vdata:
                print(f"    WARNING: No data returned")
                continue

            hs = vdata.get("historicalState", {})

            # TVL timeseries
            tvl_points = hs.get("totalAssetsUsd", [])
            tvl_at_depeg = 0
            peak_tvl = 0
            for pt in tvl_points:
                ts = pt["x"]
                val = pt["y"] or 0
                peak_tvl = max(peak_tvl, val)
                dt = datetime.fromtimestamp(ts, tz=timezone.utc)
                tvl_rows.append({
                    "vault_address": addr,
                    "vault_name": name,
                    "chain_id": chain,
                    "timestamp": ts,
                    "datetime": dt.strftime("%Y-%m-%d %H:%M"),
                    "date": dt.strftime("%Y-%m-%d"),
                    "total_assets_usd": val,
                })
                # Closest to Nov 3 00:00 (1762128000)
                if ts <= 1762128000:
                    tvl_at_depeg = val

            print(f"    TVL: {len(tvl_points)} hourly points, peak=${peak_tvl:,.0f}, at_depeg=${tvl_at_depeg:,.0f}")

            # Per-market allocation timeseries
            allocations = hs.get("allocation", [])
            alloc_at_depeg = {}
            for alloc in allocations:
                market = alloc.get("market", {})
                market_key = market.get("uniqueKey", "unknown")
                coll_sym = (market.get("collateralAsset") or {}).get("symbol", "?")
                loan_sym = (market.get("loanAsset") or {}).get("symbol", "?")
                market_label = f"{coll_sym}/{loan_sym}"

                supply_pts = alloc.get("supplyAssetsUsd", [])
                supply_at_depeg = 0
                peak_alloc = 0
                for pt in supply_pts:
                    ts = pt["x"]
                    val = pt["y"] or 0
                    peak_alloc = max(peak_alloc, val)
                    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
                    alloc_rows.append({
                        "vault_address": addr,
                        "vault_name": name,
                        "chain_id": chain,
                        "market_id": market_key,
                        "market_label": market_label,
                        "timestamp": ts,
                        "datetime": dt.strftime("%Y-%m-%d %H:%M"),
                        "date": dt.strftime("%Y-%m-%d"),
                        "supply_assets_usd": val,
                    })
                    if ts <= 1762128000:
                        supply_at_depeg = val

                if peak_alloc > 100:
                    print(f"    -> {market_label:<20} {market_key[:12]}  peak=${peak_alloc:>12,.0f}  depeg=${supply_at_depeg:>12,.0f}")
                    alloc_at_depeg[market_label] = supply_at_depeg

            depeg_summary.append({
                "vault_name": name,
                "vault_address": addr,
                "chain_id": chain,
                "tvl_at_depeg": tvl_at_depeg,
                "peak_tvl": peak_tvl,
                "allocations_at_depeg": alloc_at_depeg,
            })

        except Exception as e:
            print(f"    ERROR: {e}")

        time.sleep(DELAY)

    # Write TVL CSV
    tvl_path = os.path.join(DATA_DIR, "block8_vault_tvl_hourly.csv")
    if tvl_rows:
        fieldnames = ["vault_address", "vault_name", "chain_id", "timestamp", "datetime", "date", "total_assets_usd"]
        with open(tvl_path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            w.writerows(tvl_rows)
        print(f"\n  Wrote {len(tvl_rows)} rows to block8_vault_tvl_hourly.csv")

    # Write allocation CSV (gzipped - 100MB+ uncompressed)
    alloc_path = os.path.join(DATA_DIR, "block8_vault_allocation_hourly.csv.gz")
    if alloc_rows:
        fieldnames = ["vault_address", "vault_name", "chain_id", "market_id", "market_label",
                       "timestamp", "datetime", "date", "supply_assets_usd"]
        import gzip
        with gzip.open(alloc_path, "wt", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            w.writerows(alloc_rows)
        print(f"  Wrote {len(alloc_rows)} rows to block8_vault_allocation_hourly.csv.gz")

    # Print depeg summary
    print(f"\n{'='*70}")
    print("VAULT STATE AT DEPEG (Nov 3 00:00 UTC)")
    print(f"{'='*70}")
    print(f"\n  {'VAULT':<45} {'TVL@DEPEG':>14} {'PEAK TVL':>14}")
    print(f"  {'─'*45} {'─'*14} {'─'*14}")
    total_tvl_depeg = 0
    for s in sorted(depeg_summary, key=lambda x: -x["tvl_at_depeg"]):
        tvl_d = s["tvl_at_depeg"]
        tvl_p = s["peak_tvl"]
        total_tvl_depeg += tvl_d
        if tvl_d > 100 or tvl_p > 100:
            print(f"  {s['vault_name']:<45} ${tvl_d:>12,.0f} ${tvl_p:>12,.0f}")
            for mkt, val in s["allocations_at_depeg"].items():
                if val > 100:
                    print(f"    -> {mkt}: ${val:>12,.0f}")
    print(f"  {'─'*45} {'─'*14}")
    print(f"  {'TOTAL':<45} ${total_tvl_depeg:>12,.0f}")

    return depeg_summary


# ─── Main ────────────────────────────────────────────────────────

def main():
    print("=" * 70)
    print("BLOCK 8: FULL MARKET DEEP DIVE")
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

    # ── 3b. Elixir-Stream xUSD/USDC private market (0x82e7) ──
    # This is the $66M market. API can't price xUSD or this USDC on Plume,
    # so USD fields will be empty — raw token fields are the ground truth.
    # Both xUSD and USDC are 6-decimal tokens on this market.
    print(f"\n{'='*70}")
    print("ELIXIR-STREAM PRIVATE MARKET (0x82e7 xUSD/USDC)")
    print(f"{'='*70}")

    es_txs = fetch_transactions(PLUME_XUSD_USDC, "block8_elixir_stream_transactions.csv")
    if es_txs:
        print(f"\n  Transaction breakdown:")
        by_type = {}
        for tx in es_txs:
            t = tx["type"]
            by_type[t] = by_type.get(t, 0) + 1
        for t, c in sorted(by_type.items()):
            print(f"    {t}: {c}")

        # Decode key events (6 decimal tokens)
        print(f"\n  Key events (>$1000 equivalent, 6-decimal tokens):")
        for tx in es_txs:
            raw = int(tx.get("assets", 0) or 0)
            human = raw / 1e6
            if human > 1000 or tx["type"] == "MarketLiquidation":
                seized = int(tx.get("seized_assets", 0) or 0) / 1e6 if tx.get("seized_assets") else 0
                bd = int(tx.get("bad_debt_assets", 0) or 0) / 1e6 if tx.get("bad_debt_assets") else 0
                extra = ""
                if tx["type"] == "MarketLiquidation":
                    extra = f" seized={seized:,.0f} bad_debt={bd:,.0f}"
                print(f"    {tx['datetime']}  {tx['type']:<30} {human:>14,.0f}  {tx['user_address'][:12]}...{extra}")
    time.sleep(DELAY)

    es_oracle, es_rows = fetch_market_history(PLUME_XUSD_USDC, "block8_elixir_stream_market_history.csv")

    # Decode raw historical values (since USD fields will be empty)
    if es_rows:
        print(f"\n  Decoded daily snapshots (raw assets / 1e6):")
        seen_dates = set()
        for r in es_rows:
            d = r.get("date", "")
            if d in seen_dates or not r.get("datetime", "").endswith("00:00"):
                continue
            seen_dates.add(d)
            s = int(r.get("supply_assets", 0) or 0) / 1e6
            b = int(r.get("borrow_assets", 0) or 0) / 1e6
            c = int(r.get("collateral_assets", 0) or 0) / 1e6
            u = float(r.get("utilization", 0) or 0)
            if s > 0 or b > 0:
                print(f"    {d}  supply={s:>14,.0f}  borrow={b:>14,.0f}  collateral={c:>14,.0f}  util={u:.2%}")
    time.sleep(DELAY)

    fetch_borrower_positions(PLUME_XUSD_USDC, "block8_elixir_stream_positions.csv")
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

    # ── 5. Arbitrum xUSD/USDC ──
    print(f"\n{'='*70}")
    print("ARBITRUM xUSD/USDC (0x9e90)")
    print(f"{'='*70}")

    arb_txs = fetch_transactions(ARB_XUSD_USDC, "block8_arb_transactions.csv")
    if arb_txs:
        print(f"\n  Arbitrum transaction breakdown:")
        by_type = {}
        for tx in arb_txs:
            t = tx["type"]
            by_type[t] = by_type.get(t, 0) + 1
        for t, c in sorted(by_type.items()):
            print(f"    {t}: {c}")

        print(f"\n  Nov 3-5 events (>$1000):")
        for tx in arb_txs:
            if tx["date"] in ("2025-11-03", "2025-11-04", "2025-11-05"):
                usd = float(tx.get("assets_usd", 0) or 0)
                if usd > 1000 or tx["type"] == "MarketLiquidation":
                    seized = float(tx.get("seized_assets_usd", 0) or 0)
                    bd = float(tx.get("bad_debt_assets_usd", 0) or 0)
                    extra = ""
                    if tx["type"] == "MarketLiquidation":
                        extra = f" seized=${seized:,.0f} bad_debt=${bd:,.0f}"
                    print(f"    {tx['datetime']}  {tx['type']:<25} ${usd:>12,.2f}  {tx['user_address'][:12]}...{extra}")
    time.sleep(DELAY)

    arb_oracle, _ = fetch_market_history(ARB_XUSD_USDC, "block8_arb_market_history.csv")
    time.sleep(DELAY)

    fetch_borrower_positions(ARB_XUSD_USDC, "block8_arb_positions.csv")
    time.sleep(DELAY)

    # ── 6. Ethereum deUSD/USDC (small but complete the picture) ──
    print(f"\n{'='*70}")
    print("ETHEREUM deUSD/USDC (0xbd1a)")
    print(f"{'='*70}")

    deusd_txs = fetch_transactions(ETH_DEUSD_USDC, "block8_eth_deusd_transactions.csv")
    if deusd_txs:
        print(f"\n  deUSD/USDC transaction breakdown:")
        by_type = {}
        for tx in deusd_txs:
            t = tx["type"]
            by_type[t] = by_type.get(t, 0) + 1
        for t, c in sorted(by_type.items()):
            print(f"    {t}: {c}")
    time.sleep(DELAY)

    deusd_oracle, _ = fetch_market_history(ETH_DEUSD_USDC, "block8_eth_deusd_market_history.csv")
    time.sleep(DELAY)

    # ── 7. Oracle comparison (all markets) ──
    all_oracles = [
        (plume_oracle, "sdeUSD/pUSD Plume"),
        (eth_oracle, "sdeUSD/USDC Ethereum"),
        (es_oracle if es_oracle else {}, "xUSD/USDC Plume [Elixir-Stream]"),
        (arb_oracle if arb_oracle else {}, "xUSD/USDC Arbitrum"),
        (deusd_oracle if deusd_oracle else {}, "deUSD/USDC Ethereum"),
    ]
    write_oracle_comparison_multi(all_oracles)

    # ── 8. Vault TVL + allocation hourly ──
    fetch_vault_histories()

    # ── Summary ──
    print(f"\n{'='*70}")
    print("BLOCK 8 COMPLETE")
    print(f"{'='*70}")
    print(f"Files written to: {DATA_DIR}/")
    for f in sorted(os.listdir(DATA_DIR)):
        if f.startswith("block8_"):
            size = os.path.getsize(os.path.join(DATA_DIR, f))
            print(f"  {f} ({size:,} bytes)")

    print(f"\nAll significant markets now have hourly timeseries for cross-checking.")


if __name__ == "__main__":
    main()
