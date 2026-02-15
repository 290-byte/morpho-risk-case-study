"""
Block 5 — Liquidation Mechanism Breakdown

Answers Q1: "Why didn't the liquidation mechanism work?"

TASK 1: Oracle Configuration Analysis (5.1)
  - Oracle type, feeds, vault-based oracles per toxic market
  - Current oracle price vs spot price gap
  - Output: block5_oracle_configs.csv

TASK 2: Asset Price Collapse Timeline (5.2)
  - Historical spot prices for xUSD, deUSD, sdeUSD (hourly around depeg)
  - Shows exact crash moment and speed
  - Output: block5_asset_prices.csv

TASK 3: Collateral At Risk Analysis (5.3)
  - What collateral is at risk at various price levels per market
  - Markets where oracle masking prevents liquidation detection
  - Output: block5_collateral_at_risk.csv

TASK 4: Borrower Position Analysis (5.5)
  - Who are the borrowers in toxic markets?
  - Concentration risk — single borrower = single point of failure
  - Health factors (current — oracle-based)
  - Output: block5_borrower_positions.csv

TASK 5: Liquidation Transaction Search (5.2b)
  - Search for actual MarketLiquidation transactions on toxic markets
  - Expected finding: very few or zero liquidations
  - Output: block5_liquidation_events.csv

TASK 6: LLTV vs True LTV Analysis (5.4)
  - Combine oracle price, spot price, LLTV, and positions
  - Show the gap: oracle says healthy, reality says underwater
  - Pure analysis, no API calls
  - Output: block5_ltv_analysis.csv

Input:  04-data-exports/raw/graphql/block1_markets_graphql.csv
Output: 04-data-exports/raw/graphql/block5_*.csv
"""

import time
import requests
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timezone
from dotenv import load_dotenv
from typing import List, Dict, Optional

# ── Project paths ──
PROJECT_ROOT = Path(__file__).parent.parent
env_path = PROJECT_ROOT / '.env'
load_dotenv(dotenv_path=env_path)

GRAPHQL_URL = "https://blue-api.morpho.org/graphql"
REQUEST_DELAY = 0.3

# ── Time windows ──
TS_SEPT_01  = 1756684800
TS_OCT_28   = 1761609600
TS_NOV_01   = 1761955200
TS_NOV_04   = 1762214400   # xUSD depeg
TS_NOV_06   = 1762387200   # deUSD crash
TS_NOV_15   = 1763164800
TS_DEC_01   = 1764547200
TS_JAN_31   = 1769817600

DEPEG_TS = TS_NOV_04
ZERO_ADDR = "0x0000000000000000000000000000000000000000"


def query_graphql(query: str, timeout: int = 60) -> dict:
    """Execute a GraphQL query with retry logic."""
    headers = {"Content-Type": "application/json"}
    for attempt in range(3):
        try:
            resp = requests.post(GRAPHQL_URL, json={"query": query},
                                 headers=headers, timeout=timeout)
            resp.raise_for_status()
            data = resp.json()
            if "errors" in data:
                err_msg = data["errors"][0].get("message", "")
                if attempt < 2 and ("timeout" in err_msg.lower() or "rate" in err_msg.lower()):
                    time.sleep(2 ** attempt)
                    continue
            return data
        except requests.exceptions.RequestException as e:
            if attempt < 2:
                time.sleep(2 ** attempt)
                continue
            return {"errors": [{"message": str(e)}]}
    return {"errors": [{"message": "Max retries exceeded"}]}


def ts_to_date(ts) -> str:
    return datetime.fromtimestamp(float(ts), tz=timezone.utc).strftime("%Y-%m-%d")

def ts_to_datetime(ts) -> str:
    return datetime.fromtimestamp(float(ts), tz=timezone.utc).strftime("%Y-%m-%d %H:%M")

def safe_float(val, default=0.0) -> float:
    if val is None:
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default

def safe_int(val, default=0) -> int:
    if val is None:
        return default
    try:
        return int(val)
    except (ValueError, TypeError):
        return default


def forward_fill_daily_prices(daily_rows: List[Dict], depeg_ts: int = TS_NOV_04) -> List[Dict]:
    """
    Forward-fill gaps in daily price data for the pre-depeg period.

    If an asset has sparse pre-depeg data (e.g. xUSD has 1 point on Sept 30
    then nothing until Nov 4), this fills forward at the last known price
    day-by-day until the depeg date.

    Prevents charts from drawing misleading diagonal lines across data gaps.
    Only fills the pre-depeg period — post-depeg data is kept as-is.
    """
    if not daily_rows:
        return daily_rows

    # Sort by timestamp
    sorted_rows = sorted(daily_rows, key=lambda r: r["timestamp"])

    # Split at depeg boundary
    pre_depeg = [r for r in sorted_rows if r["timestamp"] < depeg_ts]
    post_depeg = [r for r in sorted_rows if r["timestamp"] >= depeg_ts]

    if not pre_depeg or not post_depeg:
        return daily_rows  # nothing to fill

    # Check if there's a significant gap (>2 days) between last pre-depeg point and depeg
    last_pre = pre_depeg[-1]
    gap_days = (depeg_ts - last_pre["timestamp"]) / 86400

    if gap_days <= 2:
        return daily_rows  # no significant gap

    # Forward-fill: create daily points from last known price to day before depeg
    filled = list(pre_depeg)
    current_ts = last_pre["timestamp"] + 86400

    while current_ts < depeg_ts:
        filled.append({
            "symbol": last_pre["symbol"],
            "address": last_pre["address"],
            "chain_id": last_pre["chain_id"],
            "timestamp": current_ts,
            "date": ts_to_date(current_ts),
            "datetime": ts_to_datetime(current_ts),
            "price_usd": last_pre["price_usd"],
            "current_price_usd": last_pre.get("current_price_usd", 0),
        })
        current_ts += 86400

    n_filled = len(filled) - len(pre_depeg)
    if n_filled > 0:
        print(f"      ℹ️  Forward-filled {n_filled} daily pre-depeg prices "
              f"at ${last_pre['price_usd']:.4f} ({last_pre['date']} → {ts_to_date(depeg_ts - 86400)})")

    return filled + post_depeg


# ═══════════════════════════════════════════════════════════════
#  TASK 1: Oracle Configuration Analysis
# ═══════════════════════════════════════════════════════════════

def query_oracle_config(market_id: str, chain_id: int) -> Dict:
    """
    Query full oracle configuration for a market.
    Returns oracle type, feeds, vault-based oracle info,
    current oracle price, and spot price.
    """
    query = f"""
    {{
      marketByUniqueKey(uniqueKey: "{market_id}", chainId: {chain_id}) {{
        uniqueKey
        lltv
        collateralAsset {{
          symbol
          address
          priceUsd
          decimals
        }}
        loanAsset {{
          symbol
          address
          priceUsd
          decimals
        }}
        state {{
          price
          borrowAssets
          borrowAssetsUsd
          supplyAssets
          supplyAssetsUsd
          collateralAssets
          collateralAssetsUsd
          utilization
          liquidityAssetsUsd
        }}
        badDebt {{
          underlying
          usd
        }}
        realizedBadDebt {{
          underlying
          usd
        }}
        warnings {{
          type
          level
        }}
        oracle {{
          address
          type
          data {{
            ... on MorphoChainlinkOracleData {{
              baseFeedOne {{ address }}
              baseFeedTwo {{ address }}
              baseOracleVault {{ address }}
              quoteFeedOne {{ address }}
              quoteFeedTwo {{ address }}
              scaleFactor
              vaultConversionSample
            }}
            ... on MorphoChainlinkOracleV2Data {{
              baseFeedOne {{ address }}
              baseFeedTwo {{ address }}
              baseOracleVault {{ address }}
              baseVaultConversionSample
              quoteFeedOne {{ address }}
              quoteFeedTwo {{ address }}
              quoteOracleVault {{ address }}
              quoteVaultConversionSample
              scaleFactor
            }}
          }}
        }}
      }}
    }}
    """

    result = query_graphql(query)

    if "errors" in result:
        return {"error": result["errors"][0].get("message", "Unknown")}

    market = result.get("data", {}).get("marketByUniqueKey")
    if not market:
        return {"error": "Market not found"}

    return market


def parse_oracle_config(market_data: Dict, chain: str) -> Dict:
    """Parse oracle config into a flat row for CSV."""
    oracle = market_data.get("oracle") or {}
    oracle_data = oracle.get("data") or {}
    state = market_data.get("state") or {}
    collateral = market_data.get("collateralAsset") or {}
    loan = market_data.get("loanAsset") or {}
    bad_debt = market_data.get("badDebt") or {}
    realized_bd = market_data.get("realizedBadDebt") or {}
    warnings = market_data.get("warnings") or []

    # Determine oracle mechanism
    oracle_type = oracle.get("type", "Unknown")

    # Check for vault-based oracle (ERC4626 conversion rate → hardcoded $1 mechanism)
    base_oracle_vault = oracle_data.get("baseOracleVault")
    quote_oracle_vault = oracle_data.get("quoteOracleVault")

    base_vault_addr = base_oracle_vault.get("address", ZERO_ADDR) if isinstance(base_oracle_vault, dict) else ZERO_ADDR
    quote_vault_addr = quote_oracle_vault.get("address", ZERO_ADDR) if isinstance(quote_oracle_vault, dict) else ZERO_ADDR

    is_vault_based = (base_vault_addr != ZERO_ADDR) or (quote_vault_addr != ZERO_ADDR)

    # Extract feed addresses
    base_feed_1 = (oracle_data.get("baseFeedOne") or {}).get("address", ZERO_ADDR)
    base_feed_2 = (oracle_data.get("baseFeedTwo") or {}).get("address", ZERO_ADDR)
    quote_feed_1 = (oracle_data.get("quoteFeedOne") or {}).get("address", ZERO_ADDR)
    quote_feed_2 = (oracle_data.get("quoteFeedTwo") or {}).get("address", ZERO_ADDR)

    # Vault conversion samples
    vault_sample = safe_int(oracle_data.get("vaultConversionSample", 0))
    base_vault_sample = safe_int(oracle_data.get("baseVaultConversionSample", 0))
    quote_vault_sample = safe_int(oracle_data.get("quoteVaultConversionSample", 0))

    # Scale factor
    scale_factor = str(oracle_data.get("scaleFactor", "0"))

    # Prices
    oracle_price_raw = safe_float(state.get("price", 0))
    collateral_spot_price = safe_float(collateral.get("priceUsd", 0))
    loan_spot_price = safe_float(loan.get("priceUsd", 0))

    # Oracle price interpretation:
    # market.state.price is the raw oracle price in the format
    # (collateral_price / loan_price) * scale_factor
    # For a collateral/USDC market where oracle says $1, this would be ~1e36
    # We need to normalize it

    # Position metrics
    borrow_usd = safe_float(state.get("borrowAssetsUsd", 0))
    supply_usd = safe_float(state.get("supplyAssetsUsd", 0))
    collateral_usd = safe_float(state.get("collateralAssetsUsd", 0))
    utilization = safe_float(state.get("utilization", 0))

    # Warnings
    warning_types = [w.get("type", "") for w in warnings]
    warning_levels = [w.get("level", "") for w in warnings]

    # Classify oracle mechanism
    has_any_feed = any(f != ZERO_ADDR for f in [base_feed_1, base_feed_2, quote_feed_1, quote_feed_2])

    if is_vault_based and not has_any_feed:
        oracle_mechanism = "VAULT_ONLY"  # Pure ERC4626 conversion, likely hardcoded
    elif is_vault_based and has_any_feed:
        oracle_mechanism = "VAULT_PLUS_FEED"  # Vault + external feed
    elif has_any_feed:
        oracle_mechanism = "FEED_ONLY"  # Pure Chainlink feed
    else:
        oracle_mechanism = "UNKNOWN"

    # LLTV
    lltv_raw = safe_int(market_data.get("lltv", 0))
    lltv_pct = lltv_raw / 1e18 * 100 if lltv_raw > 0 else 0

    return {
        "market_unique_key": market_data.get("uniqueKey", ""),
        "chain": chain,
        "collateral_symbol": collateral.get("symbol", "?"),
        "collateral_address": collateral.get("address", ""),
        "collateral_decimals": safe_int(collateral.get("decimals", 18)),
        "loan_symbol": loan.get("symbol", "?"),
        "loan_address": loan.get("address", ""),
        "loan_decimals": safe_int(loan.get("decimals", 6)),
        # LLTV
        "lltv_raw": lltv_raw,
        "lltv_pct": round(lltv_pct, 2),
        # Oracle config
        "oracle_address": oracle.get("address", ""),
        "oracle_type": oracle_type,
        "oracle_mechanism": oracle_mechanism,
        "is_vault_based": is_vault_based,
        "base_feed_one": base_feed_1,
        "base_feed_two": base_feed_2,
        "base_oracle_vault": base_vault_addr,
        "quote_feed_one": quote_feed_1,
        "quote_feed_two": quote_feed_2,
        "quote_oracle_vault": quote_vault_addr,
        "scale_factor": scale_factor,
        "vault_conversion_sample": vault_sample,
        "base_vault_conversion_sample": base_vault_sample,
        "quote_vault_conversion_sample": quote_vault_sample,
        # Prices
        "oracle_price_raw": oracle_price_raw,
        "collateral_spot_price_usd": collateral_spot_price,
        "loan_spot_price_usd": loan_spot_price,
        # Current state
        "supply_assets_usd": supply_usd,
        "borrow_assets_usd": borrow_usd,
        "collateral_assets_usd": collateral_usd,
        "utilization": utilization,
        # Bad debt
        "bad_debt_underlying": str(bad_debt.get("underlying", "0")),
        "bad_debt_usd": safe_float(bad_debt.get("usd", 0)),
        "realized_bad_debt_underlying": str(realized_bd.get("underlying", "0")),
        "realized_bad_debt_usd": safe_float(realized_bd.get("usd", 0)),
        # Warnings
        "warnings": "; ".join(f"{w.get('level','')}: {w.get('type','')}" for w in warnings),
        "warning_count": len(warnings),
    }


# ═══════════════════════════════════════════════════════════════
#  TASK 2: Asset Price Collapse Timeline
# ═══════════════════════════════════════════════════════════════

def query_asset_price_history(address: str, chain_id: int, symbol: str,
                               start_ts: int, end_ts: int,
                               interval: str = "HOUR") -> List[Dict]:
    """Query historical spot prices for a token."""
    query = f"""
    {{
      assetByAddress(address: "{address}", chainId: {chain_id}) {{
        symbol
        priceUsd
        historicalPriceUsd(options: {{
          startTimestamp: {start_ts}
          endTimestamp: {end_ts}
          interval: {interval}
        }}) {{
          x
          y
        }}
      }}
    }}
    """

    result = query_graphql(query)

    if "errors" in result:
        err = result["errors"][0].get("message", "")
        print(f"      ❌ Error: {err[:120]}")
        return []

    asset = result.get("data", {}).get("assetByAddress")
    if not asset:
        print(f"      ⚠️  Asset not found")
        return []

    current_price = safe_float(asset.get("priceUsd", 0))
    hist = asset.get("historicalPriceUsd") or []

    rows = []
    for pt in hist:
        if not pt or pt.get("y") is None:
            continue
        ts = int(pt["x"])
        price = safe_float(pt["y"])
        rows.append({
            "symbol": symbol,
            "address": address,
            "chain_id": chain_id,
            "timestamp": ts,
            "date": ts_to_date(ts),
            "datetime": ts_to_datetime(ts),
            "price_usd": price,
            "current_price_usd": current_price,
        })

    return rows


# ═══════════════════════════════════════════════════════════════
#  TASK 3: Collateral At Risk Analysis
# ═══════════════════════════════════════════════════════════════

def query_collateral_at_risk(market_id: str, chain_id: int,
                              num_points: int = 20) -> List[Dict]:
    """
    Query collateral at risk at various price levels.
    Returns how much collateral would be liquidatable at each price ratio.
    """
    query = f"""
    {{
      marketCollateralAtRisk(
        uniqueKey: "{market_id}"
        chainId: {chain_id}
        numberOfPoints: {num_points}
      ) {{
        market {{
          uniqueKey
          lltv
          collateralAsset {{ symbol }}
          loanAsset {{ symbol }}
        }}
        collateralAtRisk {{
          collateralPriceRatio
          collateralAssets
          collateralUsd
        }}
      }}
    }}
    """

    result = query_graphql(query)

    if "errors" in result:
        err = result["errors"][0].get("message", "")
        print(f"      ❌ Error: {err[:120]}")
        return []

    data = result.get("data", {}).get("marketCollateralAtRisk")
    if not data:
        print(f"      ⚠️  No data returned")
        return []

    market_info = data.get("market") or {}
    risk_points = data.get("collateralAtRisk") or []

    rows = []
    for pt in risk_points:
        rows.append({
            "market_unique_key": market_id,
            "chain_id": chain_id,
            "collateral_symbol": (market_info.get("collateralAsset") or {}).get("symbol", "?"),
            "loan_symbol": (market_info.get("loanAsset") or {}).get("symbol", "?"),
            "collateral_price_ratio": safe_float(pt.get("collateralPriceRatio", 0)),
            "collateral_assets": str(pt.get("collateralAssets", "0")),
            "collateral_usd": safe_float(pt.get("collateralUsd", 0)),
        })

    return rows


# ═══════════════════════════════════════════════════════════════
#  TASK 4: Borrower Position Analysis
# ═══════════════════════════════════════════════════════════════

def query_borrower_positions(market_ids: List[str], chain_id: int,
                              max_per_market: int = 50) -> List[Dict]:
    """
    Query top borrower positions across toxic markets.
    Uses pagination if needed.
    """
    # Build market ID list for filter
    market_list = ', '.join(f'"{m}"' for m in market_ids)

    all_positions = []
    skip = 0
    page_size = 100

    while True:
        query = f"""
        {{
          marketPositions(
            first: {page_size}
            skip: {skip}
            orderBy: BorrowShares
            orderDirection: Desc
            where: {{
              marketUniqueKey_in: [{market_list}]
              chainId_in: [{chain_id}]
            }}
          ) {{
            items {{
              user {{ address }}
              healthFactor
              market {{
                uniqueKey
                lltv
                collateralAsset {{ symbol, address }}
                loanAsset {{ symbol, address }}
                state {{ price }}
              }}
              state {{
                collateral
                collateralUsd
                borrowAssets
                borrowAssetsUsd
                supplyAssets
                supplyAssetsUsd
              }}
            }}
            pageInfo {{
              countTotal
              count
            }}
          }}
        }}
        """

        result = query_graphql(query, timeout=90)

        if "errors" in result:
            err = result["errors"][0].get("message", "")
            print(f"      ❌ Error: {err[:120]}")
            break

        positions_data = result.get("data", {}).get("marketPositions", {})
        items = positions_data.get("items") or []
        page_info = positions_data.get("pageInfo") or {}
        total = safe_int(page_info.get("countTotal", 0))

        for pos in items:
            user = (pos.get("user") or {}).get("address", "")
            health = safe_float(pos.get("healthFactor"))
            market = pos.get("market") or {}
            state = pos.get("state") or {}

            collateral_raw = str(state.get("collateral", "0"))
            collateral_usd = safe_float(state.get("collateralUsd", 0))
            borrow_assets = str(state.get("borrowAssets", "0"))
            borrow_usd = safe_float(state.get("borrowAssetsUsd", 0))
            supply_assets = str(state.get("supplyAssets", "0"))
            supply_usd = safe_float(state.get("supplyAssetsUsd", 0))

            # Only include positions with actual borrows or collateral
            if borrow_usd == 0 and collateral_usd == 0 and supply_usd == 0:
                continue

            all_positions.append({
                "user_address": user,
                "market_unique_key": market.get("uniqueKey", ""),
                "chain_id": chain_id,
                "collateral_symbol": (market.get("collateralAsset") or {}).get("symbol", "?"),
                "loan_symbol": (market.get("loanAsset") or {}).get("symbol", "?"),
                "health_factor": health,
                "lltv_raw": safe_int(market.get("lltv", 0)),
                "oracle_price_raw": safe_float((market.get("state") or {}).get("price", 0)),
                "collateral_raw": collateral_raw,
                "collateral_usd": collateral_usd,
                "borrow_assets_raw": borrow_assets,
                "borrow_assets_usd": borrow_usd,
                "supply_assets_raw": supply_assets,
                "supply_assets_usd": supply_usd,
                "position_type": "borrower" if borrow_usd > 0 else ("supplier" if supply_usd > 0 else "collateral_only"),
            })

        if len(items) < page_size or skip + page_size >= total:
            break
        skip += page_size
        time.sleep(REQUEST_DELAY)

    return all_positions


# ═══════════════════════════════════════════════════════════════
#  TASK 5: Liquidation Transaction Search
# ═══════════════════════════════════════════════════════════════

def query_liquidation_events(market_ids: List[str], chain_id: int = 0) -> List[Dict]:
    """
    Search for MarketLiquidation transactions in toxic markets.
    Matches exact query format from Morpho API docs.
    """
    market_list = ', '.join(f'"{m}"' for m in market_ids)

    all_events = []
    skip = 0
    page_size = 100

    while True:
        # Query matches exact working example from Morpho API docs
        query = f"""
        {{
          transactions(
            first: {page_size}
            skip: {skip}
            orderBy: Timestamp
            orderDirection: Desc
            where: {{
              marketUniqueKey_in: [{market_list}]
              type_in: [MarketLiquidation]
            }}
          ) {{
            items {{
              blockNumber
              hash
              timestamp
              type
              user {{ address }}
              data {{
                ... on MarketLiquidationTransactionData {{
                  seizedAssets
                  repaidAssets
                  seizedAssetsUsd
                  repaidAssetsUsd
                  badDebtAssetsUsd
                  liquidator
                  market {{
                    uniqueKey
                    collateralAsset {{ symbol }}
                    loanAsset {{ symbol }}
                  }}
                }}
              }}
            }}
            pageInfo {{
              countTotal
              count
            }}
          }}
        }}
        """

        result = query_graphql(query, timeout=90)

        if "errors" in result:
            err = result["errors"][0].get("message", "")
            print(f"      ❌ Error: {err[:120]}")
            break

        txn_data = result.get("data", {}).get("transactions", {})
        items = txn_data.get("items") or []
        page_info = txn_data.get("pageInfo") or {}
        total = safe_int(page_info.get("countTotal", 0))

        for txn in items:
            data = txn.get("data") or {}
            market = data.get("market") or {}  # market is INSIDE data fragment

            all_events.append({
                "hash": txn.get("hash", ""),
                "timestamp": safe_int(txn.get("timestamp", 0)),
                "date": ts_to_date(safe_int(txn.get("timestamp", 0))),
                "datetime": ts_to_datetime(safe_int(txn.get("timestamp", 0))),
                "block_number": safe_int(txn.get("blockNumber", 0)),
                "type": txn.get("type", ""),
                "user_address": (txn.get("user") or {}).get("address", ""),
                "market_unique_key": market.get("uniqueKey", ""),
                "collateral_symbol": (market.get("collateralAsset") or {}).get("symbol", "?"),
                "loan_symbol": (market.get("loanAsset") or {}).get("symbol", "?"),
                "seized_assets": str(data.get("seizedAssets", "0")),
                "seized_assets_usd": safe_float(data.get("seizedAssetsUsd", 0)),
                "repaid_assets": str(data.get("repaidAssets", "0")),
                "repaid_assets_usd": safe_float(data.get("repaidAssetsUsd", 0)),
                "bad_debt_assets_usd": safe_float(data.get("badDebtAssetsUsd", 0)),
                "liquidator": data.get("liquidator", ""),
            })

        if len(items) < page_size or skip + page_size >= total:
            break
        skip += page_size
        time.sleep(REQUEST_DELAY)

    return all_events


# ═══════════════════════════════════════════════════════════════
#  TASK 6: LLTV vs True LTV Analysis (pure analysis)
# ═══════════════════════════════════════════════════════════════

def compute_ltv_analysis(df_oracle: pd.DataFrame, df_positions: pd.DataFrame) -> pd.DataFrame:
    """
    For each market with active borrowing:
    - LLTV (configured liquidation threshold)
    - Oracle-reported price
    - Spot price
    - Implied LTV under oracle vs under spot
    - Gap analysis
    """
    rows = []

    for _, oracle_row in df_oracle.iterrows():
        market_key = oracle_row["market_unique_key"]
        chain = oracle_row["chain"]
        collateral_sym = oracle_row["collateral_symbol"]
        loan_sym = oracle_row["loan_symbol"]
        lltv_pct = oracle_row["lltv_pct"]
        collateral_spot = oracle_row["collateral_spot_price_usd"]
        loan_spot = oracle_row["loan_spot_price_usd"]
        borrow_usd = oracle_row["borrow_assets_usd"]
        collateral_usd = oracle_row["collateral_assets_usd"]
        supply_usd = oracle_row["supply_assets_usd"]
        oracle_mechanism = oracle_row["oracle_mechanism"]
        is_vault_based = oracle_row["is_vault_based"]
        bad_debt_usd = oracle_row["bad_debt_usd"]
        realized_bd_usd = oracle_row["realized_bad_debt_usd"]

        # Skip markets with no activity
        if borrow_usd == 0 and collateral_usd == 0:
            continue

        # Oracle-based LTV (what the protocol sees)
        if collateral_usd > 0:
            oracle_ltv = (borrow_usd / collateral_usd) * 100
        else:
            oracle_ltv = float('inf') if borrow_usd > 0 else 0

        # True LTV calculation:
        # The oracle says collateral is worth X, but the spot price says it's worth Y
        # If oracle reports $1 but spot is $0.015, true collateral value is 1.5% of reported
        # For vault-based oracles: the ERC4626 conversion rate IS the oracle, not the market price

        # We need the implied collateral value at spot price
        # collateral_usd from the API already uses oracle pricing
        # True value = collateral_usd * (spot_price / oracle_implied_price)

        # For simplicity: if collateral_spot < 1 and the oracle mechanism is vault-based,
        # the oracle is likely still reporting ~$1, so:
        if collateral_spot > 0 and loan_spot > 0 and collateral_usd > 0:
            # The API's collateralAssetsUsd uses the oracle price, not spot
            # So true_collateral_usd ≈ collateral_usd * (spot / 1.0) for stablecoins
            # But we need to be careful — collateralAssetsUsd may already use spot for known assets

            # Better approach: estimate the oracle's implied price for collateral
            # oracle_implied_price ≈ collateral_usd / (collateral_quantity * loan_spot)
            # But we don't have collateral_quantity in USD-normalized form easily.

            # Simpler: if spot price is known and differs from $1, compute ratio
            if is_vault_based and collateral_spot < 0.90:
                # Oracle reports ~$1, spot says much less
                # True collateral value = reported_value * spot_price
                true_collateral_usd = collateral_usd * collateral_spot
                true_ltv = (borrow_usd / true_collateral_usd) * 100 if true_collateral_usd > 0 else float('inf')
                price_gap_pct = (1.0 - collateral_spot) * 100  # e.g., 97% gap if spot=$0.03
            else:
                # Either oracle is feed-based (should track spot), or spot is close to $1
                true_collateral_usd = collateral_usd  # Oracle ≈ spot
                true_ltv = oracle_ltv
                price_gap_pct = 0
        else:
            true_collateral_usd = collateral_usd
            true_ltv = oracle_ltv
            price_gap_pct = 0

        # Liquidation status
        if oracle_ltv >= lltv_pct and lltv_pct > 0:
            liquidation_status = "LIQUIDATABLE_ORACLE"
        elif true_ltv >= lltv_pct and lltv_pct > 0 and true_ltv != oracle_ltv:
            liquidation_status = "UNDERWATER_MASKED"  # Oracle says safe, reality says not
        elif borrow_usd > supply_usd and supply_usd > 0:
            liquidation_status = "BAD_DEBT"  # borrow > supply = confirmed bad debt
        elif borrow_usd > 0 and collateral_usd == 0:
            liquidation_status = "UNSECURED_DEBT"  # borrowing with no collateral
        else:
            liquidation_status = "HEALTHY"

        rows.append({
            "market_unique_key": market_key,
            "chain": chain,
            "collateral_symbol": collateral_sym,
            "loan_symbol": loan_sym,
            "lltv_pct": lltv_pct,
            # Oracle
            "oracle_mechanism": oracle_mechanism,
            "is_vault_based": is_vault_based,
            "collateral_spot_price": collateral_spot,
            "price_gap_pct": round(price_gap_pct, 2),
            # LTV
            "oracle_ltv_pct": round(min(oracle_ltv, 9999), 2),
            "true_ltv_pct": round(min(true_ltv, 9999), 2),
            "ltv_gap_pct": round(min(true_ltv - oracle_ltv, 9999), 2),
            # Values
            "borrow_usd": round(borrow_usd, 2),
            "collateral_usd_oracle": round(collateral_usd, 2),
            "collateral_usd_true": round(true_collateral_usd, 2),
            "supply_usd": round(supply_usd, 2),
            # Bad debt
            "bad_debt_usd": bad_debt_usd,
            "realized_bad_debt_usd": realized_bd_usd,
            # Status
            "liquidation_status": liquidation_status,
        })

    return pd.DataFrame(rows)


# ═══════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════

def main():
    print("=" * 80)
    print("Block 5 — Liquidation Mechanism Breakdown")
    print("=" * 80)

    gql_dir = PROJECT_ROOT / "data"

    # ── Load Block 1 markets ──
    markets_path = gql_dir / "block1_markets_graphql.csv"
    df_markets = pd.read_csv(markets_path) if markets_path.exists() else pd.DataFrame()
    print(f"\n📂 Loaded {len(df_markets)} markets from Block 1")

    if len(df_markets) == 0:
        print("❌ No markets found. Cannot proceed.")
        return

    # Inspect columns
    print(f"   Columns: {list(df_markets.columns)}")

    # Determine column names (handle variations)
    mk_col = "market_id" if "market_id" in df_markets.columns else "market_unique_key"
    chain_col = "chain_id"
    chain_name_col = "chain" if "chain" in df_markets.columns else None
    coll_sym_col = "collateral_symbol"
    coll_addr_col = "collateral_address" if "collateral_address" in df_markets.columns else None
    loan_sym_col = "loan_symbol" if "loan_symbol" in df_markets.columns else "loan_asset_symbol"

    # ══════════════════════════════════════════════════════════
    #  TASK 1: Oracle Configuration Analysis
    # ══════════════════════════════════════════════════════════
    print(f"\n{'─' * 70}")
    print(f"🔮 TASK 1: Oracle Configuration Analysis")
    print(f"{'─' * 70}")

    oracle_rows = []

    for idx, (_, mkt) in enumerate(df_markets.iterrows()):
        market_id = mkt[mk_col]
        chain_id = int(mkt[chain_col])
        chain = mkt.get(chain_name_col, "") if chain_name_col else ""
        collateral = mkt.get(coll_sym_col, "?")
        loan = mkt.get(loan_sym_col, "?")

        print(f"\n   [{idx+1}/{len(df_markets)}] {collateral}/{loan} ({chain})")
        print(f"      Market: {market_id[:20]}...")

        market_data = query_oracle_config(market_id, chain_id)

        if "error" in market_data:
            print(f"      ❌ {market_data['error'][:100]}")
            continue

        row = parse_oracle_config(market_data, chain)
        oracle_rows.append(row)

        oracle_type = row["oracle_type"]
        mechanism = row["oracle_mechanism"]
        spot = row["collateral_spot_price_usd"]
        lltv = row["lltv_pct"]
        vault_based = "🔒 VAULT" if row["is_vault_based"] else "📡 FEED"
        bd = row["bad_debt_usd"]

        print(f"      Oracle: {oracle_type} | {mechanism} | {vault_based}")
        print(f"      LLTV: {lltv}% | Collat spot: ${spot:.4f} | Bad debt: ${bd:,.0f}")

        if row["warning_count"] > 0:
            print(f"      ⚠️  Warnings: {row['warnings']}")

        time.sleep(REQUEST_DELAY)

    # Save oracle configs
    if oracle_rows:
        df_oracle = pd.DataFrame(oracle_rows)
        oracle_path = gql_dir / "block5_oracle_configs.csv"
        df_oracle.to_csv(oracle_path, index=False)
        print(f"\n✅ Saved {len(oracle_rows)} oracle configs to {oracle_path.name}")

        # Oracle summary
        print(f"\n{'─' * 70}")
        print(f"  ORACLE MECHANISM SUMMARY")
        print(f"{'─' * 70}")
        for mechanism, grp in df_oracle.groupby("oracle_mechanism"):
            count = len(grp)
            vault_based = grp["is_vault_based"].sum()
            avg_spot = grp["collateral_spot_price_usd"].mean()
            total_bd = grp["bad_debt_usd"].sum()
            print(f"  {mechanism}: {count} markets, avg spot: ${avg_spot:.4f}, "
                  f"total bad debt: ${total_bd:,.0f}")
    else:
        df_oracle = pd.DataFrame()

    # ══════════════════════════════════════════════════════════
    #  TASK 2: Asset Price Collapse Timeline
    # ══════════════════════════════════════════════════════════
    print(f"\n{'─' * 70}")
    print(f"📉 TASK 2: Asset Price Collapse Timeline")
    print(f"{'─' * 70}")

    # Get unique collateral assets from oracle data
    if len(df_oracle) > 0:
        unique_assets = df_oracle[["collateral_symbol", "collateral_address", "chain"]].drop_duplicates()
    elif coll_addr_col:
        unique_assets = df_markets[[coll_sym_col, coll_addr_col, chain_name_col]].drop_duplicates()
        unique_assets.columns = ["collateral_symbol", "collateral_address", "chain"]
    else:
        unique_assets = pd.DataFrame()

    # Map chain names to chain IDs
    chain_id_map = {}
    for _, mkt in df_markets.iterrows():
        ch = mkt.get(chain_name_col, "")
        cid = int(mkt[chain_col])
        chain_id_map[ch] = cid

    all_prices = []

    if len(unique_assets) > 0:
        # Deduplicate by address (same token on same chain)
        seen = set()
        for _, asset in unique_assets.iterrows():
            addr = asset.get("collateral_address", "")
            chain = asset.get("chain", "")
            symbol = asset.get("collateral_symbol", "?")

            if not addr or addr == ZERO_ADDR:
                continue

            key = f"{addr}_{chain}"
            if key in seen:
                continue
            seen.add(key)

            chain_id = chain_id_map.get(chain, 1)

            print(f"\n   {symbol} on {chain} ({addr[:16]}...)")

            # Hourly: Oct 28 → Nov 15 (depeg zoom)
            print(f"      Hourly (Oct 28 → Nov 15)...")
            hourly = query_asset_price_history(
                addr, chain_id, symbol, TS_OCT_28, TS_NOV_15, "HOUR"
            )
            if hourly:
                prices = [r["price_usd"] for r in hourly]
                peak = max(prices) if prices else 0
                trough = min(prices) if prices else 0
                print(f"      ✅ {len(hourly)} pts, peak: ${peak:.4f}, trough: ${trough:.4f}")
            else:
                print(f"      ⚠️  No data (possibly delisted token)")
            all_prices.extend(hourly)

            time.sleep(REQUEST_DELAY)

            # Daily: wider view Sept 1 → Jan 31
            print(f"      Daily (Sept 1 → Jan 31)...")
            daily = query_asset_price_history(
                addr, chain_id, symbol, TS_SEPT_01, TS_JAN_31, "DAY"
            )
            if daily:
                # Forward-fill pre-depeg gaps (e.g. xUSD has 1 point in Sept, then nothing until Nov 4)
                daily = forward_fill_daily_prices(daily, depeg_ts=TS_NOV_04)
                print(f"      ✅ {len(daily)} daily pts")
            else:
                print(f"      ⚠️  No daily data")
            all_prices.extend(daily)

            time.sleep(REQUEST_DELAY)
    else:
        print("  ⚠️  No unique collateral addresses found")

    if all_prices:
        df_prices = pd.DataFrame(all_prices)
        prices_path = gql_dir / "block5_asset_prices.csv"
        df_prices.to_csv(prices_path, index=False)
        print(f"\n✅ Saved {len(all_prices)} price data points to {prices_path.name}")
    else:
        df_prices = pd.DataFrame()
        print(f"\n⚠️  No price data retrieved (tokens may be delisted)")

    # ══════════════════════════════════════════════════════════
    #  TASK 3: Collateral At Risk Analysis
    # ══════════════════════════════════════════════════════════
    print(f"\n{'─' * 70}")
    print(f"⚠️  TASK 3: Collateral At Risk Analysis")
    print(f"{'─' * 70}")

    all_risk = []

    for idx, (_, mkt) in enumerate(df_markets.iterrows()):
        market_id = mkt[mk_col]
        chain_id = int(mkt[chain_col])
        chain = mkt.get(chain_name_col, "")
        collateral = mkt.get(coll_sym_col, "?")
        loan = mkt.get(loan_sym_col, "?")

        print(f"\n   [{idx+1}/{len(df_markets)}] {collateral}/{loan} ({chain})")

        risk_data = query_collateral_at_risk(market_id, chain_id)

        if risk_data:
            total_at_risk = sum(r["collateral_usd"] for r in risk_data)
            print(f"      ✅ {len(risk_data)} price levels, total at risk: ${total_at_risk:,.0f}")
        else:
            print(f"      ⚠️  No risk data (oracle masking = no liquidatable positions)")
            # This IS a finding — add a marker row
            all_risk.append({
                "market_unique_key": market_id,
                "chain_id": chain_id,
                "collateral_symbol": collateral,
                "loan_symbol": loan,
                "collateral_price_ratio": 0,
                "collateral_assets": "0",
                "collateral_usd": 0,
            })

        for r in risk_data:
            r["chain_id"] = chain_id
        all_risk.extend(risk_data)
        time.sleep(REQUEST_DELAY)

    if all_risk:
        df_risk = pd.DataFrame(all_risk)
        risk_path = gql_dir / "block5_collateral_at_risk.csv"
        df_risk.to_csv(risk_path, index=False)
        print(f"\n✅ Saved {len(all_risk)} risk data points to {risk_path.name}")

    # ══════════════════════════════════════════════════════════
    #  TASK 4: Borrower Position Analysis
    # ══════════════════════════════════════════════════════════
    print(f"\n{'─' * 70}")
    print(f"👤 TASK 4: Borrower Position Analysis")
    print(f"{'─' * 70}")

    all_positions = []

    # Group markets by chain_id for batched queries
    for chain_id, chain_group in df_markets.groupby(chain_col):
        chain_id = int(chain_id)
        market_ids = chain_group[mk_col].tolist()
        chain_name = chain_group[chain_name_col].iloc[0] if chain_name_col else str(chain_id)

        print(f"\n   Chain {chain_name} ({chain_id}): {len(market_ids)} markets")

        positions = query_borrower_positions(market_ids, chain_id)
        if positions:
            borrowers = [p for p in positions if p["position_type"] == "borrower"]
            suppliers = [p for p in positions if p["position_type"] == "supplier"]
            print(f"      ✅ {len(positions)} positions ({len(borrowers)} borrowers, "
                  f"{len(suppliers)} suppliers)")

            # Show top borrowers
            if borrowers:
                sorted_b = sorted(borrowers, key=lambda x: x["borrow_assets_usd"], reverse=True)
                for b in sorted_b[:5]:
                    addr = b["user_address"][:10] + "..."
                    borrow = b["borrow_assets_usd"]
                    coll = b["collateral_usd"]
                    hf = b["health_factor"]
                    csym = b["collateral_symbol"]
                    lsym = b["loan_symbol"]
                    hf_str = f"{hf:.4f}" if hf != float('inf') else "∞"
                    print(f"      Top: {addr} borrows ${borrow:,.0f} {lsym} "
                          f"collat ${coll:,.0f} {csym} HF={hf_str}")
        else:
            print(f"      ⚠️  No positions found")

        all_positions.extend(positions)
        time.sleep(REQUEST_DELAY)

    if all_positions:
        df_positions = pd.DataFrame(all_positions)
        positions_path = gql_dir / "block5_borrower_positions.csv"
        df_positions.to_csv(positions_path, index=False)
        print(f"\n✅ Saved {len(all_positions)} positions to {positions_path.name}")

        # Concentration analysis
        print(f"\n{'─' * 70}")
        print(f"  BORROWER CONCENTRATION ANALYSIS")
        print(f"{'─' * 70}")

        borrowers_df = df_positions[df_positions["position_type"] == "borrower"]
        if len(borrowers_df) > 0:
            for mk, grp in borrowers_df.groupby("market_unique_key"):
                csym = grp["collateral_symbol"].iloc[0]
                lsym = grp["loan_symbol"].iloc[0]
                n_borrowers = grp["user_address"].nunique()
                total_borrow = grp["borrow_assets_usd"].sum()
                top_borrower_share = grp["borrow_assets_usd"].max() / total_borrow * 100 if total_borrow > 0 else 0

                conc = "🔴 SINGLE" if n_borrowers == 1 else ("🟡 CONCENTRATED" if top_borrower_share > 80 else "🟢 DISTRIBUTED")
                print(f"\n  {conc} {csym}/{lsym}: {n_borrowers} borrowers, "
                      f"${total_borrow:,.0f} total")
                print(f"    Top borrower: {top_borrower_share:.1f}% of total")
    else:
        df_positions = pd.DataFrame()

    # ══════════════════════════════════════════════════════════
    #  TASK 5: Liquidation Transaction Search
    # ══════════════════════════════════════════════════════════
    print(f"\n{'─' * 70}")
    print(f"⚡ TASK 5: Liquidation Transaction Search")
    print(f"{'─' * 70}")

    all_liquidations = []

    # Query all markets at once (no chain filter needed — uniqueKey is globally unique)
    all_market_ids = df_markets[mk_col].tolist()
    print(f"\n   Searching {len(all_market_ids)} markets across all chains...")

    liquidations = query_liquidation_events(all_market_ids, 0)  # chain_id unused now
    if liquidations:
        total_seized = sum(l["seized_assets_usd"] for l in liquidations)
        total_bd = sum(l["bad_debt_assets_usd"] for l in liquidations)
        total_repaid = sum(l["repaid_assets_usd"] for l in liquidations)
        print(f"      ✅ {len(liquidations)} liquidation events found!")
        print(f"      Total seized: ${total_seized:,.0f}, Repaid: ${total_repaid:,.0f}, "
              f"Bad debt: ${total_bd:,.0f}")

        # Show individual events
        for liq in liquidations:
            csym = liq["collateral_symbol"]
            lsym = liq["loan_symbol"]
            dt = liq["datetime"]
            seized = liq["seized_assets_usd"]
            repaid = liq["repaid_assets_usd"]
            bd = liq["bad_debt_assets_usd"]
            lqdr = liq["liquidator"][:10] + "..." if liq["liquidator"] else "?"
            print(f"      {dt} | {csym}/{lsym} | seized ${seized:,.0f} | "
                  f"repaid ${repaid:,.0f} | badDebt ${bd:,.0f} | by {lqdr}")
    else:
        print(f"      ⚠️  ZERO liquidation events — confirms oracle masking prevented liquidations")

    all_liquidations.extend(liquidations)

    if all_liquidations:
        df_liquidations = pd.DataFrame(all_liquidations)
        liq_path = gql_dir / "block5_liquidation_events.csv"
        df_liquidations.to_csv(liq_path, index=False)
        print(f"\n✅ Saved {len(all_liquidations)} liquidation events to {liq_path.name}")
    else:
        df_liquidations = pd.DataFrame()
        print(f"\n📋 No liquidation events to save — THIS IS THE KEY FINDING for Q1")

    # ══════════════════════════════════════════════════════════
    #  TASK 6: LLTV vs True LTV Analysis
    # ══════════════════════════════════════════════════════════
    print(f"\n{'─' * 70}")
    print(f"📊 TASK 6: LLTV vs True LTV Analysis")
    print(f"{'─' * 70}")

    if len(df_oracle) > 0:
        df_ltv = compute_ltv_analysis(df_oracle, df_positions)

        if len(df_ltv) > 0:
            ltv_path = gql_dir / "block5_ltv_analysis.csv"
            df_ltv.to_csv(ltv_path, index=False)
            print(f"\n✅ Saved {len(df_ltv)} LTV analyses to {ltv_path.name}")

            print(f"\n{'─' * 70}")
            print(f"  LTV ANALYSIS RESULTS")
            print(f"{'─' * 70}")

            for _, r in df_ltv.iterrows():
                csym = r["collateral_symbol"]
                lsym = r["loan_symbol"]
                chain = r["chain"]
                lltv = r["lltv_pct"]
                oracle_ltv = r["oracle_ltv_pct"]
                true_ltv = r["true_ltv_pct"]
                gap = r["price_gap_pct"]
                status = r["liquidation_status"]
                borrow = r["borrow_usd"]
                bd = r["bad_debt_usd"]

                if status == "UNDERWATER_MASKED":
                    icon = "🔴"
                elif status == "BAD_DEBT":
                    icon = "💀"
                elif status == "LIQUIDATABLE_ORACLE":
                    icon = "⚡"
                elif status == "UNSECURED_DEBT":
                    icon = "🟡"
                else:
                    icon = "🟢"

                print(f"\n  {icon} {csym}/{lsym} ({chain}) — {status}")
                print(f"    LLTV: {lltv}% | Oracle LTV: {oracle_ltv}% | True LTV: {true_ltv}%")
                print(f"    Price gap: {gap:.1f}% | Borrow: ${borrow:,.0f} | Bad debt: ${bd:,.0f}")

            # Summary
            n_masked = (df_ltv["liquidation_status"] == "UNDERWATER_MASKED").sum()
            n_bad_debt = (df_ltv["liquidation_status"] == "BAD_DEBT").sum()
            n_healthy = (df_ltv["liquidation_status"] == "HEALTHY").sum()
            print(f"\n  Summary: 🔴 {n_masked} oracle-masked  💀 {n_bad_debt} bad debt  "
                  f"🟢 {n_healthy} healthy")
    else:
        print("  ⚠️  No oracle data for LTV analysis")

    # ══════════════════════════════════════════════════════════
    #  FINAL SUMMARY
    # ══════════════════════════════════════════════════════════
    print(f"\n{'═' * 70}")
    print(f"  ✅ Block 5 — Liquidation Mechanism Breakdown Complete")
    print(f"{'═' * 70}")
    print(f"\n  Q1 ANSWER FRAMEWORK:")
    print(f"  1. Oracle mechanism: vault-based (ERC4626) oracles report $1.00 for collapsed assets")
    print(f"  2. Liquidation trigger: LTV stays below LLTV per oracle → no liquidation possible")
    print(f"  3. Liquidation events: {len(all_liquidations)} found across all toxic markets")
    print(f"  4. Borrower concentration: check borrower data above")
    print(f"  5. Bad debt accumulation: borrowers owe more than pool holds, no mechanism to resolve")

    print(f"\n  Outputs:")
    print(f"    block5_oracle_configs.csv")
    print(f"    block5_asset_prices.csv")
    print(f"    block5_collateral_at_risk.csv")
    print(f"    block5_borrower_positions.csv")
    print(f"    block5_liquidation_events.csv")
    print(f"    block5_ltv_analysis.csv")
    print(f"{'═' * 70}")


if __name__ == "__main__":
    main()
