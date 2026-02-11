"""
Block 2B: Market-Level Bad Debt & Oracle Data Query
=====================================================
Queries each toxic market for:
  - badDebt (unrealized) + realizedBadDebt (socialized) 
  - Full oracle architecture (oracle.data with feed addresses, vault conversions)
  - Market warnings (BadDebtUnrealizedMarketWarningMetadata)
  - Current state (supply, borrow, collateral, utilization, oracle price, liquidity)

Input:  04-data-exports/raw/graphql/block1_markets_graphql.csv  (or inline discovery)
        04-data-exports/raw/graphql/block1_vaults_graphql.csv   (fallback for market keys)
Output: 04-data-exports/raw/graphql/block2_bad_debt_by_market.csv

Depends on: Block 1 (market discovery)
"""

import os
import sys
import json
import time
import requests
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

# ── Paths ──
PROJECT_ROOT = Path(__file__).parent.parent
env_path = PROJECT_ROOT / '.env'

GRAPHQL_URL = "https://blue-api.morpho.org/graphql"

CHAIN_IDS = {
    'ethereum': 1,
    'base': 8453,
    'arbitrum': 42161,
    'optimism': 10,
    'plume': 98866,
    'unichain': 130,
    'polygon': 137,
    'hyperevm': 999,
}
CHAIN_NAMES = {v: k for k, v in CHAIN_IDS.items()}

TOXIC_COLLATERAL = ['xUSD', 'XUSD', 'deUSD', 'sdeUSD', 'deusd']
FALSE_POSITIVES = [
    'AA_FalconXUSDC', 'stakedao-crvfrxUSD', 'crvfrxUSD', 'sfrxUSD', 'fxUSD',
]

API_DELAY = 0.5  # seconds between API calls


def query_graphql(query: str) -> dict:
    """Execute GraphQL query against Morpho API"""
    headers = {"Content-Type": "application/json"}
    response = requests.post(GRAPHQL_URL, json={"query": query}, headers=headers)
    response.raise_for_status()
    return response.json()


# ══════════════════════════════════════════════════════════════════════
#  PHASE 0: Load toxic market keys
# ══════════════════════════════════════════════════════════════════════

def load_toxic_markets() -> Dict[str, Dict]:
    """
    Load toxic market uniqueKeys from available CSVs.
    Priority: block1_markets_graphql.csv > block1_vaults_graphql.csv (extract unique markets)
    Returns dict: { uniqueKey: { chain_id, chain, collateral_symbol, loan_symbol } }
    """
    data_dir = PROJECT_ROOT / "data"

    # Try block1_markets_graphql.csv first
    markets_csv = data_dir / "block1_markets_graphql.csv"
    if markets_csv.exists():
        df = pd.read_csv(markets_csv)
        print(f"\n📂 Loaded {len(df)} toxic markets from {markets_csv.name}")
        toxic_keys = {}
        chain_col = "blockchain" if "blockchain" in df.columns else "chain"
        for _, row in df.iterrows():
            key = row.get("market_id", "")
            if not key:
                continue
            toxic_keys[key] = {
                "chain_id": int(row.get("chain_id", 1)),
                "chain": row.get(chain_col, "ethereum"),
                "collateral_symbol": row.get("collateral_symbol", "?"),
                "loan_symbol": row.get("loan_symbol", "?"),
            }
        return toxic_keys

    # Fallback: extract unique market keys from block1_vaults_graphql.csv
    vaults_csv = data_dir / "block1_vaults_graphql.csv"
    if vaults_csv.exists():
        df = pd.read_csv(vaults_csv)
        print(f"\n📂 Extracting toxic markets from {vaults_csv.name}")
        toxic_keys = {}
        for _, row in df.iterrows():
            key = row.get("market_id", "")
            if not key or key in toxic_keys:
                continue
            collat = row.get("collateral_symbol", "")
            if collat in TOXIC_COLLATERAL:
                toxic_keys[key] = {
                    "chain_id": int(row.get("chain_id", 1)),
                    "chain": row.get("chain", "ethereum"),
                    "collateral_symbol": collat,
                    "loan_symbol": row.get("loan_symbol", "?"),
                }
        print(f"   Extracted {len(toxic_keys)} unique toxic markets")
        return toxic_keys

    # Last resort: discover inline
    print("\n⚠️  No CSV found — discovering toxic markets inline...")
    toxic_keys = {}
    for chain_name, chain_id in CHAIN_IDS.items():
        query = f"""
        {{
          markets(first: 500, where: {{ chainId_in: [{chain_id}] }}) {{
            items {{
              uniqueKey
              collateralAsset {{ symbol }}
              loanAsset {{ symbol }}
            }}
          }}
        }}
        """
        try:
            result = query_graphql(query)
            items = result.get("data", {}).get("markets", {}).get("items", [])
            for m in items:
                collat = (m.get("collateralAsset") or {}).get("symbol", "")
                if collat in TOXIC_COLLATERAL and collat not in FALSE_POSITIVES:
                    toxic_keys[m["uniqueKey"]] = {
                        "chain_id": chain_id,
                        "chain": chain_name,
                        "collateral_symbol": collat,
                        "loan_symbol": (m.get("loanAsset") or {}).get("symbol", ""),
                    }
            time.sleep(API_DELAY)
        except Exception as e:
            print(f"   ❌ Error querying {chain_name}: {e}")

    print(f"   Found {len(toxic_keys)} toxic markets inline")
    return toxic_keys


# ══════════════════════════════════════════════════════════════════════
#  PHASE 1: Query each market for bad debt + oracle + state
# ══════════════════════════════════════════════════════════════════════

MARKET_QUERY_TEMPLATE = """
{{
  marketByUniqueKey(uniqueKey: "{unique_key}", chainId: {chain_id}) {{
    uniqueKey
    lltv
    creationBlockNumber
    creationTimestamp
    listed

    loanAsset {{
      symbol
      address
      decimals
      priceUsd
    }}
    collateralAsset {{
      symbol
      address
      decimals
      priceUsd
    }}

    # ── Layer 2: Protocol bad debt classification ──
    badDebt {{
      underlying
      usd
    }}
    realizedBadDebt {{
      underlying
      usd
    }}

    # ── Warnings (includes BadDebtUnrealized metadata) ──
    warnings {{
      type
      level
      metadata {{
        ... on BadDebtUnrealizedMarketWarningMetadata {{
          badDebtUsd
          badDebtAssets
          totalSupplyAssets
          badDebtShare
        }}
      }}
    }}

    # ── Oracle: full architecture ──
    oracle {{
      address
      type
      data {{
        ... on MorphoChainlinkOracleV2Data {{
          baseFeedOne {{
            address
            description
            vendor
          }}
          baseFeedTwo {{
            address
            description
            vendor
          }}
          quoteFeedOne {{
            address
            description
            vendor
          }}
          quoteFeedTwo {{
            address
            description
            vendor
          }}
          baseOracleVault {{
            address
            vendor
          }}
          quoteOracleVault {{
            address
            vendor
          }}
          scaleFactor
          baseVaultConversionSample
          quoteVaultConversionSample
        }}
        ... on MorphoChainlinkOracleData {{
          baseFeedOne {{
            address
            description
            vendor
          }}
          baseFeedTwo {{
            address
            description
            vendor
          }}
          quoteFeedOne {{
            address
            description
            vendor
          }}
          quoteFeedTwo {{
            address
            description
            vendor
          }}
          baseOracleVault {{
            address
            vendor
          }}
          scaleFactor
          vaultConversionSample
        }}
      }}
    }}

    # ── Deprecated but useful oracle shortcuts ──
    oracleFeed {{
      baseFeedOneAddress
      baseFeedOneDescription
      baseFeedOneVendor
      baseFeedTwoAddress
      baseFeedTwoDescription
      baseFeedTwoVendor
      baseVault
      baseVaultDescription
      baseVaultVendor
      baseVaultConversionSample
      quoteFeedOneAddress
      quoteFeedOneDescription
      quoteFeedOneVendor
      quoteFeedTwoAddress
      quoteFeedTwoDescription
      quoteFeedTwoVendor
      quoteVault
      quoteVaultDescription
      quoteVaultVendor
      quoteVaultConversionSample
      scaleFactor
    }}

    # ── Layer 1 + Layer 3: Market state ──
    state {{
      supplyAssets
      supplyAssetsUsd
      borrowAssets
      borrowAssetsUsd
      collateralAssets
      collateralAssetsUsd
      utilization
      price
      liquidityAssets
      liquidityAssetsUsd
      supplyShares
      borrowShares
      fee
      timestamp
      rateAtUTarget
    }}
  }}
}}
"""

ZERO_ADDR = "0x0000000000000000000000000000000000000000"


def safe_float(val, default=0.0):
    """Convert to float, handling None and string values"""
    if val is None:
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def safe_str(val, default=""):
    """Convert to string, handling None"""
    if val is None:
        return default
    return str(val)


def classify_oracle_architecture(oracle_data: Dict) -> str:
    """
    Classify oracle architecture based on feed and vault addresses.
    Returns: 'feed+vault', 'feed-based', 'vault-based', 'fixed-price', or 'unknown'
    """
    bf1 = safe_str(oracle_data.get("base_feed_one_addr"))
    bf2 = safe_str(oracle_data.get("base_feed_two_addr"))
    bv = safe_str(oracle_data.get("base_oracle_vault_addr"))
    qf1 = safe_str(oracle_data.get("quote_feed_one_addr"))
    qv = safe_str(oracle_data.get("quote_oracle_vault_addr"))

    has_feed = any(
        addr not in ("", "nan", ZERO_ADDR)
        for addr in [bf1, bf2, qf1]
    )
    has_vault = any(
        addr not in ("", "nan", ZERO_ADDR)
        for addr in [bv, qv]
    )

    if has_feed and has_vault:
        return "feed+vault"
    if has_feed:
        return "feed-based"
    if has_vault:
        return "vault-based"
    return "fixed-price"


def is_hardcoded_oracle(oracle_data: Dict) -> bool:
    """
    An oracle is considered hardcoded if it has NO feed addresses AND NO vault addresses.
    This means it returns a constant price regardless of market conditions.
    """
    bf1 = safe_str(oracle_data.get("base_feed_one_addr"))
    bf2 = safe_str(oracle_data.get("base_feed_two_addr"))
    qf1 = safe_str(oracle_data.get("quote_feed_one_addr"))
    qf2 = safe_str(oracle_data.get("quote_feed_two_addr"))
    bv = safe_str(oracle_data.get("base_oracle_vault_addr"))
    qv = safe_str(oracle_data.get("quote_oracle_vault_addr"))

    all_addrs = [bf1, bf2, qf1, qf2, bv, qv]
    non_zero = [a for a in all_addrs if a not in ("", "nan", ZERO_ADDR)]
    return len(non_zero) == 0


def extract_oracle_data(oracle: Optional[Dict]) -> Dict:
    """Extract flat oracle data from nested GraphQL response"""
    result = {
        "oracle_address": "",
        "oracle_type": "",
        "base_feed_one_addr": "",
        "base_feed_one_desc": "",
        "base_feed_one_vendor": "",
        "base_feed_two_addr": "",
        "base_feed_two_desc": "",
        "base_feed_two_vendor": "",
        "quote_feed_one_addr": "",
        "quote_feed_one_desc": "",
        "quote_feed_one_vendor": "",
        "quote_feed_two_addr": "",
        "quote_feed_two_desc": "",
        "quote_feed_two_vendor": "",
        "base_oracle_vault_addr": "",
        "base_oracle_vault_vendor": "",
        "quote_oracle_vault_addr": "",
        "quote_oracle_vault_vendor": "",
        "oracle_scale_factor": "",
        "base_vault_conversion_sample": "",
        "quote_vault_conversion_sample": "",
    }

    if not oracle:
        return result

    result["oracle_address"] = safe_str(oracle.get("address"))
    result["oracle_type"] = safe_str(oracle.get("type"))

    data = oracle.get("data")
    if not data:
        return result

    # Extract feed addresses + descriptions
    def extract_feed(feed_obj):
        if not feed_obj:
            return "", "", ""
        return (
            safe_str(feed_obj.get("address")),
            safe_str(feed_obj.get("description")),
            safe_str(feed_obj.get("vendor")),
        )

    def extract_vault(vault_obj):
        if not vault_obj:
            return "", ""
        return (
            safe_str(vault_obj.get("address")),
            safe_str(vault_obj.get("vendor")),
        )

    bf1_addr, bf1_desc, bf1_vendor = extract_feed(data.get("baseFeedOne"))
    bf2_addr, bf2_desc, bf2_vendor = extract_feed(data.get("baseFeedTwo"))
    qf1_addr, qf1_desc, qf1_vendor = extract_feed(data.get("quoteFeedOne"))
    qf2_addr, qf2_desc, qf2_vendor = extract_feed(data.get("quoteFeedTwo"))
    bv_addr, bv_vendor = extract_vault(data.get("baseOracleVault"))
    qv_addr, qv_vendor = extract_vault(data.get("quoteOracleVault"))

    result.update({
        "base_feed_one_addr": bf1_addr,
        "base_feed_one_desc": bf1_desc,
        "base_feed_one_vendor": bf1_vendor,
        "base_feed_two_addr": bf2_addr,
        "base_feed_two_desc": bf2_desc,
        "base_feed_two_vendor": bf2_vendor,
        "quote_feed_one_addr": qf1_addr,
        "quote_feed_one_desc": qf1_desc,
        "quote_feed_one_vendor": qf1_vendor,
        "quote_feed_two_addr": qf2_addr,
        "quote_feed_two_desc": qf2_desc,
        "quote_feed_two_vendor": qf2_vendor,
        "base_oracle_vault_addr": bv_addr,
        "base_oracle_vault_vendor": bv_vendor,
        "quote_oracle_vault_addr": qv_addr,
        "quote_oracle_vault_vendor": qv_vendor,
        "oracle_scale_factor": safe_str(data.get("scaleFactor")),
        "base_vault_conversion_sample": safe_str(
            data.get("baseVaultConversionSample") or data.get("vaultConversionSample")
        ),
        "quote_vault_conversion_sample": safe_str(
            data.get("quoteVaultConversionSample", "")
        ),
    })

    return result


def query_market(unique_key: str, chain_id: int, market_info: Dict) -> Optional[Dict]:
    """
    Query a single market for full bad debt + oracle + state data.
    Returns flat dict ready for CSV row.
    """
    query = MARKET_QUERY_TEMPLATE.format(
        unique_key=unique_key,
        chain_id=chain_id,
    )

    try:
        result = query_graphql(query)
    except Exception as e:
        print(f"      ❌ Exception querying market: {e}")
        return None

    if "errors" in result:
        err = result["errors"][0].get("message", "")[:100]
        print(f"      ⚠️  Error: {err}")
        return None

    market = result.get("data", {}).get("marketByUniqueKey")
    if not market:
        print(f"      ⚠️  No data returned")
        return None

    # ── Extract assets ──
    loan = market.get("loanAsset") or {}
    collat = market.get("collateralAsset") or {}
    state = market.get("state") or {}

    # ── Layer 2: Bad debt ──
    bad_debt = market.get("badDebt") or {}
    realized_bd = market.get("realizedBadDebt") or {}

    L2_bad_debt_underlying = safe_str(bad_debt.get("underlying", "0"))
    L2_bad_debt_usd = safe_float(bad_debt.get("usd"))
    L2_realized_underlying = safe_str(realized_bd.get("underlying", "0"))
    L2_realized_usd = safe_float(realized_bd.get("usd"))
    L2_total_bad_debt_usd = L2_bad_debt_usd + L2_realized_usd

    # ── Warnings ──
    warnings = market.get("warnings") or []
    warning_types = [w.get("type", "") for w in warnings]
    warning_levels = [w.get("level", "") for w in warnings]

    # Extract BadDebtUnrealized metadata if present
    bd_warning_share = None
    bd_warning_usd = None
    bd_warning_assets = None
    bd_warning_supply = None
    for w in warnings:
        meta = w.get("metadata")
        if meta and "badDebtShare" in meta:
            bd_warning_share = safe_float(meta.get("badDebtShare"))
            bd_warning_usd = safe_float(meta.get("badDebtUsd"))
            bd_warning_assets = safe_str(meta.get("badDebtAssets"))
            bd_warning_supply = safe_str(meta.get("totalSupplyAssets"))

    # ── Oracle data ──
    oracle = market.get("oracle")
    oracle_data = extract_oracle_data(oracle)
    oracle_arch = classify_oracle_architecture(oracle_data)
    oracle_hardcoded = is_hardcoded_oracle(oracle_data)

    # Also grab oracleFeed (deprecated but has descriptions)
    oracle_feed = market.get("oracleFeed") or {}

    # ── Layer 1: Supply-borrow gap ──
    supply_usd = safe_float(state.get("supplyAssetsUsd"))
    borrow_usd = safe_float(state.get("borrowAssetsUsd"))
    L1_gap_usd = supply_usd - borrow_usd

    # ── Layer 3: Oracle price vs spot ──
    oracle_price_raw = safe_str(state.get("price", "0"))
    collateral_spot = safe_float(collat.get("priceUsd"))
    loan_spot = safe_float(loan.get("priceUsd"))

    # Oracle price normalization
    scale_factor_str = oracle_data.get("oracle_scale_factor", "1")
    try:
        scale_factor = float(scale_factor_str) if scale_factor_str else 1.0
    except (ValueError, TypeError):
        scale_factor = 1.0

    try:
        oracle_price_float = float(oracle_price_raw) if oracle_price_raw else 0.0
    except (ValueError, TypeError):
        oracle_price_float = 0.0

    oracle_price_normalized = oracle_price_float / scale_factor if scale_factor > 0 else 0.0

    # LTV calculations
    lltv_raw = safe_str(market.get("lltv", "0"))
    try:
        lltv_pct = float(lltv_raw) / 1e18 * 100 if float(lltv_raw) > 1e10 else float(lltv_raw) * 100
    except:
        lltv_pct = 0.0

    collateral_usd = safe_float(state.get("collateralAssetsUsd"))
    if collateral_usd > 0 and borrow_usd > 0:
        true_ltv_pct = (borrow_usd / (collateral_usd if collateral_usd > 0 else 1)) * 100
    else:
        true_ltv_pct = 0.0

    utilization = safe_float(state.get("utilization"))
    liquidity_usd = safe_float(state.get("liquidityAssetsUsd"))

    # ── Build flat row ──
    row = {
        # Identity
        "market_unique_key": unique_key,
        "market_id_short": f"{unique_key[:6]}...{unique_key[-4:]}",
        "chain": market_info.get("chain", ""),
        "chain_id": chain_id,
        "collateral_symbol": collat.get("symbol", market_info.get("collateral_symbol", "")),
        "loan_symbol": loan.get("symbol", market_info.get("loan_symbol", "")),
        "collateral_address": collat.get("address", ""),
        "loan_address": loan.get("address", ""),
        "collateral_decimals": collat.get("decimals"),
        "loan_decimals": loan.get("decimals"),
        "lltv_raw": lltv_raw,
        "lltv_pct": round(lltv_pct, 2),
        "listed": market.get("listed"),
        "creation_block": market.get("creationBlockNumber"),
        "creation_timestamp": market.get("creationTimestamp"),

        # Layer 1: Supply-Borrow Gap
        "L1_supply_assets": safe_str(state.get("supplyAssets", "0")),
        "L1_supply_usd": round(supply_usd, 2),
        "L1_borrow_assets": safe_str(state.get("borrowAssets", "0")),
        "L1_borrow_usd": round(borrow_usd, 2),
        "L1_gap_usd": round(L1_gap_usd, 2),
        "L1_collateral_assets": safe_str(state.get("collateralAssets", "0")),
        "L1_collateral_usd": round(collateral_usd, 2),

        # Layer 2: Bad Debt (unrealized + realized)
        "L2_bad_debt_underlying": L2_bad_debt_underlying,
        "L2_bad_debt_usd": round(L2_bad_debt_usd, 2),
        "L2_realized_bad_debt_underlying": L2_realized_underlying,
        "L2_realized_bad_debt_usd": round(L2_realized_usd, 2),
        "L2_total_bad_debt_usd": round(L2_total_bad_debt_usd, 2),

        # Warnings
        "warning_types": "|".join(warning_types) if warning_types else "",
        "warning_levels": "|".join(warning_levels) if warning_levels else "",
        "warning_bad_debt_share": bd_warning_share,
        "warning_bad_debt_usd": bd_warning_usd,
        "warning_bad_debt_assets": bd_warning_assets,
        "warning_total_supply_assets": bd_warning_supply,

        # Layer 3: Oracle Price
        "L3_oracle_price_raw": oracle_price_raw,
        "L3_oracle_price_normalized": round(oracle_price_normalized, 8),
        "L3_oracle_scale_factor": scale_factor_str,
        "collateral_spot_price": round(collateral_spot, 6),
        "loan_spot_price": round(loan_spot, 6),
        "L3_oracle_spot_gap_pct": round(
            ((oracle_price_normalized - collateral_spot) / collateral_spot * 100)
            if collateral_spot > 0 else 0, 2),
        "L3_oracle_spot_gap_usd": round(oracle_price_normalized - collateral_spot, 6)
            if collateral_spot > 0 else 0,

        # LTV
        "lltv_pct_display": round(lltv_pct, 1),
        "oracle_ltv_pct": round(
            (borrow_usd / (collateral_usd * oracle_price_normalized / collateral_spot) * 100)
            if collateral_usd > 0 and collateral_spot > 0 and oracle_price_normalized > 0
            else 0, 1),
        "true_ltv_pct": round(true_ltv_pct, 1),

        # Market state
        "supply_usd": round(supply_usd, 2),
        "borrow_usd": round(borrow_usd, 2),
        "utilization": round(utilization, 6),
        "liquidity_assets": safe_str(state.get("liquidityAssets", "0")),
        "liquidity_usd": round(liquidity_usd, 2),

        # Oracle architecture
        "oracle_address": oracle_data["oracle_address"],
        "oracle_type": oracle_data["oracle_type"],
        "oracle_is_hardcoded": oracle_hardcoded,
        "oracle_architecture": oracle_arch,

        # Oracle feed details
        "oracle_base_feed_one": oracle_data["base_feed_one_addr"],
        "oracle_base_feed_one_desc": oracle_data["base_feed_one_desc"],
        "oracle_base_feed_one_vendor": oracle_data["base_feed_one_vendor"],
        "oracle_base_feed_two": oracle_data["base_feed_two_addr"],
        "oracle_base_feed_two_desc": oracle_data["base_feed_two_desc"],
        "oracle_quote_feed_one": oracle_data["quote_feed_one_addr"],
        "oracle_quote_feed_one_desc": oracle_data["quote_feed_one_desc"],
        "oracle_quote_feed_two": oracle_data["quote_feed_two_addr"],
        "oracle_quote_feed_two_desc": oracle_data["quote_feed_two_desc"],

        # Oracle vault details
        "oracle_base_vault": oracle_data["base_oracle_vault_addr"],
        "oracle_base_vault_vendor": oracle_data["base_oracle_vault_vendor"],
        "oracle_quote_vault": oracle_data["quote_oracle_vault_addr"],
        "oracle_quote_vault_vendor": oracle_data["quote_oracle_vault_vendor"],
        "oracle_base_vault_conversion": oracle_data["base_vault_conversion_sample"],
        "oracle_quote_vault_conversion": oracle_data["quote_vault_conversion_sample"],
        "oracle_scale_factor": scale_factor_str,

        # Legacy oracleFeed (deprecated but descriptive)
        "feed_base_one_desc": safe_str(oracle_feed.get("baseFeedOneDescription")),
        "feed_base_two_desc": safe_str(oracle_feed.get("baseFeedTwoDescription")),
        "feed_quote_one_desc": safe_str(oracle_feed.get("quoteFeedOneDescription")),
        "feed_quote_two_desc": safe_str(oracle_feed.get("quoteFeedTwoDescription")),
        "feed_base_vault_desc": safe_str(oracle_feed.get("baseVaultDescription")),
        "feed_quote_vault_desc": safe_str(oracle_feed.get("quoteVaultDescription")),
    }

    return row


# ══════════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════════

def main():
    print("=" * 70)
    print("BLOCK 2B: Market-Level Bad Debt & Oracle Query")
    print("=" * 70)

    # ── Load toxic markets ──
    toxic_keys = load_toxic_markets()
    if not toxic_keys:
        print("❌ No toxic markets found. Run Block 1 first.")
        return

    print(f"\n📊 Querying {len(toxic_keys)} toxic markets for bad debt + oracle data...")
    print(f"{'─' * 60}")

    rows = []
    for i, (unique_key, info) in enumerate(toxic_keys.items(), 1):
        chain_id = info["chain_id"]
        chain = info["chain"]
        collat = info["collateral_symbol"]
        loan = info["loan_symbol"]

        print(f"\n[{i}/{len(toxic_keys)}] {collat}/{loan} ({chain})")
        print(f"   Key: {unique_key[:10]}...{unique_key[-6:]}")

        row = query_market(unique_key, chain_id, info)
        if row:
            rows.append(row)
            # Print key findings
            bd_usd = row["L2_bad_debt_usd"]
            rbd_usd = row["L2_realized_bad_debt_usd"]
            total_bd = row["L2_total_bad_debt_usd"]
            oracle_type = row["oracle_type"]
            oracle_arch = row["oracle_architecture"]
            hardcoded = row["oracle_is_hardcoded"]
            util = row["utilization"]

            print(f"   ✅ Bad debt: ${bd_usd:,.0f} unrealized + ${rbd_usd:,.0f} realized = ${total_bd:,.0f} total")
            print(f"   🔮 Oracle: {oracle_type} | arch: {oracle_arch} | hardcoded: {hardcoded}")
            print(f"   📊 Utilization: {util:.1%} | Supply: ${row['supply_usd']:,.0f} | Borrow: ${row['borrow_usd']:,.0f}")

            if row.get("warning_types"):
                print(f"   ⚠️  Warnings: {row['warning_types']}")
            if row.get("warning_bad_debt_share"):
                print(f"   📉 Bad debt share: {row['warning_bad_debt_share']:.2%}")

            # Oracle detail summary
            feed_desc = row.get("oracle_base_feed_one_desc") or row.get("feed_base_one_desc")
            vault_desc = row.get("oracle_base_vault_vendor") or row.get("feed_base_vault_desc")
            if feed_desc:
                print(f"   🔗 Base feed: {feed_desc}")
            if vault_desc:
                print(f"   🏦 Base vault: {vault_desc}")
        else:
            print(f"   ❌ Failed to query")

        time.sleep(API_DELAY)

    # ── Save CSV ──
    if not rows:
        print("\n❌ No market data collected")
        return

    df = pd.DataFrame(rows)
    output_dir = PROJECT_ROOT / "data"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "block2_bad_debt_by_market.csv"
    df.to_csv(output_path, index=False)
    print(f"\n✅ Saved {len(df)} markets to {output_path.name}")

    # ── Summary ──
    print(f"\n{'═' * 70}")
    print(f"SUMMARY")
    print(f"{'═' * 70}")

    total_unrealized = df["L2_bad_debt_usd"].sum()
    total_realized = df["L2_realized_bad_debt_usd"].sum()
    total_combined = df["L2_total_bad_debt_usd"].sum()

    print(f"\n💰 Bad Debt Totals:")
    print(f"   Unrealized (badDebt):        ${total_unrealized:>12,.2f}")
    print(f"   Realized (realizedBadDebt):   ${total_realized:>12,.2f}")
    print(f"   Combined:                     ${total_combined:>12,.2f}")

    print(f"\n🔮 Oracle Architecture:")
    for arch, count in df["oracle_architecture"].value_counts().items():
        mkt_list = df[df["oracle_architecture"] == arch]["market_id_short"].tolist()
        print(f"   {arch}: {count} markets — {', '.join(mkt_list)}")

    print(f"\n🔒 Hardcoded Oracles:")
    hc = df[df["oracle_is_hardcoded"] == True]
    nhc = df[df["oracle_is_hardcoded"] == False]
    print(f"   Hardcoded (no feeds, no vaults): {len(hc)} markets")
    print(f"   Dynamic (has feeds/vaults):       {len(nhc)} markets")

    if len(hc) > 0:
        print(f"\n   Hardcoded markets:")
        for _, r in hc.iterrows():
            print(f"     {r['collateral_symbol']}/{r['loan_symbol']} ({r['chain']}) — "
                  f"oracle: {r['oracle_type']}, BD: ${r['L2_total_bad_debt_usd']:,.0f}")

    print(f"\n⚠️  Warnings:")
    warned = df[df["warning_types"].str.len() > 0]
    if len(warned) > 0:
        for _, r in warned.iterrows():
            print(f"   {r['collateral_symbol']}/{r['loan_symbol']} ({r['chain']}): "
                  f"{r['warning_types']} [{r['warning_levels']}]")
            if r.get("warning_bad_debt_share"):
                print(f"     Bad debt share: {r['warning_bad_debt_share']:.2%}")
    else:
        print(f"   No warnings on any market")

    # Oracle feed descriptions (for human understanding)
    print(f"\n📋 Oracle Feed Descriptions:")
    for _, r in df.iterrows():
        label = f"{r['collateral_symbol']}/{r['loan_symbol']} ({r['chain']})"
        print(f"\n   {label}:")
        print(f"     Type: {r['oracle_type']}")
        print(f"     Architecture: {r['oracle_architecture']}")
        print(f"     Hardcoded: {r['oracle_is_hardcoded']}")
        bf1 = r.get("oracle_base_feed_one_desc") or r.get("feed_base_one_desc")
        bf2 = r.get("oracle_base_feed_two_desc") or r.get("feed_base_two_desc")
        qf1 = r.get("oracle_quote_feed_one_desc") or r.get("feed_quote_one_desc")
        qf2 = r.get("oracle_quote_feed_two_desc") or r.get("feed_quote_two_desc")
        bv = r.get("oracle_base_vault_vendor") or r.get("feed_base_vault_desc")
        qv = r.get("oracle_quote_vault_vendor") or r.get("feed_quote_vault_desc")

        if bf1: print(f"     Base feed 1: {bf1}")
        if bf2: print(f"     Base feed 2: {bf2}")
        if qf1: print(f"     Quote feed 1: {qf1}")
        if qf2: print(f"     Quote feed 2: {qf2}")
        if bv:  print(f"     Base vault:   {bv}")
        if qv:  print(f"     Quote vault:  {qv}")

        price_norm = r.get("L3_oracle_price_normalized", 0)
        spot = r.get("collateral_spot_price", 0)
        print(f"     Oracle price (normalized): {price_norm}")
        print(f"     Collateral spot price:     ${spot}")
        if spot > 0:
            gap_pct = (price_norm - spot) / spot * 100
            print(f"     Gap: {gap_pct:+.1f}%")

    print(f"\n{'═' * 70}")
    print(f"✅ Block 2B complete — {len(df)} markets saved to {output_path.name}")
    print(f"{'═' * 70}")


if __name__ == "__main__":
    main()
