import os
import sys
import requests
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from typing import List, Dict

# Load .env from project root
# Script lives at: 03-queries/block1-exposure/graphsql/script.py → 4 levels to /app/
PROJECT_ROOT = Path(__file__).parent.parent
env_path = PROJECT_ROOT / '.env'
load_dotenv(dotenv_path=env_path)

GRAPHQL_URL = "https://blue-api.morpho.org/graphql"

# Chain IDs for Morpho GraphQL API
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

# Toxic collateral — filter by symbol (primary) and exclude false positives
TOXIC_COLLATERAL = ['xUSD', 'XUSD', 'deUSD', 'sdeUSD', 'deusd']
FALSE_POSITIVES = [
    'AA_FalconXUSDC', 'stakedao-crvfrxUSD', 'crvfrxUSD', 'sfrxUSD', 'fxUSD',
]


def query_graphql(query: str) -> dict:
    """Execute GraphQL query against Morpho API"""
    headers = {"Content-Type": "application/json"}
    response = requests.post(GRAPHQL_URL, json={"query": query}, headers=headers)
    response.raise_for_status()
    return response.json()


def fetch_all_markets(chain_name: str, chain_id: int) -> List[Dict]:
    """
    Fetch all markets for a given chain using pagination.
    Uses 'first' and 'skip' for pagination per the schema.
    """
    print(f"\n🔍 Querying {chain_name} (chainId={chain_id})...")

    all_markets = []
    skip = 0
    page_size = 100
    page = 1

    while True:
        query = f"""
        {{
          markets(
            first: {page_size}
            skip: {skip}
            where: {{
              chainId_in: [{chain_id}]
            }}
          ) {{
            items {{
              uniqueKey
              listed
              creationTimestamp
              lltv
              irmAddress
              loanAsset {{
                address
                symbol
                name
                decimals
                priceUsd
              }}
              collateralAsset {{
                address
                symbol
                name
                decimals
                priceUsd
              }}
              oracle {{
                address
                type
              }}
              morphoBlue {{
                address
                chain {{
                  id
                  network
                }}
              }}
              state {{
                timestamp
                blockNumber
                supplyAssets
                supplyShares
                borrowAssets
                borrowShares
                supplyAssetsUsd
                borrowAssetsUsd
                collateralAssets
                collateralAssetsUsd
                liquidityAssets
                liquidityAssetsUsd
                utilization
                price
                supplyApy
                borrowApy
                netSupplyApy
                netBorrowApy
                fee
              }}
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
              badDebt {{
                underlying
                usd
              }}
              realizedBadDebt {{
                underlying
                usd
              }}
              supplyingVaults {{
                address
                name
              }}
            }}
            pageInfo {{
              count
              countTotal
              limit
              skip
            }}
          }}
        }}
        """

        try:
            result = query_graphql(query)

            if "errors" in result:
                error_msg = result['errors'][0].get('message', 'Unknown error')
                print(f"   ❌ GraphQL Error on page {page}: {error_msg}")
                print(f"   Error details: {result['errors']}")
                break

            data = result.get("data", {})
            if not data or "markets" not in data:
                print(f"   ❌ No data returned on page {page}")
                break

            markets_data = data.get("markets", {})
            items = markets_data.get("items", [])
            page_info = markets_data.get("pageInfo", {})

            if not items:
                break

            all_markets.extend(items)

            count_total = page_info.get("countTotal", 0)
            print(f"   📄 Page {page}: {len(items)} markets (total on chain: {count_total})")

            # Check if we've fetched everything
            if skip + len(items) >= count_total:
                break

            skip += page_size
            page += 1

        except Exception as e:
            print(f"   ❌ Exception on page {page}: {e}")
            import traceback
            traceback.print_exc()
            break

    print(f"   ✅ Fetched {len(all_markets)} total markets on {chain_name}")
    return all_markets


def filter_toxic_markets(markets: List[Dict]) -> List[Dict]:
    """Filter markets that have toxic collateral, excluding false positives"""
    toxic_markets = []

    for market in markets:
        collateral = market.get("collateralAsset")
        if not collateral:
            continue

        symbol = collateral.get("symbol", "")

        # Skip false positives
        if symbol in FALSE_POSITIVES:
            continue

        if symbol in TOXIC_COLLATERAL:
            toxic_markets.append(market)

    return toxic_markets


def safe_float(val, default=0.0):
    """Safely convert a value to float"""
    if val is None:
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def safe_int(val, default=0):
    """Safely convert a value to int"""
    if val is None:
        return default
    try:
        return int(val)
    except (ValueError, TypeError):
        return default


def parse_market_data(market: Dict) -> Dict:
    """Parse market data into flat dictionary for DataFrame — all fields for Blocks 2-6"""
    state = market.get("state") or {}
    collateral = market.get("collateralAsset") or {}
    loan = market.get("loanAsset") or {}
    chain = (market.get("morphoBlue") or {}).get("chain") or {}
    bad_debt = market.get("badDebt") or {}
    realized_bad_debt = market.get("realizedBadDebt") or {}
    oracle = market.get("oracle") or {}

    # --- Warnings: extract types, levels, and bad-debt metadata ---
    warnings = market.get("warnings") or []
    warning_types = [w.get("type") for w in warnings] if warnings else []
    warning_levels = [w.get("level") for w in warnings] if warnings else []

    # Extract BadDebtUnrealized warning metadata (if present)
    warn_bad_debt_usd = None
    warn_bad_debt_share = None
    for w in warnings:
        meta = w.get("metadata")
        if meta and "badDebtShare" in meta:
            warn_bad_debt_usd = safe_float(meta.get("badDebtUsd"))
            warn_bad_debt_share = safe_float(meta.get("badDebtShare"))

    # --- Raw BigInt values (oracle-independent, for supply-borrow gap) ---
    supply_assets_raw = safe_int(state.get("supplyAssets"))
    borrow_assets_raw = safe_int(state.get("borrowAssets"))
    collateral_assets_raw = safe_int(state.get("collateralAssets"))
    liquidity_assets_raw = safe_int(state.get("liquidityAssets"))

    # --- USD values ---
    supply_usd = safe_float(state.get("supplyAssetsUsd"))
    borrow_usd = safe_float(state.get("borrowAssetsUsd"))
    collat_usd = safe_float(state.get("collateralAssetsUsd"))
    liquidity_usd = safe_float(state.get("liquidityAssetsUsd"))
    utilization = safe_float(state.get("utilization"))

    # --- Prices ---
    collateral_spot_price = safe_float(collateral.get("priceUsd"))
    loan_spot_price = safe_float(loan.get("priceUsd"))
    oracle_price_raw = safe_int(state.get("price"))  # on-chain oracle price (BigInt, 36 decimals)

    # --- Computed: Supply-Borrow Gap (Layer 1 bad debt — oracle-independent) ---
    supply_borrow_gap_raw = supply_assets_raw - borrow_assets_raw
    supply_borrow_gap_usd = supply_usd - borrow_usd

    # --- Computed: Oracle vs spot gap (Layer 3) ---
    # oracle_price_raw is in 36-decimal fixed point; convert to ratio
    # For collateral/loan pair: oracle_price = collateral_price / loan_price
    # We compare the oracle's implied collateral price to the spot price
    oracle_spot_gap_pct = None
    if collateral_spot_price and collateral_spot_price > 0 and loan_spot_price and loan_spot_price > 0:
        # The oracle price from state.price is scaled by 1e36
        # It represents: how many loan tokens per collateral token (adjusted for decimals)
        oracle_collateral_value_usd = None
        if oracle_price_raw > 0:
            loan_decimals = safe_int(loan.get("decimals"), 18)
            collateral_decimals = safe_int(collateral.get("decimals"), 18)
            # oracle_price = (collateral_price / loan_price) * 10^(36 + loan_decimals - collateral_decimals)
            # So: implied_collateral_usd = oracle_price * loan_spot_price / 10^(36 + loan_decimals - collateral_decimals)
            scale = 10 ** (36 + loan_decimals - collateral_decimals)
            oracle_collateral_value_usd = (oracle_price_raw / scale) * loan_spot_price
            if oracle_collateral_value_usd > 0:
                oracle_spot_gap_pct = (oracle_collateral_value_usd - collateral_spot_price) / oracle_collateral_value_usd

    # --- Computed: Bad Debt Status (three-layer classification) ---
    native_bad_debt = safe_float(bad_debt.get("usd"))
    if supply_borrow_gap_raw < 0:
        bad_debt_status = "BAD_DEBT_CONFIRMED"
    elif utilization >= 0.99:
        bad_debt_status = "AT_RISK_100PCT_UTIL"
    elif oracle_spot_gap_pct is not None and oracle_spot_gap_pct > 0.10:
        bad_debt_status = "ORACLE_MISPRICING"
    elif native_bad_debt > 0 or warn_bad_debt_usd is not None:
        bad_debt_status = "BAD_DEBT_NATIVE_REPORTED"
    else:
        bad_debt_status = "HEALTHY"

    # Flag oracle masking: accounting shows bad debt but protocol reports 0
    oracle_masking = supply_borrow_gap_raw < 0 and native_bad_debt == 0

    # --- Oracle config (for Block 5.1) ---
    oracle_type = oracle.get("type")
    oracle_address = oracle.get("address")
    # NOTE: Detailed oracle feed data (baseFeedOne, scaleFactor, etc.) is NOT fetched
    # in the bulk query because some markets have null scaleFactor which crashes the API.
    # Feed details will be fetched per-market in Block 5.1 using marketByUniqueKey.

    # --- Supplying vaults count ---
    supplying_vaults = market.get("supplyingVaults") or []
    supplying_vault_count = len(supplying_vaults)
    supplying_vault_addresses = "|".join([v.get("address", "") for v in supplying_vaults]) if supplying_vaults else None

    return {
        # --- IDs & classification ---
        "market_id": market.get("uniqueKey"),
        "market_id_short": f"{market.get('uniqueKey', '')[:6]}...{market.get('uniqueKey', '')[-4:]}" if market.get('uniqueKey') else None,
        "chain": chain.get("network"),
        "chain_id": chain.get("id"),
        "listed": market.get("listed"),
        "creation_timestamp": market.get("creationTimestamp"),

        # --- Collateral asset ---
        "collateral_symbol": collateral.get("symbol"),
        "collateral_address": collateral.get("address"),
        "collateral_name": collateral.get("name"),
        "collateral_decimals": safe_int(collateral.get("decimals")),
        "collateral_price_usd": collateral_spot_price,

        # --- Loan asset ---
        "loan_symbol": loan.get("symbol"),
        "loan_address": loan.get("address"),
        "loan_name": loan.get("name"),
        "loan_decimals": safe_int(loan.get("decimals")),
        "loan_price_usd": loan_spot_price,

        # --- Market params ---
        "lltv": float(market.get("lltv", 0)) / 1e18 if market.get("lltv") else 0,
        "irm_address": market.get("irmAddress"),

        # --- Oracle config (Block 5.1 — type + address only; feeds fetched per-market later) ---
        "oracle_type": oracle_type,
        "oracle_address": oracle_address,

        # --- Raw state (oracle-independent, for Block 2.1 supply-borrow gap) ---
        "supply_assets": supply_assets_raw,
        "borrow_assets": borrow_assets_raw,
        "collateral_assets": collateral_assets_raw,
        "liquidity_assets": liquidity_assets_raw,
        "supply_shares": safe_int(state.get("supplyShares")),
        "borrow_shares": safe_int(state.get("borrowShares")),

        # --- USD state ---
        "total_supply_usd": supply_usd,
        "total_borrow_usd": borrow_usd,
        "total_collat_usd": collat_usd,
        "liquidity_usd": liquidity_usd,
        "utilization": utilization,

        # --- APYs ---
        "supply_apy": safe_float(state.get("supplyApy")),
        "borrow_apy": safe_float(state.get("borrowApy")),
        "net_supply_apy": safe_float(state.get("netSupplyApy")),
        "net_borrow_apy": safe_float(state.get("netBorrowApy")),
        "fee": safe_float(state.get("fee")),

        # --- Oracle price (Block 5.1 — the critical hardcoded $1 finding) ---
        "oracle_price_raw": oracle_price_raw,
        "oracle_collateral_value_usd": oracle_collateral_value_usd if oracle_spot_gap_pct is not None else None,
        "oracle_spot_gap_pct": oracle_spot_gap_pct,

        # --- Bad debt: Layer 2 (native protocol) ---
        "bad_debt_usd": safe_float(bad_debt.get("usd")),
        "bad_debt_underlying": safe_int(bad_debt.get("underlying")),
        "realized_bad_debt_usd": safe_float(realized_bad_debt.get("usd")),
        "realized_bad_debt_underlying": safe_int(realized_bad_debt.get("underlying")),

        # --- Bad debt: Layer 1 (supply-borrow gap, oracle-independent) ---
        "supply_borrow_gap_raw": supply_borrow_gap_raw,
        "supply_borrow_gap_usd": supply_borrow_gap_usd,

        # --- Bad debt: warning metadata ---
        "warn_bad_debt_usd": warn_bad_debt_usd,
        "warn_bad_debt_share": warn_bad_debt_share,

        # --- Computed classifications ---
        "bad_debt_status": bad_debt_status,
        "oracle_masking": oracle_masking,

        # --- Warnings ---
        "warning_types": "|".join(warning_types) if warning_types else None,
        "warning_levels": "|".join(warning_levels) if warning_levels else None,

        # --- Supplying vaults (Block 6.1 cross-ref) ---
        "supplying_vault_count": supplying_vault_count,
        "supplying_vault_addresses": supplying_vault_addresses,

        # --- Metadata ---
        "state_timestamp": state.get("timestamp") if state else None,
        "state_block_number": state.get("blockNumber") if state else None,
    }


def main():
    print("=" * 80)
    print("Block 1.1 (GraphQL): Morpho markets with toxic collateral — ENRICHED")
    print("=" * 80)

    all_markets = []

    # Query each chain
    for chain_name, chain_id in CHAIN_IDS.items():
        chain_markets = fetch_all_markets(chain_name, chain_id)
        all_markets.extend(chain_markets)

    print(f"\n📊 Total markets fetched across all chains: {len(all_markets)}")

    # Filter for toxic collateral
    print(f"\n🔍 Filtering for toxic collateral: {TOXIC_COLLATERAL}")
    print(f"   Excluding false positives: {FALSE_POSITIVES}")
    toxic_markets = filter_toxic_markets(all_markets)

    if not toxic_markets:
        print("\n⚠️  No markets found with toxic collateral")
        return

    print(f"✅ Found {len(toxic_markets)} markets with toxic collateral")

    # Parse market data
    print(f"\n📊 Parsing market data...")
    parsed_data = [parse_market_data(m) for m in toxic_markets]
    df = pd.DataFrame(parsed_data)

    # Sort by total collateral USD descending
    df = df.sort_values('total_collat_usd', ascending=False)

    # Save output
    output_dir = PROJECT_ROOT / "data"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "block1_markets_graphql.csv"

    df.to_csv(output_path, index=False)

    print(f"\n✅ Found {len(df)} affected markets")
    print(f"✅ Saved to {output_path}")

    # ---- Summary Statistics ----
    if len(df) > 0:
        print(f"\n{'─' * 60}")
        print(f"📊 SUMMARY")
        print(f"{'─' * 60}")
        print(f"   Markets found:             {len(df)}")
        print(f"   Chains:                    {df['chain'].unique().tolist()}")
        print(f"   Collateral types:          {df['collateral_symbol'].unique().tolist()}")
        print(f"   Listed markets:            {df['listed'].sum()} / {len(df)}")
        print(f"   Total collateral exposure: ${df['total_collat_usd'].sum():,.2f}")
        print(f"   Total supply:              ${df['total_supply_usd'].sum():,.2f}")
        print(f"   Total borrow:              ${df['total_borrow_usd'].sum():,.2f}")
        print(f"   Total liquidity:           ${df['liquidity_usd'].sum():,.2f}")

        # Three-layer bad debt summary
        print(f"\n{'─' * 60}")
        print(f"💀 THREE-LAYER BAD DEBT ANALYSIS")
        print(f"{'─' * 60}")

        # Layer 1: Supply-borrow gap
        gap_negative = df[df['supply_borrow_gap_usd'] < 0]
        print(f"\n   Layer 1 — Supply-Borrow Gap (oracle-independent):")
        print(f"   Markets with gap < 0:      {len(gap_negative)}")
        if len(gap_negative) > 0:
            print(f"   Total gap deficit:         ${abs(gap_negative['supply_borrow_gap_usd'].sum()):,.2f}")
            for _, row in gap_negative.iterrows():
                print(f"     {row['collateral_symbol']}/{row['loan_symbol']} ({row['chain']}): "
                      f"gap = ${row['supply_borrow_gap_usd']:,.2f}")

        # Layer 2: Native bad debt
        print(f"\n   Layer 2 — Native Protocol Bad Debt:")
        print(f"   Total bad debt (current):  ${df['bad_debt_usd'].sum():,.2f}")
        print(f"   Total realized bad debt:   ${df['realized_bad_debt_usd'].sum():,.2f}")
        markets_native_bd = df[df['bad_debt_usd'] > 0]
        if len(markets_native_bd) > 0:
            for _, row in markets_native_bd.iterrows():
                print(f"     {row['collateral_symbol']}/{row['loan_symbol']} ({row['chain']}): "
                      f"${row['bad_debt_usd']:,.2f}")

        # Layer 3: Oracle gap
        oracle_mispriced = df[df['oracle_spot_gap_pct'].notna() & (df['oracle_spot_gap_pct'] > 0.05)]
        print(f"\n   Layer 3 — Oracle vs Spot Gap (>5%):")
        print(f"   Markets mispriced:         {len(oracle_mispriced)}")
        if len(oracle_mispriced) > 0:
            for _, row in oracle_mispriced.iterrows():
                gap_str = f"{row['oracle_spot_gap_pct']*100:.1f}%" if row['oracle_spot_gap_pct'] else "N/A"
                oracle_val = row.get('oracle_collateral_value_usd')
                oracle_str = f"${oracle_val:.4f}" if oracle_val is not None else "N/A"
                print(f"     {row['collateral_symbol']}/{row['loan_symbol']} ({row['chain']}): "
                      f"gap = {gap_str}  oracle={oracle_str}  "
                      f"spot=${row['collateral_price_usd']:.4f}")

        # Oracle masking
        masking = df[df['oracle_masking'] == True]
        if len(masking) > 0:
            print(f"\n   ⚠️  ORACLE MASKING: {len(masking)} markets where Layer 1 shows bad debt but Layer 2 = $0")
            for _, row in masking.iterrows():
                print(f"     {row['collateral_symbol']}/{row['loan_symbol']} ({row['chain']}): "
                      f"gap=${row['supply_borrow_gap_usd']:,.2f}  native=$0")

        # Bad debt status summary
        print(f"\n   Status classification:")
        for status, count in df['bad_debt_status'].value_counts().items():
            status_total = df[df['bad_debt_status'] == status]['total_collat_usd'].sum()
            print(f"     {status}: {count} markets (${status_total:,.2f} collateral)")

        # Oracle types
        print(f"\n{'─' * 60}")
        print(f"🔮 ORACLE CONFIGURATION")
        print(f"{'─' * 60}")
        for _, row in df.iterrows():
            print(f"   {row['collateral_symbol']}/{row['loan_symbol']} ({row['chain']}): "
                  f"type={row['oracle_type']}  addr={row['oracle_address']}")

        # Warnings
        markets_with_warnings = df[df['warning_types'].notna()]
        if len(markets_with_warnings) > 0:
            print(f"\n{'─' * 60}")
            print(f"⚠️  WARNINGS: {len(markets_with_warnings)} markets")
            print(f"{'─' * 60}")
            for _, row in markets_with_warnings.iterrows():
                print(f"   {row['collateral_symbol']}/{row['loan_symbol']} ({row['chain']}): "
                      f"{row['warning_types']} [{row['warning_levels']}]")
                if row['warn_bad_debt_share'] is not None:
                    print(f"     → bad debt share: {row['warn_bad_debt_share']*100:.2f}%  "
                          f"bad debt USD: ${row['warn_bad_debt_usd']:,.2f}")

        # High utilization
        high_util = df[df['utilization'] > 0.9]
        if len(high_util) > 0:
            print(f"\n🚨 High utilization (>90%): {len(high_util)} markets")
            for _, row in high_util.iterrows():
                print(f"   {row['collateral_symbol']}/{row['loan_symbol']} ({row['chain']}): "
                      f"{row['utilization']*100:.1f}%  liquidity=${row['liquidity_usd']:,.2f}")

        # Top markets table
        print(f"\n{'─' * 60}")
        print(f"🔝 ALL MARKETS BY COLLATERAL")
        print(f"{'─' * 60}")
        cols = ['collateral_symbol', 'loan_symbol', 'chain', 'lltv', 'total_collat_usd',
                'total_supply_usd', 'total_borrow_usd', 'utilization', 'bad_debt_status']
        display = df[cols].copy()
        display['lltv'] = display['lltv'].apply(lambda x: f"{x*100:.0f}%")
        display['utilization'] = display['utilization'].apply(lambda x: f"{x*100:.1f}%")
        print(display.to_string(index=False))

    print(f"\n{'=' * 80}")


if __name__ == "__main__":
    main()
