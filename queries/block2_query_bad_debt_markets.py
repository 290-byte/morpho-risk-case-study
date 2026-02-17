"""
Block 2.1 — Bad Debt Quantification Per Market (Three-Layer Analysis)

Queries each toxic market individually via marketByUniqueKey to get:
  - Layer 1: Supply-Borrow Gap (oracle-independent)
  - Layer 2: Native protocol bad debt (badDebt.usd + realizedBadDebt.usd)
  - Layer 3: Oracle vs spot price gap (collateral mispricing)

Also captures detailed oracle feed config (piggybacks Block 5.1).

Input:  04-data-exports/raw/graphql/block1_markets_graphql.csv
Output: 04-data-exports/raw/graphql/block2_bad_debt_by_market.csv
"""

import time
import requests
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from typing import Dict, Optional

# Script lives at: 03-queries/block2-bad-debt/graphsql/script.py → 4 levels to /app/
PROJECT_ROOT = Path(__file__).parent.parent
env_path = PROJECT_ROOT / '.env'
load_dotenv(dotenv_path=env_path)

GRAPHQL_URL = "https://blue-api.morpho.org/graphql"

# Rate limit: 5000 req / 5 min. We're doing ~13 calls, no concern, but be polite.
REQUEST_DELAY = 0.2  # seconds between API calls


def query_graphql(query: str) -> dict:
    """Execute GraphQL query against Morpho API"""
    headers = {"Content-Type": "application/json"}
    response = requests.post(GRAPHQL_URL, json={"query": query}, headers=headers)
    response.raise_for_status()
    return response.json()


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


def fetch_market_detail(unique_key: str, chain_id: int) -> Optional[Dict]:
    """
    Fetch a single market by uniqueKey with full bad debt + oracle data.
    Uses marketByUniqueKey — safe for oracle.data since we query one at a time
    and wrap in error handling.
    """
    # First try WITH oracle.data (for Block 5.1 piggyback)
    query_with_oracle_data = f"""
    {{
      marketByUniqueKey(uniqueKey: "{unique_key}", chainId: {chain_id}) {{
        uniqueKey
        listed
        lltv
        creationTimestamp
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
          data {{
            ... on MorphoChainlinkOracleData {{
              baseFeedOne {{ address }}
              baseFeedTwo {{ address }}
              quoteFeedOne {{ address }}
              quoteFeedTwo {{ address }}
              scaleFactor
              baseOracleVault {{ address }}
              vaultConversionSample
            }}
            ... on MorphoChainlinkOracleV2Data {{
              baseFeedOne {{ address }}
              baseFeedTwo {{ address }}
              quoteFeedOne {{ address }}
              quoteFeedTwo {{ address }}
              scaleFactor
              baseOracleVault {{ address }}
              baseVaultConversionSample
              quoteOracleVault {{ address }}
              quoteVaultConversionSample
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
          fee
        }}
        supplyingVaults {{
          address
          name
        }}
      }}
    }}
    """

    # Fallback query WITHOUT oracle.data (in case scaleFactor is null)
    query_without_oracle_data = f"""
    {{
      marketByUniqueKey(uniqueKey: "{unique_key}", chainId: {chain_id}) {{
        uniqueKey
        listed
        lltv
        creationTimestamp
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
          metadata {{
            ... on BadDebtUnrealizedMarketWarningMetadata {{
              badDebtUsd
              badDebtAssets
              totalSupplyAssets
              badDebtShare
            }}
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
          fee
        }}
        supplyingVaults {{
          address
          name
        }}
      }}
    }}
    """

    # Try with oracle data first
    try:
        result = query_graphql(query_with_oracle_data)
        if "errors" not in result:
            return result.get("data", {}).get("marketByUniqueKey")
        # If oracle.data caused the error, fall back
        print(f"      ⚠️  oracle.data failed, retrying without: {result['errors'][0].get('message', '')[:80]}")
    except Exception as e:
        print(f"      ⚠️  Full query failed: {e}")

    # Fallback without oracle.data
    try:
        result = query_graphql(query_without_oracle_data)
        if "errors" in result:
            print(f"      ❌ Fallback also failed: {result['errors'][0].get('message', '')[:80]}")
            return None
        return result.get("data", {}).get("marketByUniqueKey")
    except Exception as e:
        print(f"      ❌ Fallback exception: {e}")
        return None


def analyze_market(market: Dict) -> Dict:
    """
    Three-layer bad debt analysis + oracle config extraction for a single market.
    """
    state = market.get("state") or {}
    collateral = market.get("collateralAsset") or {}
    loan = market.get("loanAsset") or {}
    bad_debt = market.get("badDebt") or {}
    realized_bad_debt = market.get("realizedBadDebt") or {}
    oracle = market.get("oracle") or {}
    oracle_data = oracle.get("data") or {}

    # ── Asset info ──
    collateral_symbol = collateral.get("symbol", "")
    loan_symbol = loan.get("symbol", "")
    collateral_decimals = safe_int(collateral.get("decimals"), 18)
    loan_decimals = safe_int(loan.get("decimals"), 18)
    collateral_spot_price = safe_float(collateral.get("priceUsd"))
    loan_spot_price = safe_float(loan.get("priceUsd"))

    # ── Raw on-chain values ──
    supply_assets = safe_int(state.get("supplyAssets"))
    borrow_assets = safe_int(state.get("borrowAssets"))
    collateral_assets = safe_int(state.get("collateralAssets"))
    liquidity_assets = safe_int(state.get("liquidityAssets"))

    # ── USD values ──
    supply_usd = safe_float(state.get("supplyAssetsUsd"))
    borrow_usd = safe_float(state.get("borrowAssetsUsd"))
    collat_usd = safe_float(state.get("collateralAssetsUsd"))
    liquidity_usd = safe_float(state.get("liquidityAssetsUsd"))
    utilization = safe_float(state.get("utilization"))
    oracle_price_raw = safe_int(state.get("price"))

    # ══════════════════════════════════════════════════════════════
    # LAYER 1: Supply-Borrow Gap (oracle-independent)
    # ══════════════════════════════════════════════════════════════
    gap_raw = supply_assets - borrow_assets
    gap_usd = supply_usd - borrow_usd
    has_bad_debt_layer1 = gap_raw < 0
    bad_debt_gap_raw = abs(min(0, gap_raw))
    bad_debt_gap_usd = abs(min(0, gap_usd))
    bad_debt_gap_pct = bad_debt_gap_raw / supply_assets if supply_assets > 0 else 0

    # Sanity check: liquidityAssets should ≈ supplyAssets - borrowAssets
    expected_liquidity = supply_assets - borrow_assets
    liquidity_discrepancy = liquidity_assets - expected_liquidity if expected_liquidity >= 0 else None

    # ══════════════════════════════════════════════════════════════
    # LAYER 2: Native Protocol Bad Debt
    # ══════════════════════════════════════════════════════════════
    native_bad_debt_usd = safe_float(bad_debt.get("usd"))
    native_bad_debt_underlying = safe_int(bad_debt.get("underlying"))
    realized_bad_debt_usd = safe_float(realized_bad_debt.get("usd"))
    realized_bad_debt_underlying = safe_int(realized_bad_debt.get("underlying"))
    total_native_bad_debt_usd = native_bad_debt_usd + realized_bad_debt_usd

    # Warning metadata (protocol's own bad debt share)
    warn_bad_debt_usd = None
    warn_bad_debt_share = None
    warn_bad_debt_assets = None
    warn_total_supply_assets = None
    warnings = market.get("warnings") or []
    warning_types = [w.get("type") for w in warnings]
    for w in warnings:
        meta = w.get("metadata")
        if meta and "badDebtShare" in meta:
            warn_bad_debt_usd = safe_float(meta.get("badDebtUsd"))
            warn_bad_debt_share = safe_float(meta.get("badDebtShare"))
            warn_bad_debt_assets = safe_int(meta.get("badDebtAssets"))
            warn_total_supply_assets = safe_int(meta.get("totalSupplyAssets"))

    # ══════════════════════════════════════════════════════════════
    # LAYER 3: Oracle vs Spot Price Gap
    # ══════════════════════════════════════════════════════════════
    oracle_collateral_value_usd = None
    oracle_spot_gap_pct = None
    oracle_spot_gap_usd = None

    if collateral_spot_price and collateral_spot_price > 0 and loan_spot_price and loan_spot_price > 0:
        if oracle_price_raw > 0:
            # oracle state.price = (collateral_price / loan_price) * 10^(36 + loan_dec - collateral_dec)
            scale = 10 ** (36 + loan_decimals - collateral_decimals)
            oracle_collateral_value_usd = (oracle_price_raw / scale) * loan_spot_price
            if oracle_collateral_value_usd > 0:
                oracle_spot_gap_pct = (oracle_collateral_value_usd - collateral_spot_price) / oracle_collateral_value_usd
                # Total unrealized loss from oracle mispricing
                if collateral_assets > 0:
                    collateral_human = collateral_assets / (10 ** collateral_decimals)
                    oracle_spot_gap_usd = collateral_human * (oracle_collateral_value_usd - collateral_spot_price)

    # ── True LTV calculation ──
    # Displayed LTV (using oracle price)
    lltv = float(market.get("lltv", 0)) / 1e18 if market.get("lltv") else 0
    # True LTV = borrow_usd / (collateral × spot_price)
    true_collateral_value = 0
    if collateral_assets > 0 and collateral_spot_price > 0:
        collateral_human = collateral_assets / (10 ** collateral_decimals)
        true_collateral_value = collateral_human * collateral_spot_price
    true_ltv = borrow_usd / true_collateral_value if true_collateral_value > 0 else None
    # Displayed LTV (using oracle)
    displayed_ltv = borrow_usd / collat_usd if collat_usd > 0 else None

    # ══════════════════════════════════════════════════════════════
    # CLASSIFICATION
    # ══════════════════════════════════════════════════════════════
    oracle_masking = has_bad_debt_layer1 and native_bad_debt_usd == 0

    if has_bad_debt_layer1:
        bad_debt_status = "BAD_DEBT_CONFIRMED"
    elif utilization >= 0.99:
        bad_debt_status = "AT_RISK_100PCT_UTIL"
    elif oracle_spot_gap_pct is not None and oracle_spot_gap_pct > 0.10:
        bad_debt_status = "ORACLE_MISPRICING"
    elif total_native_bad_debt_usd > 0:
        bad_debt_status = "BAD_DEBT_NATIVE_REPORTED"
    else:
        bad_debt_status = "HEALTHY"

    # ── Best estimate of actual bad debt ──
    # Use the max of the three layers
    best_estimate_bad_debt_usd = max(
        bad_debt_gap_usd,           # Layer 1
        total_native_bad_debt_usd,  # Layer 2
    )
    # Layer 3 is additive context (unrealized loss) — oracle_spot_gap_usd if available
    if oracle_spot_gap_usd and oracle_spot_gap_usd > best_estimate_bad_debt_usd:
        best_estimate_bad_debt_usd = oracle_spot_gap_usd

    # ══════════════════════════════════════════════════════════════
    # ORACLE CONFIG (Block 5.1 piggyback)
    # ══════════════════════════════════════════════════════════════
    oracle_type = oracle.get("type")
    oracle_address = oracle.get("address")

    # Extract feed details if available
    oracle_base_feed_one = None
    oracle_base_feed_two = None
    oracle_quote_feed_one = None
    oracle_quote_feed_two = None
    oracle_base_vault = None
    oracle_quote_vault = None
    oracle_scale_factor = None
    oracle_base_vault_conversion = None
    oracle_quote_vault_conversion = None
    oracle_is_vault_based = False
    oracle_is_hardcoded = False

    if oracle_data:
        bf1 = oracle_data.get("baseFeedOne")
        oracle_base_feed_one = bf1.get("address") if bf1 else None
        bf2 = oracle_data.get("baseFeedTwo")
        oracle_base_feed_two = bf2.get("address") if bf2 else None
        qf1 = oracle_data.get("quoteFeedOne")
        oracle_quote_feed_one = qf1.get("address") if qf1 else None
        qf2 = oracle_data.get("quoteFeedTwo")
        oracle_quote_feed_two = qf2.get("address") if qf2 else None
        bv = oracle_data.get("baseOracleVault")
        oracle_base_vault = bv.get("address") if bv else None
        qv = oracle_data.get("quoteOracleVault")
        oracle_quote_vault = qv.get("address") if qv else None
        sf = oracle_data.get("scaleFactor")
        oracle_scale_factor = str(sf) if sf is not None else None
        bvc = oracle_data.get("vaultConversionSample") or oracle_data.get("baseVaultConversionSample")
        oracle_base_vault_conversion = str(bvc) if bvc is not None else None
        qvc = oracle_data.get("quoteVaultConversionSample")
        oracle_quote_vault_conversion = str(qvc) if qvc is not None else None

        # Vault-based oracle detection
        ZERO_ADDR = "0x0000000000000000000000000000000000000000"
        has_base_vault = oracle_base_vault and oracle_base_vault != ZERO_ADDR
        has_quote_vault = oracle_quote_vault and oracle_quote_vault != ZERO_ADDR
        oracle_is_vault_based = has_base_vault or has_quote_vault

        # Hardcoded oracle detection: no feeds AND no vault → price is static
        has_feeds = any([
            oracle_base_feed_one and oracle_base_feed_one != ZERO_ADDR,
            oracle_base_feed_two and oracle_base_feed_two != ZERO_ADDR,
            oracle_quote_feed_one and oracle_quote_feed_one != ZERO_ADDR,
            oracle_quote_feed_two and oracle_quote_feed_two != ZERO_ADDR,
        ])
        oracle_is_hardcoded = not has_feeds and not oracle_is_vault_based
    elif oracle_type == "Unknown":
        # Unknown oracle type with no data → likely custom/hardcoded
        oracle_is_hardcoded = True

    # ── Supplying vaults ──
    supplying_vaults = market.get("supplyingVaults") or []
    supplying_vault_count = len(supplying_vaults)
    supplying_vault_names = "|".join(
        [v.get("name", v.get("address", "")) for v in supplying_vaults]
    ) if supplying_vaults else None

    # ══════════════════════════════════════════════════════════════
    # OUTPUT ROW
    # ══════════════════════════════════════════════════════════════
    return {
        # --- IDs ---
        "market_id": market.get("uniqueKey"),
        "market_id_short": f"{market.get('uniqueKey', '')[:6]}...{market.get('uniqueKey', '')[-4:]}" if market.get('uniqueKey') else None,
        "chain_id": None,  # filled from input CSV
        "chain": None,      # filled from input CSV
        "listed": market.get("listed"),

        # --- Pair info ---
        "collateral_symbol": collateral_symbol,
        "collateral_address": collateral.get("address"),
        "collateral_decimals": collateral_decimals,
        "collateral_spot_price": collateral_spot_price,
        "loan_symbol": loan_symbol,
        "loan_address": loan.get("address"),
        "loan_decimals": loan_decimals,
        "loan_spot_price": loan_spot_price,
        "lltv": lltv,

        # --- Raw on-chain state ---
        "supply_assets": supply_assets,
        "borrow_assets": borrow_assets,
        "collateral_assets": collateral_assets,
        "liquidity_assets": liquidity_assets,

        # --- USD state ---
        "supply_usd": supply_usd,
        "borrow_usd": borrow_usd,
        "collateral_usd": collat_usd,
        "liquidity_usd": liquidity_usd,
        "utilization": utilization,

        # ── LAYER 1: Supply-Borrow Gap ──
        "L1_gap_raw": gap_raw,
        "L1_gap_usd": gap_usd,
        "L1_bad_debt_raw": bad_debt_gap_raw,
        "L1_bad_debt_usd": bad_debt_gap_usd,
        "L1_bad_debt_pct": bad_debt_gap_pct,
        "L1_has_bad_debt": has_bad_debt_layer1,
        "L1_liquidity_discrepancy": liquidity_discrepancy,

        # ── LAYER 2: Native Bad Debt ──
        "L2_bad_debt_usd": native_bad_debt_usd,
        "L2_bad_debt_underlying": native_bad_debt_underlying,
        "L2_realized_bad_debt_usd": realized_bad_debt_usd,
        "L2_realized_bad_debt_underlying": realized_bad_debt_underlying,
        "L2_total_bad_debt_usd": total_native_bad_debt_usd,
        "L2_warn_bad_debt_usd": warn_bad_debt_usd,
        "L2_warn_bad_debt_share": warn_bad_debt_share,
        "L2_warn_bad_debt_assets": warn_bad_debt_assets,
        "L2_warn_total_supply_assets": warn_total_supply_assets,

        # ── LAYER 3: Oracle vs Spot ──
        "L3_oracle_price_raw": oracle_price_raw,
        "L3_oracle_collateral_usd": oracle_collateral_value_usd,
        "L3_oracle_spot_gap_pct": oracle_spot_gap_pct,
        "L3_oracle_spot_gap_usd": oracle_spot_gap_usd,
        "L3_true_ltv": true_ltv,
        "L3_displayed_ltv": displayed_ltv,

        # ── Classification ──
        "bad_debt_status": bad_debt_status,
        "oracle_masking": oracle_masking,
        "best_estimate_bad_debt_usd": best_estimate_bad_debt_usd,

        # ── Oracle config (Block 5.1) ──
        "oracle_type": oracle_type,
        "oracle_address": oracle_address,
        "oracle_base_feed_one": oracle_base_feed_one,
        "oracle_base_feed_two": oracle_base_feed_two,
        "oracle_quote_feed_one": oracle_quote_feed_one,
        "oracle_quote_feed_two": oracle_quote_feed_two,
        "oracle_base_vault": oracle_base_vault,
        "oracle_quote_vault": oracle_quote_vault,
        "oracle_scale_factor": oracle_scale_factor,
        "oracle_base_vault_conversion": oracle_base_vault_conversion,
        "oracle_quote_vault_conversion": oracle_quote_vault_conversion,
        "oracle_is_vault_based": oracle_is_vault_based,
        "oracle_is_hardcoded": oracle_is_hardcoded,

        # ── Context ──
        "warning_types": "|".join(warning_types) if warning_types else None,
        "supplying_vault_count": supplying_vault_count,
        "supplying_vault_names": supplying_vault_names,
        "state_timestamp": state.get("timestamp"),
        "state_block_number": state.get("blockNumber"),
    }


def main():
    print("=" * 80)
    print("Block 2.1: Bad Debt Quantification Per Market — Three-Layer Analysis")
    print("=" * 80)

    # ── Load Block 1 market data ──
    input_path = PROJECT_ROOT / "data" / "block1_markets_graphql.csv"
    if not input_path.exists():
        print(f"❌ Block 1 markets CSV not found: {input_path}")
        print("   Run block1_query_markets_graphql.py first.")
        return

    block1 = pd.read_csv(input_path)
    print(f"📂 Loaded {len(block1)} markets from Block 1")
    print(f"   Markets: {block1['collateral_symbol'].value_counts().to_dict()}")

    # ── Query each market individually ──
    print(f"\n🔍 Querying {len(block1)} markets via marketByUniqueKey...")
    results = []

    for idx, row in block1.iterrows():
        market_id = row["market_id"]
        chain_id = int(row["chain_id"])
        chain = row["chain"]
        label = f"{row['collateral_symbol']}/{row['loan_symbol']} ({chain})"
        short_id = f"{market_id[:6]}...{market_id[-4:]}"

        print(f"\n   [{idx+1}/{len(block1)}] {label}  {short_id}")
        print(f"      chain_id={chain_id}")

        market_data = fetch_market_detail(market_id, chain_id)
        if not market_data:
            print(f"      ❌ No data returned — skipping")
            continue

        # Analyze
        analyzed = analyze_market(market_data)
        analyzed["chain_id"] = chain_id
        analyzed["chain"] = chain
        results.append(analyzed)

        # Quick status print
        status = analyzed["bad_debt_status"]
        gap = analyzed["L1_gap_usd"]
        native = analyzed["L2_total_bad_debt_usd"]
        oracle_gap = analyzed["L3_oracle_spot_gap_pct"]
        best = analyzed["best_estimate_bad_debt_usd"]
        util = analyzed["utilization"]

        gap_str = f"${gap:,.2f}" if gap >= 0 else f"-${abs(gap):,.2f}"
        oracle_str = f"{oracle_gap*100:.1f}%" if oracle_gap is not None else "N/A"
        print(f"      Status:    {status}")
        print(f"      L1 gap:    {gap_str}")
        print(f"      L2 native: ${native:,.2f}")
        print(f"      L3 oracle: {oracle_str}")
        print(f"      Best est:  ${best:,.2f}")
        print(f"      Util:      {util*100:.1f}%")
        if analyzed["oracle_masking"]:
            print(f"      ⚠️  ORACLE MASKING: Layer 1 shows bad debt but Layer 2 = $0")
        if analyzed["oracle_is_hardcoded"]:
            print(f"      🔒 HARDCODED ORACLE: no price feeds detected")
        if analyzed["oracle_is_vault_based"]:
            print(f"      🏦 VAULT-BASED ORACLE: base={analyzed['oracle_base_vault']}  quote={analyzed['oracle_quote_vault']}")

        time.sleep(REQUEST_DELAY)

    if not results:
        print("\n❌ No market data retrieved")
        return

    # ── Build DataFrame ──
    df = pd.DataFrame(results)
    df = df.sort_values("best_estimate_bad_debt_usd", ascending=False)

    # ── Save ──
    output_dir = PROJECT_ROOT / "data"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "block2_bad_debt_by_market.csv"
    df.to_csv(output_path, index=False)

    print(f"\n✅ Saved {len(df)} markets to {output_path}")

    # ══════════════════════════════════════════════════════════════
    # SUMMARY REPORT
    # ══════════════════════════════════════════════════════════════
    print(f"\n{'═' * 70}")
    print(f"  THREE-LAYER BAD DEBT REPORT")
    print(f"{'═' * 70}")

    # ── Layer 1 ──
    print(f"\n{'─' * 70}")
    print(f"  LAYER 1: Supply-Borrow Gap (oracle-independent)")
    print(f"{'─' * 70}")
    l1_bad = df[df['L1_has_bad_debt'] == True]
    if len(l1_bad) > 0:
        print(f"  🔴 {len(l1_bad)} markets with gap < 0 (confirmed bad debt)")
        for _, r in l1_bad.iterrows():
            print(f"     {r['collateral_symbol']}/{r['loan_symbol']} ({r['chain']}): "
                  f"gap = -${abs(r['L1_gap_usd']):,.2f}  ({r['L1_bad_debt_pct']*100:.2f}% of supply)")
    else:
        print(f"  ✅ No markets with negative supply-borrow gap")

    l1_at_risk = df[(df['utilization'] >= 0.99) & (df['L1_has_bad_debt'] == False)]
    if len(l1_at_risk) > 0:
        print(f"  ⚠️  {len(l1_at_risk)} markets at 100% utilization (gap = 0, depositors locked)")
        for _, r in l1_at_risk.iterrows():
            print(f"     {r['collateral_symbol']}/{r['loan_symbol']} ({r['chain']}): "
                  f"supply=${r['supply_usd']:,.2f}  borrow=${r['borrow_usd']:,.2f}  liq=${r['liquidity_usd']:,.2f}")

    # ── Layer 2 ──
    print(f"\n{'─' * 70}")
    print(f"  LAYER 2: Native Protocol Bad Debt")
    print(f"{'─' * 70}")
    total_l2 = df['L2_total_bad_debt_usd'].sum()
    print(f"  Total native bad debt:     ${total_l2:,.2f}")
    print(f"  - Current (unrealized):    ${df['L2_bad_debt_usd'].sum():,.2f}")
    print(f"  - Realized (socialized):   ${df['L2_realized_bad_debt_usd'].sum():,.2f}")

    l2_markets = df[df['L2_total_bad_debt_usd'] > 0]
    if len(l2_markets) > 0:
        for _, r in l2_markets.iterrows():
            share_str = f"{r['L2_warn_bad_debt_share']*100:.1f}%" if pd.notna(r.get('L2_warn_bad_debt_share')) else "N/A"
            print(f"     {r['collateral_symbol']}/{r['loan_symbol']} ({r['chain']}): "
                  f"${r['L2_total_bad_debt_usd']:,.2f}  (bad debt share: {share_str})")

    # ── Layer 3 ──
    print(f"\n{'─' * 70}")
    print(f"  LAYER 3: Oracle vs Spot Price Gap")
    print(f"{'─' * 70}")
    # Convert to numeric first — column may contain Python None which breaks .abs()
    df['L3_oracle_spot_gap_pct'] = pd.to_numeric(df['L3_oracle_spot_gap_pct'], errors='coerce')
    l3_mispriced = df[df['L3_oracle_spot_gap_pct'].notna() & (df['L3_oracle_spot_gap_pct'].abs() > 0.05)]
    if len(l3_mispriced) > 0:
        print(f"  🔴 {len(l3_mispriced)} markets with >5% oracle mispricing")
        for _, r in l3_mispriced.iterrows():
            oracle_str = f"${r['L3_oracle_collateral_usd']:.4f}" if pd.notna(r.get('L3_oracle_collateral_usd')) else "N/A"
            gap_usd_str = f"${r['L3_oracle_spot_gap_usd']:,.2f}" if pd.notna(r.get('L3_oracle_spot_gap_usd')) else "N/A"
            true_ltv_str = f"{r['L3_true_ltv']*100:.0f}%" if pd.notna(r.get('L3_true_ltv')) else "N/A"
            disp_ltv_str = f"{r['L3_displayed_ltv']*100:.0f}%" if pd.notna(r.get('L3_displayed_ltv')) else "N/A"
            print(f"     {r['collateral_symbol']}/{r['loan_symbol']} ({r['chain']}):")
            print(f"       Oracle: {oracle_str}  Spot: ${r['collateral_spot_price']:.4f}  "
                  f"Gap: {r['L3_oracle_spot_gap_pct']*100:.1f}%")
            print(f"       True LTV: {true_ltv_str}  Displayed LTV: {disp_ltv_str}")
            print(f"       Unrealized loss: {gap_usd_str}")
    else:
        print(f"  ✅ No significant oracle mispricing detected")

    # ── Oracle masking ──
    masking = df[df['oracle_masking'] == True]
    if len(masking) > 0:
        print(f"\n{'─' * 70}")
        print(f"  ⚠️  ORACLE MASKING DETECTED: {len(masking)} markets")
        print(f"{'─' * 70}")
        print(f"  These markets have Layer 1 bad debt but Layer 2 reports $0.")
        print(f"  The oracle is hiding the true state of the market.")
        for _, r in masking.iterrows():
            print(f"     {r['collateral_symbol']}/{r['loan_symbol']} ({r['chain']}): "
                  f"L1 gap=-${abs(r['L1_gap_usd']):,.2f}  L2=$0  oracle_type={r['oracle_type']}")

    # ── Oracle config summary ──
    print(f"\n{'─' * 70}")
    print(f"  ORACLE CONFIGURATION (Block 5.1)")
    print(f"{'─' * 70}")
    for _, r in df.iterrows():
        tags = []
        if r.get('oracle_is_hardcoded'):
            tags.append("🔒 HARDCODED")
        if r.get('oracle_is_vault_based'):
            tags.append("🏦 VAULT-BASED")
        tags_str = "  ".join(tags) if tags else ""
        oracle_gap_str = f"  gap={r['L3_oracle_spot_gap_pct']*100:.1f}%" if pd.notna(r.get('L3_oracle_spot_gap_pct')) else ""
        print(f"  {r['collateral_symbol']}/{r['loan_symbol']} ({r['chain']}): "
              f"type={r['oracle_type']}  {tags_str}{oracle_gap_str}")
        if r.get('oracle_base_vault'):
            print(f"    base_vault: {r['oracle_base_vault']}")
        if r.get('oracle_quote_vault'):
            print(f"    quote_vault: {r['oracle_quote_vault']}")

    # ── Status classification ──
    print(f"\n{'─' * 70}")
    print(f"  STATUS CLASSIFICATION")
    print(f"{'─' * 70}")
    for status in ['BAD_DEBT_CONFIRMED', 'AT_RISK_100PCT_UTIL', 'ORACLE_MISPRICING',
                    'BAD_DEBT_NATIVE_REPORTED', 'HEALTHY']:
        subset = df[df['bad_debt_status'] == status]
        if len(subset) > 0:
            total_est = subset['best_estimate_bad_debt_usd'].sum()
            total_collat = subset['collateral_usd'].sum()
            print(f"  {status}: {len(subset)} markets  "
                  f"bad_debt≈${total_est:,.2f}  collateral=${total_collat:,.2f}")

    # ── Totals ──
    print(f"\n{'─' * 70}")
    print(f"  TOTALS")
    print(f"{'─' * 70}")
    total_best_est = df['best_estimate_bad_debt_usd'].sum()
    total_supply = df['supply_usd'].sum()
    total_borrow = df['borrow_usd'].sum()
    total_collat_all = df['collateral_usd'].sum()
    print(f"  Best estimate total bad debt:  ${total_best_est:,.2f}")
    print(f"  Total supply across markets:   ${total_supply:,.2f}")
    print(f"  Total borrow across markets:   ${total_borrow:,.2f}")
    print(f"  Total collateral value:        ${total_collat_all:,.2f}")
    if total_supply > 0:
        print(f"  Bad debt as % of supply:       {total_best_est/total_supply*100:.2f}%")

    # ── Breakdown by collateral type ──
    print(f"\n{'─' * 70}")
    print(f"  BY COLLATERAL TYPE")
    print(f"{'─' * 70}")
    for symbol in df['collateral_symbol'].unique():
        subset = df[df['collateral_symbol'] == symbol]
        est = subset['best_estimate_bad_debt_usd'].sum()
        sup = subset['supply_usd'].sum()
        print(f"  {symbol}: {len(subset)} markets  bad_debt≈${est:,.2f}  supply=${sup:,.2f}")

    # ── Breakdown by chain ──
    print(f"\n{'─' * 70}")
    print(f"  BY CHAIN")
    print(f"{'─' * 70}")
    for chain in df['chain'].unique():
        subset = df[df['chain'] == chain]
        est = subset['best_estimate_bad_debt_usd'].sum()
        sup = subset['supply_usd'].sum()
        print(f"  {chain}: {len(subset)} markets  bad_debt≈${est:,.2f}  supply=${sup:,.2f}")

    print(f"\n{'═' * 70}")


if __name__ == "__main__":
    main()
