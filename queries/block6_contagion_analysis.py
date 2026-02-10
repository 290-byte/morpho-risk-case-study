"""
Block 6 — Cross-Protocol Risk Contagion

Answers Q2: "Are liquidity risks shared despite isolated markets?"

TASK 1: Multi-Market Vault Exposure (0 API calls)
  - From Block 1 Dune data: map vault → toxic markets
  - Identify vaults exposed to multiple toxic markets
  - Output: block6_vault_market_exposure.csv

TASK 2: Full Vault Allocation Snapshot (API per significant vault)
  - For vaults with >$1K supply: query full allocation to ALL markets
  - Shows toxic vs clean market split
  - Output: block6_vault_full_allocations.csv

TASK 3: Public Allocator Configuration (API per significant vault)
  - Flow caps per market: how much can PA move in/out
  - Output: block6_public_allocator_config.csv

TASK 4: Vault Reallocations During Crisis (API)
  - vaultReallocates for affected vaults, Sept-Jan period
  - Shows curator actions to move funds between markets
  - Output: block6_vault_reallocations.csv

TASK 5: Public Allocator Reallocations (API)
  - publicAllocatorReallocates for toxic markets
  - Shows automated rebalancing events
  - Output: block6_pa_reallocations.csv

TASK 6: Contagion Network Summary (computation)
  - Vault→market adjacency, shared exposure paths
  - Output: block6_contagion_summary.csv

Input:  04-data-exports/raw/dune/block1_dune_vaults_filtered.csv
        04-data-exports/raw/dune/block1_dune_markets_filtered.csv
        04-data-exports/raw/graphql/block1_markets_graphql.csv
"""

import time
import requests
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timezone
from collections import defaultdict
from typing import List, Dict, Tuple, Optional

# ── Project paths ──
PROJECT_ROOT = Path(__file__).parent.parent.parent
GRAPHQL_URL = "https://blue-api.morpho.org/graphql"
REQUEST_DELAY = 0.3

# ── Time windows ──
TS_SEPT_01  = 1756684800
TS_OCT_01   = 1759276800
TS_NOV_01   = 1761955200
TS_NOV_04   = 1762214400   # xUSD depeg
TS_NOV_06   = 1762387200   # deUSD crash
TS_NOV_15   = 1763164800
TS_DEC_01   = 1764547200
TS_JAN_31   = 1769817600

# ── Chain ID map ──
CHAIN_ID_MAP = {
    "ethereum": 1,
    "arbitrum": 42161,
    "plume": 98866,
    "worldchain": 480,
    "base": 8453,
}


def query_graphql(query: str, timeout: int = 60) -> dict:
    headers = {"Content-Type": "application/json"}
    for attempt in range(3):
        try:
            resp = requests.post(GRAPHQL_URL, json={"query": query},
                                 headers=headers, timeout=timeout)
            # Parse JSON body BEFORE raise_for_status — GraphQL returns errors in JSON even on 400
            try:
                data = resp.json()
            except Exception:
                resp.raise_for_status()
                return {"errors": [{"message": f"Non-JSON response: {resp.status_code}"}]}

            if "errors" in data:
                err_msg = data["errors"][0].get("message", "")
                if attempt < 2 and ("timeout" in err_msg.lower() or "rate" in err_msg.lower()):
                    time.sleep(2 ** attempt)
                    continue
                # Return the error with details (don't raise)
                return data

            if resp.status_code >= 400:
                return {"errors": [{"message": f"HTTP {resp.status_code}: {str(data)[:200]}"}]}

            return data
        except requests.exceptions.RequestException as e:
            if attempt < 2:
                time.sleep(2 ** attempt)
                continue
            return {"errors": [{"message": str(e)}]}
    return {"errors": [{"message": "Max retries exceeded"}]}


def safe_float(v, default=0.0):
    try:
        return float(v) if v is not None else default
    except (ValueError, TypeError):
        return default

def safe_int(v, default=0):
    try:
        return int(v) if v is not None else default
    except (ValueError, TypeError):
        return default

def ts_to_date(ts) -> str:
    return datetime.fromtimestamp(float(ts), tz=timezone.utc).strftime("%Y-%m-%d")

def ts_to_datetime(ts) -> str:
    return datetime.fromtimestamp(float(ts), tz=timezone.utc).strftime("%Y-%m-%d %H:%M")


# ═══════════════════════════════════════════════════════════════
#  TASK 2: Full Vault Allocation Snapshot
# ═══════════════════════════════════════════════════════════════

def query_vault_allocation(vault_address: str, chain_id: int) -> Dict:
    """
    Query full allocation for a vault: all markets it supplies to.
    Returns vault info + allocation list.
    Matches exact Morpho API docs pattern for vault allocation.
    """
    query = f"""
    {{
      vaultByAddress(address: "{vault_address}", chainId: {chain_id}) {{
        address
        name
        symbol
        state {{
          totalAssetsUsd
          totalAssets
          totalSupply
          fee
          allocation {{
            market {{
              uniqueKey
              collateralAsset {{ symbol }}
              loanAsset {{ symbol }}
              oracleAddress
              irmAddress
              lltv
            }}
            supplyCap
            supplyAssets
            supplyAssetsUsd
          }}
        }}
      }}
    }}
    """

    result = query_graphql(query)

    if "errors" in result:
        err = result["errors"][0].get("message", "")
        return {"error": err[:200]}

    vault = result.get("data", {}).get("vaultByAddress")
    if not vault:
        return {"error": "Vault not found"}

    return vault


# ═══════════════════════════════════════════════════════════════
#  TASK 3: Public Allocator Configuration
# ═══════════════════════════════════════════════════════════════

def query_public_allocator_config(vault_address: str, chain_id: int) -> Dict:
    """
    Query public allocator config for a vault: fee + flow caps per market.
    """
    query = f"""
    {{
      vaultByAddress(address: "{vault_address}", chainId: {chain_id}) {{
        address
        name
        publicAllocatorConfig {{
          fee
          accruedFee
          flowCaps {{
            market {{
              uniqueKey
              collateralAsset {{ symbol }}
              loanAsset {{ symbol }}
            }}
            maxIn
            maxOut
          }}
        }}
      }}
    }}
    """

    result = query_graphql(query)

    if "errors" in result:
        err = result["errors"][0].get("message", "")
        return {"error": err[:120]}

    vault = result.get("data", {}).get("vaultByAddress")
    if not vault:
        return {"error": "Vault not found"}

    return vault


# ═══════════════════════════════════════════════════════════════
#  TASK 4: Vault Reallocations
# ═══════════════════════════════════════════════════════════════

def query_vault_reallocations(vault_addresses: List[str],
                               start_ts: int, end_ts: int) -> List[Dict]:
    """
    Query vault reallocation events for given vaults in a time window.
    Uses the vaultReallocates endpoint.
    """
    addr_list = ', '.join(f'"{a}"' for a in vault_addresses)

    all_events = []
    skip = 0
    page_size = 100

    while True:
        query = f"""
        {{
          vaultReallocates(
            first: {page_size}
            skip: {skip}
            orderBy: Timestamp
            orderDirection: Desc
            where: {{
              vaultAddress_in: [{addr_list}]
              timestamp_gte: {start_ts}
              timestamp_lte: {end_ts}
            }}
          ) {{
            items {{
              id
              timestamp
              hash
              blockNumber
              caller
              shares
              assets
              type
              vault {{
                id
                address
                name
                chain {{ id network }}
              }}
              market {{
                uniqueKey
                lltv
                loanAsset {{ symbol }}
                collateralAsset {{ symbol }}
              }}
            }}
            pageInfo {{
              countTotal
              count
              skip
              limit
            }}
          }}
        }}
        """

        result = query_graphql(query, timeout=90)

        if "errors" in result:
            err = result["errors"][0].get("message", "")
            print(f"      ❌ Error: {err[:120]}")
            break

        realloc_data = result.get("data", {}).get("vaultReallocates", {})
        items = realloc_data.get("items") or []
        page_info = realloc_data.get("pageInfo") or {}
        total = safe_int(page_info.get("countTotal", 0))

        for item in items:
            vault = item.get("vault") or {}
            market = item.get("market") or {}
            chain = vault.get("chain") or {}

            all_events.append({
                "id": item.get("id", ""),
                "timestamp": safe_int(item.get("timestamp", 0)),
                "date": ts_to_date(safe_int(item.get("timestamp", 0))),
                "datetime": ts_to_datetime(safe_int(item.get("timestamp", 0))),
                "hash": item.get("hash", ""),
                "block_number": safe_int(item.get("blockNumber", 0)),
                "caller": item.get("caller", ""),
                "type": item.get("type", ""),  # ReallocateSupply / ReallocateWithdraw
                "assets": str(item.get("assets", "0")),
                "shares": str(item.get("shares", "0")),
                "vault_address": vault.get("address", ""),
                "vault_name": vault.get("name", ""),
                "chain_id": safe_int(chain.get("id", 0)),
                "chain": chain.get("network", ""),
                "market_unique_key": market.get("uniqueKey", ""),
                "collateral_symbol": (market.get("collateralAsset") or {}).get("symbol", "?"),
                "loan_symbol": (market.get("loanAsset") or {}).get("symbol", "?"),
                "lltv": safe_float(market.get("lltv", 0)),
            })

        if len(items) < page_size or skip + page_size >= total:
            break
        skip += page_size
        time.sleep(REQUEST_DELAY)

    return all_events


# ═══════════════════════════════════════════════════════════════
#  TASK 5: Public Allocator Reallocations
# ═══════════════════════════════════════════════════════════════

def query_pa_reallocations(vault_addresses: List[str],
                            start_ts: int = 0, end_ts: int = 0) -> List[Dict]:
    """
    Query public allocator reallocation events.
    Uses publicAllocatorReallocates endpoint.
    """
    addr_list = ', '.join(f'"{a}"' for a in vault_addresses)

    all_events = []
    skip = 0
    page_size = 100

    ts_filter = ""
    if start_ts > 0:
        ts_filter += f"timestamp_gte: {start_ts}\n"
    if end_ts > 0:
        ts_filter += f"timestamp_lte: {end_ts}\n"

    while True:
        query = f"""
        {{
          publicAllocatorReallocates(
            first: {page_size}
            skip: {skip}
            orderBy: Timestamp
            orderDirection: Desc
            where: {{
              vaultAddress_in: [{addr_list}]
              {ts_filter}
            }}
          ) {{
            items {{
              id
              timestamp
              hash
              blockNumber
              sender
              assets
              type
              vault {{
                address
                name
              }}
              market {{
                uniqueKey
                collateralAsset {{ symbol }}
                loanAsset {{ symbol }}
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

        pa_data = result.get("data", {}).get("publicAllocatorReallocates", {})
        items = pa_data.get("items") or []
        page_info = pa_data.get("pageInfo") or {}
        total = safe_int(page_info.get("countTotal", 0))

        for item in items:
            vault = item.get("vault") or {}
            market = item.get("market") or {}

            all_events.append({
                "id": item.get("id", ""),
                "timestamp": safe_int(item.get("timestamp", 0)),
                "date": ts_to_date(safe_int(item.get("timestamp", 0))),
                "datetime": ts_to_datetime(safe_int(item.get("timestamp", 0))),
                "hash": item.get("hash", ""),
                "block_number": safe_int(item.get("blockNumber", 0)),
                "sender": item.get("sender", ""),
                "type": item.get("type", ""),  # Deposit / Withdraw
                "assets": str(item.get("assets", "0")),
                "vault_address": vault.get("address", ""),
                "vault_name": vault.get("name", ""),
                "market_unique_key": market.get("uniqueKey", ""),
                "collateral_symbol": (market.get("collateralAsset") or {}).get("symbol", "?"),
                "loan_symbol": (market.get("loanAsset") or {}).get("symbol", "?"),
            })

        if len(items) < page_size or skip + page_size >= total:
            break
        skip += page_size
        time.sleep(REQUEST_DELAY)

    return all_events


# ═══════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════

def main():
    print("=" * 80)
    print("Block 6 — Cross-Protocol Risk Contagion")
    print("=" * 80)

    # ── Paths ──
    dune_dir = PROJECT_ROOT / "04-data-exports" / "raw" / "dune"
    gql_dir  = PROJECT_ROOT / "04-data-exports" / "raw" / "graphql"
    out_dir  = gql_dir  # output alongside other graphql data

    # ── Load data ──
    vaults_path = dune_dir / "block1_dune_vaults_filtered.csv"
    markets_dune_path = dune_dir / "block1_dune_markets_filtered.csv"
    markets_gql_path = gql_dir / "block1_markets_graphql.csv"

    df_vaults = pd.read_csv(vaults_path) if vaults_path.exists() else pd.DataFrame()
    df_markets_dune = pd.read_csv(markets_dune_path) if markets_dune_path.exists() else pd.DataFrame()
    df_markets_gql = pd.read_csv(markets_gql_path) if markets_gql_path.exists() else pd.DataFrame()

    print(f"\n📂 Loaded data:")
    print(f"   Vaults (Dune):      {len(df_vaults)} rows")
    print(f"   Markets (Dune):     {len(df_markets_dune)} rows")
    print(f"   Markets (GraphQL):  {len(df_markets_gql)} rows")

    if len(df_vaults) == 0:
        print("   ❌ No vault data found — cannot proceed")
        return

    # Get toxic market IDs from GraphQL markets data
    toxic_market_ids = set()
    if len(df_markets_gql) > 0:
        mk_col = "market_id" if "market_id" in df_markets_gql.columns else "market_unique_key"
        toxic_market_ids = set(df_markets_gql[mk_col].tolist())
    print(f"   Toxic market IDs:   {len(toxic_market_ids)}")

    # ═══════════════════════════════════════════════════════════
    #  TASK 1: Multi-Market Vault Exposure
    # ═══════════════════════════════════════════════════════════
    print(f"\n{'─' * 70}")
    print(f"🏗️  TASK 1: Multi-Market Vault Exposure")
    print(f"{'─' * 70}")

    df_v = df_vaults.copy()
    df_v["chain_id"] = df_v["chain"].map(CHAIN_ID_MAP).fillna(0).astype(int)

    # Build vault → market exposure
    exposure_rows = []
    vault_groups = df_v.groupby("metamorpho")

    for vault_addr, grp in vault_groups:
        name = grp["metamorpho_name"].iloc[0]
        markets = grp["market_name"].unique().tolist()
        chains = grp["chain"].unique().tolist()
        total_supply = grp["vault_supply_usd"].sum()
        n_markets = len(markets)

        # Determine primary chain (most markets or highest supply)
        chain_supply = grp.groupby("chain")["vault_supply_usd"].sum()
        primary_chain = chain_supply.idxmax() if len(chain_supply) > 0 else chains[0]
        primary_chain_id = CHAIN_ID_MAP.get(primary_chain, 1)

        # Classify exposure
        collateral_types = grp["collateral_symbol"].unique().tolist()
        n_collat_types = len(collateral_types)

        if n_markets >= 3:
            risk_class = "HIGH"
        elif n_markets == 2:
            risk_class = "MODERATE"
        else:
            risk_class = "SINGLE"

        exposure_rows.append({
            "vault_address": vault_addr,
            "vault_name": name,
            "primary_chain": primary_chain,
            "primary_chain_id": primary_chain_id,
            "chains": ", ".join(sorted(chains)),
            "n_toxic_markets": n_markets,
            "toxic_markets": " | ".join(sorted(markets)),
            "collateral_types": ", ".join(sorted(collateral_types)),
            "n_collateral_types": n_collat_types,
            "total_supply_usd": total_supply,
            "risk_class": risk_class,
        })

    df_exposure = pd.DataFrame(exposure_rows).sort_values(
        ["n_toxic_markets", "total_supply_usd"], ascending=[False, False]
    )

    exposure_path = out_dir / "block6_vault_market_exposure.csv"
    df_exposure.to_csv(exposure_path, index=False)
    print(f"\n✅ Saved {len(df_exposure)} vault exposure profiles to {exposure_path.name}")

    # Summary
    n_multi = (df_exposure["n_toxic_markets"] > 1).sum()
    n_single = (df_exposure["n_toxic_markets"] == 1).sum()
    n_high = (df_exposure["risk_class"] == "HIGH").sum()

    print(f"\n{'─' * 70}")
    print(f"  VAULT EXPOSURE SUMMARY")
    print(f"{'─' * 70}")
    print(f"  Total vaults exposed to toxic markets: {len(df_exposure)}")
    print(f"  Multi-market exposure (≥2 markets):    {n_multi}")
    print(f"  High exposure (≥3 markets):            {n_high}")
    print(f"  Single-market exposure:                {n_single}")

    # Show top multi-exposed vaults
    multi = df_exposure[df_exposure["n_toxic_markets"] > 1].head(15)
    for _, r in multi.iterrows():
        supply = r["total_supply_usd"]
        supply_str = f"${supply:,.0f}" if supply > 0 else "$0"
        print(f"\n  🔗 {r['vault_name']} ({r['vault_address'][:10]}...)")
        print(f"     {r['n_toxic_markets']} markets | {r['chains']} | Supply: {supply_str}")
        print(f"     Markets: {r['toxic_markets'][:100]}...")

    # ═══════════════════════════════════════════════════════════
    #  TASK 2: Full Vault Allocation Snapshot
    # ═══════════════════════════════════════════════════════════
    print(f"\n{'─' * 70}")
    print(f"📊 TASK 2: Full Vault Allocation Snapshot")
    print(f"{'─' * 70}")

    # Query vaults with significant supply (>$100)
    significant = df_exposure[df_exposure["total_supply_usd"] > 100].copy()
    # Also include multi-market vaults even if supply is small (they show contagion paths)
    multi_market = df_exposure[df_exposure["n_toxic_markets"] > 1].copy()
    query_vaults = pd.concat([significant, multi_market]).drop_duplicates("vault_address")

    # Filter out zero-address and obviously non-vault addresses
    query_vaults = query_vaults[~query_vaults["vault_address"].str.startswith("0x000000000000")]

    print(f"   Querying {len(query_vaults)} significant/multi-exposed vaults...")

    all_allocations = []
    vault_summaries = []

    for idx, (_, vault_row) in enumerate(query_vaults.iterrows()):
        vault_addr = vault_row["vault_address"]
        chain_id = int(vault_row["primary_chain_id"])
        vault_name = vault_row["vault_name"]

        print(f"\n   [{idx+1}/{len(query_vaults)}] {vault_name} ({vault_addr[:10]}...) chain={chain_id}")

        vault_data = query_vault_allocation(vault_addr, chain_id)
        time.sleep(REQUEST_DELAY)

        if "error" in vault_data:
            print(f"      ⚠️  {vault_data['error'][:80]}")
            continue

        state = vault_data.get("state") or {}
        total_usd = safe_float(state.get("totalAssetsUsd", 0))
        # Curator not in this query - use vault name as identifier
        curator_name = "Unknown"

        allocations = state.get("allocation") or []
        n_allocs = len(allocations)

        n_toxic = 0
        toxic_usd = 0
        clean_usd = 0

        for alloc in allocations:
            market = alloc.get("market") or {}
            mk_key = market.get("uniqueKey", "")
            collat = (market.get("collateralAsset") or {}).get("symbol", "?")
            loan = (market.get("loanAsset") or {}).get("symbol", "?")
            supply_usd = safe_float(alloc.get("supplyAssetsUsd", 0))
            supply_assets = str(alloc.get("supplyAssets", "0"))
            supply_cap = str(alloc.get("supplyCap", "0"))
            lltv = safe_float(market.get("lltv", 0))

            is_toxic = mk_key in toxic_market_ids
            if is_toxic:
                n_toxic += 1
                toxic_usd += supply_usd
            else:
                clean_usd += supply_usd

            all_allocations.append({
                "vault_address": vault_addr,
                "vault_name": vault_data.get("name", vault_name),
                "vault_total_usd": total_usd,
                "chain_id": chain_id,
                "market_unique_key": mk_key,
                "collateral_symbol": collat,
                "loan_symbol": loan,
                "lltv": lltv,
                "supply_assets": supply_assets,
                "supply_assets_usd": supply_usd,
                "supply_cap": supply_cap,
                "is_toxic_market": is_toxic,
            })

        toxic_pct = (toxic_usd / total_usd * 100) if total_usd > 0 else 0

        vault_summaries.append({
            "vault_address": vault_addr,
            "vault_name": vault_data.get("name", vault_name),
            "curator_name": curator_name,
            "chain_id": chain_id,
            "total_assets_usd": total_usd,
            "n_total_markets": n_allocs,
            "n_toxic_markets": n_toxic,
            "toxic_allocation_usd": toxic_usd,
            "clean_allocation_usd": clean_usd,
            "toxic_pct_of_total": toxic_pct,
        })

        if n_toxic > 0:
            print(f"      ✅ {n_allocs} markets ({n_toxic} toxic), TVL ${total_usd:,.0f}, "
                  f"toxic: ${toxic_usd:,.0f} ({toxic_pct:.1f}%)")
        else:
            print(f"      ✅ {n_allocs} markets (0 toxic), TVL ${total_usd:,.0f}")

    if all_allocations:
        df_alloc = pd.DataFrame(all_allocations)
        alloc_path = out_dir / "block6_vault_full_allocations.csv"
        df_alloc.to_csv(alloc_path, index=False)
        print(f"\n✅ Saved {len(df_alloc)} allocation records to {alloc_path.name}")

    if vault_summaries:
        df_vsumm = pd.DataFrame(vault_summaries).sort_values("toxic_pct_of_total", ascending=False)
        vsumm_path = out_dir / "block6_vault_allocation_summary.csv"
        df_vsumm.to_csv(vsumm_path, index=False)
        print(f"✅ Saved {len(df_vsumm)} vault summaries to {vsumm_path.name}")

        # Print allocation summary
        print(f"\n{'─' * 70}")
        print(f"  VAULT ALLOCATION SPLIT (toxic vs clean)")
        print(f"{'─' * 70}")

        for _, r in df_vsumm.head(20).iterrows():
            if r["toxic_pct_of_total"] <= 0 and r["total_assets_usd"] < 1000:
                continue
            icon = "🔴" if r["toxic_pct_of_total"] > 50 else ("🟡" if r["toxic_pct_of_total"] > 10 else "🟢")
            print(f"\n  {icon} {r['vault_name']} ({r['curator_name']})")
            print(f"     TVL: ${r['total_assets_usd']:,.0f} | "
                  f"Markets: {r['n_total_markets']} total, {r['n_toxic_markets']} toxic")
            print(f"     Toxic: ${r['toxic_allocation_usd']:,.0f} ({r['toxic_pct_of_total']:.1f}%) | "
                  f"Clean: ${r['clean_allocation_usd']:,.0f}")

    # ═══════════════════════════════════════════════════════════
    #  TASK 3: Public Allocator Configuration
    # ═══════════════════════════════════════════════════════════
    print(f"\n{'─' * 70}")
    print(f"🔧 TASK 3: Public Allocator Configuration")
    print(f"{'─' * 70}")

    # Only query named vaults with supply > $100 (likely real vaults, not EOAs)
    pa_vaults = query_vaults[
        (query_vaults["vault_name"] != "Unknown") &
        (query_vaults["total_supply_usd"] > 100)
    ].head(20)  # cap at 20 to limit API calls

    print(f"   Querying PA config for {len(pa_vaults)} named vaults...")

    pa_config_rows = []

    for idx, (_, vault_row) in enumerate(pa_vaults.iterrows()):
        vault_addr = vault_row["vault_address"]
        chain_id = int(vault_row["primary_chain_id"])
        vault_name = vault_row["vault_name"]

        print(f"\n   [{idx+1}/{len(pa_vaults)}] {vault_name} ({vault_addr[:10]}...)")

        pa_data = query_public_allocator_config(vault_addr, chain_id)
        time.sleep(REQUEST_DELAY)

        if "error" in pa_data:
            print(f"      ⚠️  {pa_data['error'][:80]}")
            continue

        pa_config = pa_data.get("publicAllocatorConfig")
        if not pa_config:
            print(f"      ℹ️  No public allocator configured")
            continue

        fee = str(pa_config.get("fee", "0"))
        flow_caps = pa_config.get("flowCaps") or []

        n_toxic_caps = 0
        for cap in flow_caps:
            market = cap.get("market") or {}
            mk_key = market.get("uniqueKey", "")
            collat = (market.get("collateralAsset") or {}).get("symbol", "?")
            loan = (market.get("loanAsset") or {}).get("symbol", "?")
            max_in = str(cap.get("maxIn", "0"))
            max_out = str(cap.get("maxOut", "0"))

            is_toxic = mk_key in toxic_market_ids

            if is_toxic:
                n_toxic_caps += 1

            pa_config_rows.append({
                "vault_address": vault_addr,
                "vault_name": pa_data.get("name", vault_name),
                "chain_id": chain_id,
                "pa_fee": fee,
                "market_unique_key": mk_key,
                "collateral_symbol": collat,
                "loan_symbol": loan,
                "max_in": max_in,
                "max_out": max_out,
                "is_toxic_market": is_toxic,
            })

        print(f"      ✅ {len(flow_caps)} flow caps ({n_toxic_caps} toxic markets)")

    if pa_config_rows:
        df_pa_config = pd.DataFrame(pa_config_rows)
        pa_config_path = out_dir / "block6_public_allocator_config.csv"
        df_pa_config.to_csv(pa_config_path, index=False)
        print(f"\n✅ Saved {len(df_pa_config)} PA flow cap records to {pa_config_path.name}")

        # Show toxic market flow caps
        toxic_caps = df_pa_config[df_pa_config["is_toxic_market"] == True]
        if len(toxic_caps) > 0:
            print(f"\n{'─' * 70}")
            print(f"  PUBLIC ALLOCATOR FLOW CAPS FOR TOXIC MARKETS")
            print(f"{'─' * 70}")
            for _, r in toxic_caps.iterrows():
                print(f"  {r['vault_name']} → {r['collateral_symbol']}/{r['loan_symbol']}: "
                      f"maxIn={r['max_in']}, maxOut={r['max_out']}")
        else:
            print(f"\n   ℹ️  No PA flow caps found for toxic markets")
    else:
        print(f"\n   ℹ️  No public allocator configs found")

    # ═══════════════════════════════════════════════════════════
    #  TASK 4: Vault Reallocations During Crisis
    # ═══════════════════════════════════════════════════════════
    print(f"\n{'─' * 70}")
    print(f"🔄 TASK 4: Vault Reallocations During Crisis")
    print(f"{'─' * 70}")

    # Get all unique vault addresses with significant supply
    realloc_vaults = query_vaults[query_vaults["total_supply_usd"] > 100]["vault_address"].tolist()
    # Also add multi-market vaults
    multi_addrs = df_exposure[df_exposure["n_toxic_markets"] > 1]["vault_address"].tolist()
    realloc_vaults = list(set(realloc_vaults + multi_addrs))
    # Filter out zero addresses
    realloc_vaults = [a for a in realloc_vaults if not a.startswith("0x000000000000")]

    print(f"   Searching reallocations for {len(realloc_vaults)} vaults (Oct 1 → Jan 31)...")

    # Query in batches of 10 to avoid query size limits
    all_realloc_events = []
    batch_size = 10

    for i in range(0, len(realloc_vaults), batch_size):
        batch = realloc_vaults[i:i+batch_size]
        batch_num = i // batch_size + 1
        total_batches = (len(realloc_vaults) + batch_size - 1) // batch_size

        print(f"   Batch {batch_num}/{total_batches} ({len(batch)} vaults)...")

        events = query_vault_reallocations(batch, TS_OCT_01, TS_JAN_31)
        all_realloc_events.extend(events)

        if events:
            print(f"      ✅ {len(events)} reallocation events")
        else:
            print(f"      ℹ️  No reallocations found")

        time.sleep(REQUEST_DELAY)

    if all_realloc_events:
        df_realloc = pd.DataFrame(all_realloc_events)
        realloc_path = out_dir / "block6_vault_reallocations.csv"
        df_realloc.to_csv(realloc_path, index=False)
        print(f"\n✅ Saved {len(df_realloc)} reallocation events to {realloc_path.name}")

        # Analyze reallocations involving toxic markets
        df_realloc["is_toxic"] = df_realloc["market_unique_key"].isin(toxic_market_ids)
        toxic_realloc = df_realloc[df_realloc["is_toxic"]]

        print(f"\n{'─' * 70}")
        print(f"  REALLOCATION ANALYSIS")
        print(f"{'─' * 70}")
        print(f"  Total reallocations:         {len(df_realloc)}")
        print(f"  Involving toxic markets:     {len(toxic_realloc)}")
        print(f"  Non-toxic market movements:  {len(df_realloc) - len(toxic_realloc)}")

        if len(toxic_realloc) > 0:
            for _, r in toxic_realloc.head(20).iterrows():
                direction = "→ IN" if "Supply" in str(r.get("type", "")) else "← OUT"
                print(f"  {r['datetime']} | {r['vault_name']} | {direction} | "
                      f"{r['collateral_symbol']}/{r['loan_symbol']} | assets={r['assets']}")
        else:
            print(f"  ℹ️  No reallocations touched toxic markets — curator actions stayed in clean markets")
    else:
        print(f"\n   ℹ️  No vault reallocations found in crisis period")

    # ═══════════════════════════════════════════════════════════
    #  TASK 5: Public Allocator Reallocations
    # ═══════════════════════════════════════════════════════════
    print(f"\n{'─' * 70}")
    print(f"🤖 TASK 5: Public Allocator Reallocations")
    print(f"{'─' * 70}")

    print(f"   Searching PA reallocations for {len(realloc_vaults)} vaults...")

    all_pa_events = []

    for i in range(0, len(realloc_vaults), batch_size):
        batch = realloc_vaults[i:i+batch_size]
        batch_num = i // batch_size + 1
        total_batches = (len(realloc_vaults) + batch_size - 1) // batch_size

        print(f"   Batch {batch_num}/{total_batches}...")

        events = query_pa_reallocations(batch, TS_OCT_01, TS_JAN_31)
        all_pa_events.extend(events)

        if events:
            print(f"      ✅ {len(events)} PA reallocation events")
        else:
            print(f"      ℹ️  No PA reallocations")

        time.sleep(REQUEST_DELAY)

    if all_pa_events:
        df_pa = pd.DataFrame(all_pa_events)
        pa_path = out_dir / "block6_pa_reallocations.csv"
        df_pa.to_csv(pa_path, index=False)
        print(f"\n✅ Saved {len(df_pa)} PA reallocation events to {pa_path.name}")

        # Analyze PA movements involving toxic markets
        df_pa["is_toxic"] = df_pa["market_unique_key"].isin(toxic_market_ids)
        toxic_pa = df_pa[df_pa["is_toxic"]]

        print(f"\n{'─' * 70}")
        print(f"  PUBLIC ALLOCATOR REALLOCATION ANALYSIS")
        print(f"{'─' * 70}")
        print(f"  Total PA reallocations:      {len(df_pa)}")
        print(f"  Involving toxic markets:     {len(toxic_pa)}")

        if len(toxic_pa) > 0:
            deposits = toxic_pa[toxic_pa["type"] == "Deposit"]
            withdrawals = toxic_pa[toxic_pa["type"] == "Withdraw"]
            print(f"  Deposits into toxic:         {len(deposits)}")
            print(f"  Withdrawals from toxic:      {len(withdrawals)}")

            for _, r in toxic_pa.head(15).iterrows():
                direction = "DEPOSIT →" if r["type"] == "Deposit" else "← WITHDRAW"
                print(f"  {r['datetime']} | {r['vault_name']} | {direction} | "
                      f"{r['collateral_symbol']}/{r['loan_symbol']} | assets={r['assets']}")
        else:
            print(f"  ℹ️  No PA reallocations touched toxic markets")
    else:
        print(f"\n   ℹ️  No PA reallocations found")

    # ═══════════════════════════════════════════════════════════
    #  TASK 6: Contagion Network Summary
    # ═══════════════════════════════════════════════════════════
    print(f"\n{'─' * 70}")
    print(f"🕸️  TASK 6: Contagion Network Summary")
    print(f"{'─' * 70}")

    # Build vault → market adjacency from allocation data
    contagion_rows = []

    if all_allocations:
        df_alloc_full = pd.DataFrame(all_allocations)

        # Find vaults that bridge toxic and clean markets
        bridge_vaults = []

        for vault_addr, grp in df_alloc_full.groupby("vault_address"):
            vault_name = grp["vault_name"].iloc[0]
            toxic_mkts = grp[grp["is_toxic_market"] == True]
            clean_mkts = grp[grp["is_toxic_market"] == False]

            toxic_supply = toxic_mkts["supply_assets_usd"].sum()
            clean_supply = clean_mkts["supply_assets_usd"].sum()
            total = toxic_supply + clean_supply

            if len(toxic_mkts) > 0 and len(clean_mkts) > 0:
                # This vault bridges toxic and clean → contagion path
                bridge_vaults.append({
                    "vault_address": vault_addr,
                    "vault_name": vault_name,
                    "n_toxic_markets": len(toxic_mkts),
                    "n_clean_markets": len(clean_mkts),
                    "toxic_supply_usd": toxic_supply,
                    "clean_supply_usd": clean_supply,
                    "total_supply_usd": total,
                    "toxic_pct": (toxic_supply / total * 100) if total > 0 else 0,
                    "contagion_path": "BRIDGE",
                    "risk": "Depositors in clean markets share loss from toxic markets",
                })
            elif len(toxic_mkts) > 1:
                # Multi-toxic vault — amplified exposure
                bridge_vaults.append({
                    "vault_address": vault_addr,
                    "vault_name": vault_name,
                    "n_toxic_markets": len(toxic_mkts),
                    "n_clean_markets": 0,
                    "toxic_supply_usd": toxic_supply,
                    "clean_supply_usd": 0,
                    "total_supply_usd": total,
                    "toxic_pct": 100.0,
                    "contagion_path": "MULTI_TOXIC",
                    "risk": "Vault exposed to multiple toxic markets simultaneously",
                })

        if bridge_vaults:
            df_bridges = pd.DataFrame(bridge_vaults).sort_values("total_supply_usd", ascending=False)
            bridge_path = out_dir / "block6_contagion_bridges.csv"
            df_bridges.to_csv(bridge_path, index=False)
            print(f"\n✅ Saved {len(df_bridges)} contagion bridges to {bridge_path.name}")

            n_bridges = (df_bridges["contagion_path"] == "BRIDGE").sum()
            n_multi_toxic = (df_bridges["contagion_path"] == "MULTI_TOXIC").sum()

            print(f"\n  Contagion paths found:")
            print(f"  🔗 BRIDGE (toxic ↔ clean): {n_bridges} vaults")
            print(f"  💀 MULTI_TOXIC:             {n_multi_toxic} vaults")

            for _, r in df_bridges.head(10).iterrows():
                icon = "🔗" if r["contagion_path"] == "BRIDGE" else "💀"
                print(f"\n  {icon} {r['vault_name']} ({r['vault_address'][:10]}...)")
                print(f"     Toxic: {r['n_toxic_markets']} mkts (${r['toxic_supply_usd']:,.0f}) | "
                      f"Clean: {r['n_clean_markets']} mkts (${r['clean_supply_usd']:,.0f})")
                print(f"     Risk: {r['risk']}")
        else:
            print(f"\n  ℹ️  No contagion bridges found — markets may be isolated")

    # Build market → market connections (markets sharing depositors via vaults)
    market_connections = defaultdict(set)
    if all_allocations:
        vault_markets = defaultdict(set)
        for a in all_allocations:
            if a["supply_assets_usd"] > 0:
                vault_markets[a["vault_address"]].add(a["market_unique_key"])

        for vault_addr, markets in vault_markets.items():
            market_list = list(markets)
            for i in range(len(market_list)):
                for j in range(i + 1, len(market_list)):
                    market_connections[market_list[i]].add(market_list[j])
                    market_connections[market_list[j]].add(market_list[i])

        # Count toxic market connections
        toxic_connections = []
        for mk_key in toxic_market_ids:
            connected = market_connections.get(mk_key, set())
            toxic_connected = connected & toxic_market_ids
            clean_connected = connected - toxic_market_ids

            toxic_connections.append({
                "market_unique_key": mk_key,
                "n_connected_markets": len(connected),
                "n_connected_toxic": len(toxic_connected),
                "n_connected_clean": len(clean_connected),
            })

        if toxic_connections:
            df_connections = pd.DataFrame(toxic_connections).sort_values(
                "n_connected_markets", ascending=False
            )
            conn_path = out_dir / "block6_market_connections.csv"
            df_connections.to_csv(conn_path, index=False)
            print(f"\n✅ Saved {len(df_connections)} market connection profiles to {conn_path.name}")

    # ═══════════════════════════════════════════════════════════
    #  FINAL SUMMARY
    # ═══════════════════════════════════════════════════════════
    print(f"\n{'═' * 70}")
    print(f"  ✅ Block 6 — Cross-Protocol Risk Contagion Complete")
    print(f"{'═' * 70}")

    print(f"\n  Q2 ANSWER FRAMEWORK:")
    print(f"  1. Vault isolation:  {n_single} vaults exposed to only 1 toxic market")
    print(f"  2. Shared exposure:  {n_multi} vaults exposed to ≥2 toxic markets")
    print(f"  3. High-risk vaults: {n_high} vaults exposed to ≥3 toxic markets")
    if all_allocations:
        n_bridge_total = len([b for b in bridge_vaults if b["contagion_path"] == "BRIDGE"]) if bridge_vaults else 0
        print(f"  4. Contagion bridges: {n_bridge_total} vaults bridge toxic ↔ clean markets")
    print(f"  5. Vault reallocations: {len(all_realloc_events)} events during crisis")
    print(f"  6. PA reallocations:    {len(all_pa_events)} events during crisis")

    print(f"\n  Outputs:")
    print(f"    block6_vault_market_exposure.csv")
    print(f"    block6_vault_full_allocations.csv")
    print(f"    block6_vault_allocation_summary.csv")
    print(f"    block6_public_allocator_config.csv")
    print(f"    block6_vault_reallocations.csv")
    print(f"    block6_pa_reallocations.csv")
    print(f"    block6_contagion_bridges.csv")
    print(f"    block6_market_connections.csv")
    print(f"{'═' * 70}")


if __name__ == "__main__":
    main()
