"""
Block 3A1 — Curator Response: Allocation Timeseries

TASK 1 (3.1): Historical Allocation Timeseries
  - Per vault: daily supply to each toxic market
  - Output: block3_allocation_timeseries.csv

Input:  04-data-exports/raw/graphql/block1_vaults_graphql.csv
        04-data-exports/raw/graphql/block1_markets_graphql.csv
"""

import time
import requests
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timezone
from dotenv import load_dotenv
from typing import List, Dict, Set, Tuple

# ── Project paths ──
PROJECT_ROOT = Path(__file__).parent.parent
env_path = PROJECT_ROOT / '.env'
load_dotenv(dotenv_path=env_path)

GRAPHQL_URL = "https://blue-api.morpho.org/graphql"
REQUEST_DELAY = 0.3

# ── Time windows (Oct 1 → Nov 30, 2025) ──
TS_OCT_01   = 1759276800
TS_NOV_30   = 1764547199


def query_graphql(query: str, variables: dict = None) -> dict:
    headers = {"Content-Type": "application/json"}
    payload = {"query": query}
    if variables:
        payload["variables"] = variables
    for attempt in range(3):
        try:
            resp = requests.post(GRAPHQL_URL, json=payload, headers=headers, timeout=60)
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


def load_block1_data() -> Tuple[pd.DataFrame, pd.DataFrame, Dict, Set]:
    gql_dir = PROJECT_ROOT / "data"
    vaults_path = gql_dir / "block1_vaults_graphql.csv"
    markets_path = gql_dir / "block1_markets_graphql.csv"
    if not vaults_path.exists() or not markets_path.exists():
        raise FileNotFoundError(f"Block 1 CSVs not found in {gql_dir}")
    df_vaults = pd.read_csv(vaults_path)
    df_markets = pd.read_csv(markets_path)
    if "blockchain" in df_vaults.columns and "chain" not in df_vaults.columns:
        df_vaults.rename(columns={"blockchain": "chain"}, inplace=True)
    toxic_keys = set(df_markets["market_id"].dropna().unique())
    chain_keys = {}
    for _, r in df_markets.iterrows():
        cid = int(r["chain_id"])
        key = r["market_id"]
        chain_keys.setdefault(cid, set()).add(key)
    return df_vaults, df_markets, chain_keys, toxic_keys


def get_unique_vaults(df_vaults: pd.DataFrame) -> List[Dict]:
    seen = set()
    vaults = []
    for _, r in df_vaults.iterrows():
        addr = str(r.get("vault_address", "")).lower()
        cid = int(r.get("chain_id", 0))
        key = (addr, cid)
        if key in seen:
            continue
        seen.add(key)
        vaults.append({
            "address": r.get("vault_address", addr),
            "name": r.get("vault_name", "Unknown"),
            "chain": r.get("chain", ""),
            "chain_id": cid,
            "curator_name": r.get("curator_name", "Unknown"),
            "exposure_status": r.get("exposure_status", "UNKNOWN"),
            "discovery_method": r.get("discovery_method", "current_allocation"),
            "vault_tvl_usd": float(r.get("vault_total_assets_usd", 0) or 0),
        })
    vaults.sort(key=lambda v: v["vault_tvl_usd"], reverse=True)
    return vaults


def query_vault_allocation_history(vault: Dict, toxic_keys: Set[str]) -> List[Dict]:
    address = vault["address"]
    chain_id = vault["chain_id"]

    query = f"""
    {{
      vaultByAddress(address: "{address}", chainId: {chain_id}) {{
        address
        name
        historicalState {{
          allocation {{
            market {{
              uniqueKey
              collateralAsset {{ symbol }}
              loanAsset {{ symbol }}
            }}
            supplyAssetsUsd(options: {{
              startTimestamp: {TS_OCT_01}
              endTimestamp: {TS_NOV_30}
              interval: DAY
            }}) {{ x, y }}
            supplyCap(options: {{
              startTimestamp: {TS_OCT_01}
              endTimestamp: {TS_NOV_30}
              interval: DAY
            }}) {{ x, y }}
            supplyCapUsd(options: {{
              startTimestamp: {TS_OCT_01}
              endTimestamp: {TS_NOV_30}
              interval: DAY
            }}) {{ x, y }}
          }}
        }}
      }}
    }}
    """

    result = query_graphql(query)

    if "errors" in result:
        err = result["errors"][0].get("message", "")
        print(f"      ❌ Error: {err[:100]}")
        return []

    vault_data = result.get("data", {}).get("vaultByAddress")
    if not vault_data:
        print(f"      ⚠️  Vault not found")
        return []

    allocations = vault_data.get("historicalState", {}).get("allocation", [])
    rows = []

    for alloc in allocations:
        market = alloc.get("market", {})
        unique_key = market.get("uniqueKey", "")
        if unique_key not in toxic_keys:
            continue

        collateral = (market.get("collateralAsset") or {}).get("symbol", "?")
        loan = (market.get("loanAsset") or {}).get("symbol", "?")

        supply_usd_pts = alloc.get("supplyAssetsUsd", []) or []
        supply_cap_pts = alloc.get("supplyCap", []) or []
        supply_cap_usd_pts = alloc.get("supplyCapUsd", []) or []

        cap_by_ts = {int(p["x"]): p["y"] for p in supply_cap_pts if p}
        cap_usd_by_ts = {int(p["x"]): p["y"] for p in supply_cap_usd_pts if p}

        for pt in supply_usd_pts:
            if not pt or pt.get("y") is None:
                continue
            ts = int(pt["x"])
            supply_usd = float(pt["y"]) if pt["y"] is not None else 0.0
            cap_raw = cap_by_ts.get(ts)
            cap_usd = cap_usd_by_ts.get(ts)

            rows.append({
                "vault_address": address,
                "vault_name": vault["name"],
                "chain": vault["chain"],
                "chain_id": chain_id,
                "curator_name": vault["curator_name"],
                "market_unique_key": unique_key,
                "collateral_symbol": collateral,
                "loan_symbol": loan,
                "timestamp": ts,
                "date": ts_to_date(ts),
                "supply_assets_usd": supply_usd,
                "supply_cap": int(cap_raw) if cap_raw is not None else None,
                "supply_cap_usd": float(cap_usd) if cap_usd is not None else None,
            })

    return rows


def main():
    print("=" * 80)
    print("Block 3A1 — Allocation Timeseries")
    print("=" * 80)

    df_vaults, df_markets, chain_keys, toxic_keys = load_block1_data()
    vaults = get_unique_vaults(df_vaults)

    print(f"\n📂 Loaded Block 1: {len(vaults)} vaults, {len(toxic_keys)} toxic markets")

    out_dir = PROJECT_ROOT / "data"
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"\n{'─' * 70}")
    print(f"📊 TASK 1: Historical Allocation Timeseries")
    print(f"{'─' * 70}")

    all_rows = []
    for idx, v in enumerate(vaults):
        print(f"\n   [{idx+1}/{len(vaults)}] {v['name']} ({v['chain']}) [{v['discovery_method']}]")
        rows = query_vault_allocation_history(v, toxic_keys)
        if rows:
            df_tmp = pd.DataFrame(rows)
            peak = df_tmp["supply_assets_usd"].max()
            nonzero = df_tmp[df_tmp["supply_assets_usd"] > 1]
            if len(nonzero) > 0:
                print(f"      ✅ {len(rows)} pts, peak ${peak:,.0f}, {nonzero['date'].min()} → {nonzero['date'].max()}")
            else:
                print(f"      ✅ {len(rows)} pts, all ~$0")
        else:
            print(f"      ⚠️  No toxic allocation history")
        all_rows.extend(rows)
        time.sleep(REQUEST_DELAY)

    if all_rows:
        df = pd.DataFrame(all_rows)
        path = out_dir / "block3_allocation_timeseries.csv"
        df.to_csv(path, index=False)
        print(f"\n✅ Saved {len(all_rows)} rows to {path.name}")
    else:
        print(f"\n⚠️  No allocation data collected")

    print(f"\n{'═' * 70}")
    print(f"  ✅ Block 3A1 complete — run block3_curator_response_A2.py next")
    print(f"{'═' * 70}")


if __name__ == "__main__":
    main()
