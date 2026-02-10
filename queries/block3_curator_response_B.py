"""
Block 3B — Curator Response Analysis (Part B: Reallocations + Classification)

Reads CSVs from Part A (block3_allocation_timeseries.csv, block3_admin_events.csv)
then runs Tasks 3 & 4.

TASK 3 (3.3): Reallocation Events
  - All vaultReallocates touching toxic markets
  - Output: block3_reallocations.csv

TASK 4 (3.4): Curator Response Classification (no API — analysis only)
  - Combines tasks 1-3 to classify each curator's response speed
  - Output: block3_curator_profiles.csv

Input:  04-data-exports/raw/graphql/block1_vaults_graphql.csv
        04-data-exports/raw/graphql/block1_markets_graphql.csv
        04-data-exports/raw/graphql/block3_allocation_timeseries.csv  (from Part A)
        04-data-exports/raw/graphql/block3_admin_events.csv           (from Part A)
"""

import time
import json
import requests
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timezone
from dotenv import load_dotenv
from typing import List, Dict, Set, Tuple, Optional

# ── Project paths ──
PROJECT_ROOT = Path(__file__).parent.parent.parent
env_path = PROJECT_ROOT / '.env'
load_dotenv(dotenv_path=env_path)

GRAPHQL_URL = "https://blue-api.morpho.org/graphql"
REQUEST_DELAY = 0.3

# ── Time windows ──
TS_SEPT_01  = 1756684800
TS_OCT_28   = 1761696000
TS_NOV_04   = 1762214400   # Stream Finance collapse / xUSD depeg
TS_JAN_31   = 1769817600

DEPEG_TS = TS_NOV_04


# ═══════════════════════════════════════════════════════════════
#  SHARED UTILITIES
# ═══════════════════════════════════════════════════════════════

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


def ts_to_datetime(ts) -> str:
    return datetime.fromtimestamp(float(ts), tz=timezone.utc).strftime("%Y-%m-%d %H:%M")


def load_block1_data() -> Tuple[pd.DataFrame, pd.DataFrame, Dict, Set]:
    gql_dir = PROJECT_ROOT / "04-data-exports" / "raw" / "graphql"
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


# ═══════════════════════════════════════════════════════════════
#  TASK 3: Reallocation Events (3.3)
# ═══════════════════════════════════════════════════════════════

def query_reallocations_for_chain(chain_id: int, chain_name: str,
                                   vault_addresses: List[str],
                                   toxic_keys: Set[str]) -> List[Dict]:
    if not vault_addresses:
        return []

    addrs_str = ", ".join(f'"{a}"' for a in vault_addresses)
    all_rows = []
    skip = 0
    page_size = 1000

    while True:
        query = f"""
        {{
          vaultReallocates(
            first: {page_size}
            skip: {skip}
            orderBy: Timestamp
            orderDirection: Asc
            where: {{
              vaultAddress_in: [{addrs_str}]
              chainId_in: [{chain_id}]
              timestamp_gte: {TS_SEPT_01}
              timestamp_lte: {TS_JAN_31}
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
                address
                name
              }}
              market {{
                uniqueKey
                collateralAsset {{ symbol }}
                loanAsset {{ symbol }}
              }}
            }}
            pageInfo {{ countTotal count skip limit }}
          }}
        }}
        """

        result = query_graphql(query)

        if "errors" in result:
            err = result["errors"][0].get("message", "")
            print(f"      ❌ vaultReallocates error: {err[:100]}")
            break

        data = result.get("data", {}).get("vaultReallocates", {})
        items = data.get("items", [])
        if not items:
            break

        for item in items:
            market = item.get("market", {})
            market_key = market.get("uniqueKey", "")
            vault_info = item.get("vault", {})
            collateral = (market.get("collateralAsset") or {}).get("symbol", "?")
            loan = (market.get("loanAsset") or {}).get("symbol", "?")
            ts = int(item.get("timestamp", 0))

            all_rows.append({
                "vault_address": vault_info.get("address", ""),
                "vault_name": vault_info.get("name", ""),
                "chain": chain_name,
                "chain_id": chain_id,
                "market_unique_key": market_key,
                "collateral_symbol": collateral,
                "loan_symbol": loan,
                "is_toxic_market": market_key in toxic_keys,
                "timestamp": ts,
                "date": ts_to_date(ts),
                "datetime": ts_to_datetime(ts),
                "tx_hash": item.get("hash", ""),
                "block_number": item.get("blockNumber"),
                "caller": item.get("caller", ""),
                "realloc_type": item.get("type", ""),
                "assets": str(item.get("assets", "0")),
                "shares": str(item.get("shares", "0")),
            })

        skip += page_size
        count_total = data.get("pageInfo", {}).get("countTotal", 0)
        if skip >= count_total:
            break
        time.sleep(REQUEST_DELAY)

    return all_rows


# ═══════════════════════════════════════════════════════════════
#  TASK 4: Curator Response Classification (3.4)
# ═══════════════════════════════════════════════════════════════

def classify_curator_response(
    vault: Dict,
    alloc_rows: List[Dict],
    admin_rows: List[Dict],
    realloc_rows: List[Dict],
    toxic_keys: Set[str]
) -> Dict:
    address = vault["address"].lower()
    name = vault["name"]
    curator = vault["curator_name"]
    status = vault["exposure_status"]

    profile = {
        "vault_address": address,
        "vault_name": name,
        "chain": vault["chain"],
        "chain_id": vault["chain_id"],
        "curator_name": curator,
        "exposure_status": status,
        "discovery_method": vault.get("discovery_method", ""),
        "vault_tvl_usd": vault.get("vault_tvl_usd", 0),
    }

    # ── Allocation timeline analysis ──
    vault_allocs = [r for r in alloc_rows if r["vault_address"].lower() == address]

    if vault_allocs:
        df_a = pd.DataFrame(vault_allocs)
        df_a["supply_assets_usd"] = pd.to_numeric(df_a["supply_assets_usd"], errors="coerce").fillna(0)
        df_a["timestamp"] = pd.to_numeric(df_a["timestamp"])

        peak_row = df_a.loc[df_a["supply_assets_usd"].idxmax()] if len(df_a) > 0 else None
        profile["peak_toxic_supply_usd"] = float(df_a["supply_assets_usd"].max())
        profile["peak_toxic_date"] = peak_row["date"] if peak_row is not None else None

        after_peak = df_a[df_a["timestamp"] >= df_a.loc[df_a["supply_assets_usd"].idxmax(), "timestamp"]]
        zero_rows = after_peak[after_peak["supply_assets_usd"] < 1.0]
        if len(zero_rows) > 0:
            first_zero = zero_rows.sort_values("timestamp").iloc[0]
            profile["first_zero_alloc_ts"] = int(first_zero["timestamp"])
            profile["first_zero_alloc_date"] = first_zero["date"]
        else:
            profile["first_zero_alloc_ts"] = None
            profile["first_zero_alloc_date"] = None

        depeg_day = df_a[(df_a["timestamp"] >= DEPEG_TS) & (df_a["timestamp"] < DEPEG_TS + 86400)]
        profile["alloc_at_depeg_usd"] = float(depeg_day["supply_assets_usd"].iloc[0]) if len(depeg_day) > 0 else None

        week_before = df_a[(df_a["timestamp"] >= TS_OCT_28) & (df_a["timestamp"] < TS_OCT_28 + 86400)]
        profile["alloc_week_before_usd"] = float(week_before["supply_assets_usd"].iloc[0]) if len(week_before) > 0 else None
    else:
        profile["peak_toxic_supply_usd"] = 0
        profile["peak_toxic_date"] = None
        profile["first_zero_alloc_ts"] = None
        profile["first_zero_alloc_date"] = None
        profile["alloc_at_depeg_usd"] = None
        profile["alloc_week_before_usd"] = None

    # ── Admin event analysis ──
    vault_admin = [r for r in admin_rows
                   if r["vault_address"].lower() == address and r["touches_toxic_market"]]

    cap_zero_events = []
    for evt in vault_admin:
        if evt["event_type"] in ("SetCap", "SubmitCap") and evt.get("details"):
            try:
                d = json.loads(evt["details"])
                if d.get("cap_is_zero"):
                    cap_zero_events.append(evt)
            except (json.JSONDecodeError, TypeError):
                pass

    if cap_zero_events:
        first_cap_zero = min(cap_zero_events, key=lambda e: e["timestamp"])
        profile["first_cap_zero_ts"] = first_cap_zero["timestamp"]
        profile["first_cap_zero_date"] = first_cap_zero["date"]
    else:
        profile["first_cap_zero_ts"] = None
        profile["first_cap_zero_date"] = None

    queue_removals = [e for e in vault_admin if e["event_type"] == "SetWithdrawQueue"]
    queue_removed_toxic_ts = None
    for evt in sorted(queue_removals, key=lambda e: e["timestamp"]):
        try:
            d = json.loads(evt["details"])
            if not d.get("queue_has_toxic", True):
                queue_removed_toxic_ts = evt["timestamp"]
                break
        except (json.JSONDecodeError, TypeError):
            pass

    profile["queue_removed_toxic_ts"] = queue_removed_toxic_ts
    profile["queue_removed_toxic_date"] = ts_to_date(queue_removed_toxic_ts) if queue_removed_toxic_ts else None
    profile["total_admin_events"] = len(vault_admin)

    # ── Reallocation analysis ──
    vault_reallocs = [r for r in realloc_rows
                      if r["vault_address"].lower() == address and r["is_toxic_market"]]

    withdrawals = [r for r in vault_reallocs if r["realloc_type"] == "ReallocateWithdraw"]
    supplies = [r for r in vault_reallocs if r["realloc_type"] == "ReallocateSupply"]

    profile["toxic_realloc_withdraw_count"] = len(withdrawals)
    profile["toxic_realloc_supply_count"] = len(supplies)

    if withdrawals:
        first_w = min(withdrawals, key=lambda r: r["timestamp"])
        last_w = max(withdrawals, key=lambda r: r["timestamp"])
        profile["first_realloc_withdraw_ts"] = first_w["timestamp"]
        profile["first_realloc_withdraw_date"] = first_w["date"]
        profile["last_realloc_withdraw_ts"] = last_w["timestamp"]
        profile["last_realloc_withdraw_date"] = last_w["date"]
    else:
        profile["first_realloc_withdraw_ts"] = None
        profile["first_realloc_withdraw_date"] = None
        profile["last_realloc_withdraw_ts"] = None
        profile["last_realloc_withdraw_date"] = None

    # ── Classification ──
    action_timestamps = []
    if profile.get("first_zero_alloc_ts"):
        action_timestamps.append(profile["first_zero_alloc_ts"])
    if profile.get("first_cap_zero_ts"):
        action_timestamps.append(profile["first_cap_zero_ts"])
    if profile.get("first_realloc_withdraw_ts"):
        action_timestamps.append(profile["first_realloc_withdraw_ts"])

    if action_timestamps:
        earliest_action = min(action_timestamps)
        profile["earliest_action_ts"] = earliest_action
        profile["earliest_action_date"] = ts_to_date(earliest_action)

        days_before_depeg = (DEPEG_TS - earliest_action) / 86400

        if days_before_depeg > 7:
            profile["response_class"] = "PROACTIVE"
        elif days_before_depeg > 1:
            profile["response_class"] = "EARLY_REACTOR"
        elif days_before_depeg > 0:
            profile["response_class"] = "LAST_MINUTE"
        elif days_before_depeg > -3:
            profile["response_class"] = "DURING_DEPEG"
        elif days_before_depeg > -14:
            profile["response_class"] = "SLOW_REACTOR"
        else:
            profile["response_class"] = "VERY_LATE"

        profile["days_vs_depeg"] = round(days_before_depeg, 1)
    else:
        profile["earliest_action_ts"] = None
        profile["earliest_action_date"] = None
        profile["days_vs_depeg"] = None

        if status == "ACTIVE_DEPEG":
            profile["response_class"] = "STAYED_EXPOSED"
        elif status in ("HISTORICALLY_EXPOSED", "FULLY_EXITED", "STOPPED_SUPPLYING"):
            profile["response_class"] = "EXITED_TIMING_UNKNOWN"
        else:
            profile["response_class"] = "UNKNOWN"

    return profile


# ═══════════════════════════════════════════════════════════════
#  MAIN — Part B
# ═══════════════════════════════════════════════════════════════

def main():
    print("=" * 80)
    print("Block 3B — Curator Response Analysis (Part B: Reallocations + Classification)")
    print("=" * 80)

    df_vaults, df_markets, chain_keys, toxic_keys = load_block1_data()
    vaults = get_unique_vaults(df_vaults)

    out_dir = PROJECT_ROOT / "04-data-exports" / "raw" / "graphql"

    # ── Load Part A outputs ──
    alloc_path = out_dir / "block3_allocation_timeseries.csv"
    admin_path = out_dir / "block3_admin_events.csv"

    if alloc_path.exists():
        df_alloc = pd.read_csv(alloc_path)
        all_alloc_rows = df_alloc.to_dict("records")
        print(f"📂 Loaded {len(all_alloc_rows)} allocation rows from Part A")
    else:
        print(f"⚠️  {alloc_path.name} not found — run block3_curator_response_A.py first")
        all_alloc_rows = []

    if admin_path.exists():
        df_admin = pd.read_csv(admin_path)
        # Ensure touches_toxic_market is boolean (CSV reads as string)
        if "touches_toxic_market" in df_admin.columns:
            df_admin["touches_toxic_market"] = df_admin["touches_toxic_market"].astype(str).str.lower() == "true"
        # Ensure is_toxic_market same treatment if present
        if "is_toxic_market" in df_admin.columns:
            df_admin["is_toxic_market"] = df_admin["is_toxic_market"].astype(str).str.lower() == "true"
        all_admin_rows = df_admin.to_dict("records")
        print(f"📂 Loaded {len(all_admin_rows)} admin events from Part A")
    else:
        print(f"⚠️  {admin_path.name} not found — run block3_curator_response_A.py first")
        all_admin_rows = []

    # ══════════════════════════════════════════════════════════
    #  TASK 3: Reallocation Events
    # ══════════════════════════════════════════════════════════
    print(f"\n{'─' * 70}")
    print(f"🔄 TASK 3 (3.3): Reallocation Events")
    print(f"{'─' * 70}")

    chain_names = {}
    for _, r in df_vaults.iterrows():
        cid = int(r.get("chain_id", 0))
        chain = str(r.get("chain", ""))
        if cid and chain:
            chain_names[cid] = chain

    all_realloc_rows = []

    for chain_id, chain_name in chain_names.items():
        chain_vaults = [v["address"] for v in vaults if v["chain_id"] == chain_id]
        if not chain_vaults:
            continue

        print(f"\n   {chain_name} ({len(chain_vaults)} vaults)...")
        rows = query_reallocations_for_chain(chain_id, chain_name, chain_vaults, toxic_keys)
        toxic_rows = [r for r in rows if r["is_toxic_market"]]
        print(f"      ✅ {len(rows)} total reallocations, {len(toxic_rows)} involving toxic markets")
        all_realloc_rows.extend(rows)
        time.sleep(REQUEST_DELAY)

    if all_realloc_rows:
        df_realloc = pd.DataFrame(all_realloc_rows)
        realloc_path = out_dir / "block3_reallocations.csv"
        df_realloc.to_csv(realloc_path, index=False)
        print(f"\n✅ Saved {len(all_realloc_rows)} reallocation events to {realloc_path.name}")

        toxic_reallocs = df_realloc[df_realloc["is_toxic_market"]]
        if len(toxic_reallocs) > 0:
            print(f"   Toxic market reallocations: {len(toxic_reallocs)}")
            print(f"   Types: {dict(toxic_reallocs['realloc_type'].value_counts())}")
    else:
        print(f"\n⚠️  No reallocation events collected")

    # ══════════════════════════════════════════════════════════
    #  TASK 4: Curator Response Classification
    # ══════════════════════════════════════════════════════════
    print(f"\n{'─' * 70}")
    print(f"🏷️  TASK 4 (3.4): Curator Response Classification")
    print(f"{'─' * 70}")

    profiles = []
    for v in vaults:
        p = classify_curator_response(v, all_alloc_rows, all_admin_rows, all_realloc_rows, toxic_keys)
        profiles.append(p)

    if profiles:
        df_profiles = pd.DataFrame(profiles)
        profiles_path = out_dir / "block3_curator_profiles.csv"
        df_profiles.to_csv(profiles_path, index=False)
        print(f"\n✅ Saved {len(profiles)} curator profiles to {profiles_path.name}")

    # ══════════════════════════════════════════════════════════
    #  SUMMARY REPORT
    # ══════════════════════════════════════════════════════════
    print(f"\n{'═' * 70}")
    print(f"  CURATOR RESPONSE REPORT")
    print(f"{'═' * 70}")

    if not profiles:
        print("  No profiles to report.")
        return

    df_p = pd.DataFrame(profiles)

    class_order = ["PROACTIVE", "EARLY_REACTOR", "LAST_MINUTE", "DURING_DEPEG",
                   "SLOW_REACTOR", "VERY_LATE", "STAYED_EXPOSED", "EXITED_TIMING_UNKNOWN", "UNKNOWN"]

    for cls in class_order:
        subset = df_p[df_p["response_class"] == cls]
        if len(subset) == 0:
            continue
        total_tvl = subset["vault_tvl_usd"].sum()
        print(f"\n  {cls} ({len(subset)} vaults, TVL=${total_tvl:,.0f})")
        for _, r in subset.sort_values("vault_tvl_usd", ascending=False).iterrows():
            days = r.get("days_vs_depeg")
            days_str = f"{days:+.1f}d" if pd.notna(days) else "N/A"
            action = r.get("earliest_action_date", "")
            tvl = r.get("vault_tvl_usd", 0)
            print(f"    {r['vault_name']} ({r['chain']}): "
                  f"curator={r['curator_name']}  TVL=${tvl:,.0f}  "
                  f"action={action}  days_vs_depeg={days_str}")

    # Key curator timelines
    print(f"\n{'─' * 70}")
    print(f"  KEY CURATOR ACTION TIMELINES")
    print(f"{'─' * 70}")

    for curator in df_p["curator_name"].unique():
        c_vaults = df_p[df_p["curator_name"] == curator].sort_values("vault_tvl_usd", ascending=False)
        total_tvl = c_vaults["vault_tvl_usd"].sum()
        classes = c_vaults["response_class"].unique()
        print(f"\n  {curator} (TVL=${total_tvl:,.0f}, {len(c_vaults)} vaults)")
        print(f"    Classes: {', '.join(classes)}")

        for _, r in c_vaults.iterrows():
            events = []
            if pd.notna(r.get("first_cap_zero_date")):
                events.append(f"cap→0: {r['first_cap_zero_date']}")
            if pd.notna(r.get("first_realloc_withdraw_date")):
                events.append(f"1st withdraw: {r['first_realloc_withdraw_date']}")
            if pd.notna(r.get("first_zero_alloc_date")):
                events.append(f"alloc→0: {r['first_zero_alloc_date']}")
            if pd.notna(r.get("queue_removed_toxic_date")):
                events.append(f"queue removal: {r['queue_removed_toxic_date']}")

            if events:
                print(f"    {r['vault_name']}: {' → '.join(events)}")
            else:
                print(f"    {r['vault_name']}: no timestamped actions found")

    print(f"\n{'═' * 70}")
    print(f"  ✅ Block 3B complete — 2 CSVs saved (reallocations + profiles)")
    print(f"{'═' * 70}")


if __name__ == "__main__":
    main()
