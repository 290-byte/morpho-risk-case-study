"""
Block 3A2 — Curator Response: Admin Events

TASK 2 (3.2): Admin Events (Cap Sets, Queue Changes, Removals)
  - Per vault: SetCap(0), SetWithdrawQueue, ReallocateWithdraw, etc.
  - Output: block3_admin_events.csv

Input:  04-data-exports/raw/graphql/block1_vaults_graphql.csv
        04-data-exports/raw/graphql/block1_markets_graphql.csv
"""

import time
import json
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


def query_vault_admin_events(vault: Dict, toxic_keys: Set[str]) -> List[Dict]:
    address = vault["address"]
    chain_id = vault["chain_id"]

    # Types we need — reallocations handled by block3_B via vaultReallocates
    # API uses camelCase: setCap, submitCap, etc.
    ADMIN_TYPES = '["setCap", "submitCap", "revokePendingCap", "revokePendingMarketRemoval", "setSupplyQueue", "setWithdrawQueue"]'

    # ── Pass 1: Lightweight scan (filtered to admin-only types) ──
    raw_events = []
    skip = 0
    page_size = 100

    while True:
        query = f"""
        {{
          vaultByAddress(address: "{address}", chainId: {chain_id}) {{
            adminEvents(
              first: {page_size}
              skip: {skip}
              where: {{ type_in: {ADMIN_TYPES} }}
            ) {{
              items {{
                hash
                timestamp
                type
              }}
              pageInfo {{ countTotal count skip limit }}
            }}
          }}
        }}
        """

        result = query_graphql(query)

        if "errors" in result:
            err = result["errors"][0].get("message", "")
            print(f"      ❌ adminEvents pass1 error: {err[:200]}")
            break

        vault_data = result.get("data", {}).get("vaultByAddress")
        if not vault_data:
            break

        events_data = vault_data.get("adminEvents", {})
        items = events_data.get("items", [])
        page_info = events_data.get("pageInfo", {})
        count_total = page_info.get("countTotal", 0)

        if skip == 0:
            print(f"      📋 Total admin events in API: {count_total}")

        if not items:
            break

        raw_events.extend(items)

        skip += page_size
        if skip >= count_total:
            break
        time.sleep(REQUEST_DELAY)

    if not raw_events:
        return []

    type_counts = {}
    for evt in raw_events:
        t = evt.get("type", "?")
        type_counts[t] = type_counts.get(t, 0) + 1
    print(f"      Event types found: {type_counts}")

    # ── Pass 2: Enrichment ──
    ENRICHABLE_TYPES = {"setCap", "submitCap",
                        "revokePendingCap", "revokePendingMarketRemoval",
                        "setSupplyQueue", "setWithdrawQueue"}

    has_enrichable = any(evt.get("type") in ENRICHABLE_TYPES for evt in raw_events)

    enriched_data = {}
    if has_enrichable:
        for attempt_size in [25, 10]:
            enrich_skip = 0
            success = False
            while True:
                query = f"""
                {{
                  vaultByAddress(address: "{address}", chainId: {chain_id}) {{
                    adminEvents(
                      first: {attempt_size}
                      skip: {enrich_skip}
                      where: {{ type_in: {ADMIN_TYPES} }}
                    ) {{
                      items {{
                        hash
                        type
                        data {{
                          ... on CapEventData {{
                            cap
                          }}
                          ... on TimelockEventData {{
                            timelock
                          }}
                        }}
                      }}
                      pageInfo {{ countTotal count skip limit }}
                    }}
                  }}
                }}
                """

                result = query_graphql(query)

                if "errors" in result:
                    if attempt_size > 10:
                        break
                    err = result["errors"][0].get("message", "")
                    print(f"      ⚠️  Enrichment failed (size={attempt_size}): {err[:120]}")
                    break

                vault_data = result.get("data", {}).get("vaultByAddress")
                if not vault_data:
                    break

                items = vault_data.get("adminEvents", {}).get("items", [])
                if not items:
                    success = True
                    break

                for item in items:
                    h = item.get("hash", "")
                    if h and item.get("data"):
                        enriched_data[h] = item["data"]

                success = True
                enrich_skip += attempt_size
                ct = vault_data.get("adminEvents", {}).get("pageInfo", {}).get("countTotal", 0)
                if enrich_skip >= ct:
                    break
                time.sleep(REQUEST_DELAY)

            if success:
                if enriched_data:
                    print(f"      ✅ Enriched {len(enriched_data)} events with data")
                break

    # ── Queue enrichment ──
    queue_events = [e for e in raw_events if e.get("type") in ("setWithdrawQueue", "setSupplyQueue")]
    queue_data = {}
    if queue_events:
        for q_skip in range(0, len(raw_events), 25):
            query = f"""
            {{
              vaultByAddress(address: "{address}", chainId: {chain_id}) {{
                adminEvents(
                  first: 25
                  skip: {q_skip}
                  where: {{ type_in: ["setWithdrawQueue", "setSupplyQueue"] }}
                ) {{
                  items {{
                    hash
                    type
                    data {{
                      ... on SetWithdrawQueueEventData {{
                        withdrawQueue {{ uniqueKey }}
                      }}
                      ... on SetSupplyQueueEventData {{
                        supplyQueue {{ uniqueKey }}
                      }}
                    }}
                  }}
                  pageInfo {{ countTotal }}
                }}
              }}
            }}
            """
            result = query_graphql(query)
            if "errors" in result:
                print(f"      ⚠️  Queue enrichment failed (skip={q_skip}), skipping")
                break

            vault_data = result.get("data", {}).get("vaultByAddress")
            if not vault_data:
                break

            items = vault_data.get("adminEvents", {}).get("items", [])
            for item in items:
                h = item.get("hash", "")
                d = item.get("data") or {}
                if item.get("type") == "setWithdrawQueue" and "withdrawQueue" in d:
                    queue_data[h] = {"type": "withdraw", "keys": [
                        m.get("uniqueKey", "") for m in (d["withdrawQueue"] or [])
                    ]}
                elif item.get("type") == "setSupplyQueue" and "supplyQueue" in d:
                    queue_data[h] = {"type": "supply", "keys": [
                        m.get("uniqueKey", "") for m in (d["supplyQueue"] or [])
                    ]}

            ct = vault_data.get("adminEvents", {}).get("pageInfo", {}).get("countTotal", 0)
            if q_skip + 25 >= ct:
                break
            time.sleep(REQUEST_DELAY)

    # ── Combine into final rows ──
    all_events = []
    for evt in raw_events:
        ts = int(evt.get("timestamp", 0))
        evt_type = evt.get("type", "")
        tx_hash = evt.get("hash", "")
        data = enriched_data.get(tx_hash, {})
        q_info = queue_data.get(tx_hash, {})

        market_key = None
        collateral = None
        details = {}

        if evt_type in ("setCap", "submitCap"):
            cap_val = data.get("cap")
            if cap_val is not None:
                details["cap"] = str(cap_val)
                details["cap_is_zero"] = (int(cap_val) == 0)

        elif evt_type == "setWithdrawQueue" and q_info:
            queue_keys = q_info.get("keys", [])
            details["queue_size"] = len(queue_keys)
            details["queue_has_toxic"] = any(k in toxic_keys for k in queue_keys)
            details["toxic_in_queue"] = [k for k in queue_keys if k in toxic_keys]

        elif evt_type == "setSupplyQueue" and q_info:
            queue_keys = q_info.get("keys", [])
            details["queue_size"] = len(queue_keys)
            details["queue_has_toxic"] = any(k in toxic_keys for k in queue_keys)

        touches_toxic = (market_key in toxic_keys) if market_key else False
        if evt_type in ("setWithdrawQueue", "setSupplyQueue"):
            touches_toxic = True

        all_events.append({
            "vault_address": address,
            "vault_name": vault["name"],
            "chain": vault["chain"],
            "chain_id": chain_id,
            "curator_name": vault["curator_name"],
            "tx_hash": tx_hash,
            "timestamp": ts,
            "date": ts_to_date(ts),
            "datetime": ts_to_datetime(ts),
            "event_type": evt_type,
            "market_unique_key": market_key or "",
            "collateral_symbol": collateral or "",
            "touches_toxic_market": touches_toxic,
            "details": json.dumps(details) if details else "",
        })

    return all_events


def main():
    print("=" * 80)
    print("Block 3A2 — Admin Events")
    print("=" * 80)

    df_vaults, df_markets, chain_keys, toxic_keys = load_block1_data()
    vaults = get_unique_vaults(df_vaults)

    print(f"\n📂 Loaded Block 1: {len(vaults)} vaults, {len(toxic_keys)} toxic markets")

    out_dir = PROJECT_ROOT / "data"
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"\n{'─' * 70}")
    print(f"🔧 TASK 2: Admin Events (Cap Sets, Queue Changes)")
    print(f"{'─' * 70}")

    all_rows = []
    for idx, v in enumerate(vaults):
        print(f"\n   [{idx+1}/{len(vaults)}] {v['name']} ({v['chain']})")
        rows = query_vault_admin_events(v, toxic_keys)
        toxic_events = [r for r in rows if r["touches_toxic_market"]]
        print(f"      ✅ {len(rows)} total events, {len(toxic_events)} touching toxic markets")

        for evt in toxic_events:
            if evt["event_type"] in ("setCap", "submitCap"):
                try:
                    d = json.loads(evt["details"]) if evt["details"] else {}
                    if d.get("cap_is_zero"):
                        print(f"         🚫 {evt['datetime']}: SetCap → 0 ({evt['collateral_symbol']})")
                except (json.JSONDecodeError, TypeError):
                    pass
            elif evt["event_type"] == "setWithdrawQueue":
                try:
                    d = json.loads(evt["details"]) if evt["details"] else {}
                    if not d.get("queue_has_toxic", True):
                        print(f"         ❌ {evt['datetime']}: Toxic market REMOVED from withdraw queue")
                except (json.JSONDecodeError, TypeError):
                    pass

        all_rows.extend(rows)
        time.sleep(REQUEST_DELAY)

    if all_rows:
        df = pd.DataFrame(all_rows)
        path = out_dir / "block3_admin_events.csv"
        df.to_csv(path, index=False)
        print(f"\n✅ Saved {len(all_rows)} admin events to {path.name}")

        toxic_admin = df[df["touches_toxic_market"]]
        print(f"   Toxic-related events: {len(toxic_admin)}")
        if len(toxic_admin) > 0:
            print(f"   Event types: {dict(toxic_admin['event_type'].value_counts())}")
    else:
        print(f"\n⚠️  No admin events collected")

    print(f"\n{'═' * 70}")
    print(f"  ✅ Block 3A2 complete — run block3_curator_response_B.py next")
    print(f"{'═' * 70}")


if __name__ == "__main__":
    main()
