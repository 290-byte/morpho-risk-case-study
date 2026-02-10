"""
Block 3 — Curator Response Analysis

Answers: How did each curator react to the deUSD/xUSD depeg? When? How fast?

TASK 1 (3.1): Historical Allocation Timeseries
  - Per vault: daily supply to each toxic market (supplyAssetsUsd, supplyCap)
  - Shows exactly when each vault reduced → 0 allocation
  - Output: block3_allocation_timeseries.csv

TASK 2 (3.2): Admin Events (Cap Sets, Queue Changes, Removals)
  - Per vault: SetCap(0), SetWithdrawQueue, ReallocateWithdraw, etc.
  - Captures the exact moment curators acted
  - Output: block3_admin_events.csv

TASK 3 (3.3): Reallocation Events
  - All vaultReallocates touching toxic markets
  - Shows fund flows: which markets did curators move assets FROM/TO
  - Output: block3_reallocations.csv

TASK 4 (3.4): Curator Response Classification (no API — analysis only)
  - Combines tasks 1-3 to classify each curator's response speed
  - PROACTIVE / EARLY_REACTOR / DURING_DEPEG / SLOW_REACTOR / STAYED_EXPOSED
  - Output: block3_curator_profiles.csv

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
from typing import List, Dict, Set, Tuple, Optional

# ── Project paths ──
PROJECT_ROOT = Path(__file__).parent.parent.parent
env_path = PROJECT_ROOT / '.env'
load_dotenv(dotenv_path=env_path)

GRAPHQL_URL = "https://blue-api.morpho.org/graphql"
REQUEST_DELAY = 0.3  # seconds between API calls (5k/5min limit)

# ── Time windows ──
# Full arc: Sept 1 2025 → Jan 31 2026
TS_SEPT_01  = 1756684800
TS_OCT_01   = 1759363200
TS_OCT_28   = 1761696000   # Pre-depeg week start
TS_NOV_01   = 1761955200
TS_NOV_04   = 1762214400   # Stream Finance collapse / xUSD depeg
TS_NOV_06   = 1762387200   # Elixir deUSD crash
TS_NOV_07   = 1762473600
TS_NOV_08   = 1762560000
TS_NOV_12   = 1762905600   # MEV Capital completes delisting
TS_NOV_15   = 1763164800
TS_DEC_01   = 1764547200
TS_JAN_31   = 1769817600

# Curator response classification thresholds
DEPEG_TS = TS_NOV_04


# ═══════════════════════════════════════════════════════════════
#  SHARED UTILITIES
# ═══════════════════════════════════════════════════════════════

def query_graphql(query: str, variables: dict = None) -> dict:
    """Execute GraphQL query against Morpho API with retry"""
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
                # Some errors are retryable
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
    """Convert unix timestamp to ISO date"""
    return datetime.fromtimestamp(float(ts), tz=timezone.utc).strftime("%Y-%m-%d")


def ts_to_datetime(ts) -> str:
    """Convert unix timestamp to ISO datetime"""
    return datetime.fromtimestamp(float(ts), tz=timezone.utc).strftime("%Y-%m-%d %H:%M")


def load_block1_data() -> Tuple[pd.DataFrame, pd.DataFrame, Dict, Set]:
    """Load Block 1 outputs and extract toxic market keys + vault list"""
    gql_dir = PROJECT_ROOT / "04-data-exports" / "raw" / "graphql"
    vaults_path = gql_dir / "block1_vaults_graphql.csv"
    markets_path = gql_dir / "block1_markets_graphql.csv"

    if not vaults_path.exists() or not markets_path.exists():
        raise FileNotFoundError(f"Block 1 CSVs not found in {gql_dir}")

    df_vaults = pd.read_csv(vaults_path)
    df_markets = pd.read_csv(markets_path)

    # Handle column name variations
    if "blockchain" in df_vaults.columns and "chain" not in df_vaults.columns:
        df_vaults.rename(columns={"blockchain": "chain"}, inplace=True)

    # Build toxic market uniqueKeys set
    toxic_keys = set(df_markets["market_id"].dropna().unique())

    # Build chain→keys mapping for efficient queries
    chain_keys = {}
    for _, r in df_markets.iterrows():
        cid = int(r["chain_id"])
        key = r["market_id"]
        chain_keys.setdefault(cid, set()).add(key)

    return df_vaults, df_markets, chain_keys, toxic_keys


def get_unique_vaults(df_vaults: pd.DataFrame) -> List[Dict]:
    """Deduplicate vaults from Block 1 (which has vault-market pairs)"""
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
    # Sort by TVL descending
    vaults.sort(key=lambda v: v["vault_tvl_usd"], reverse=True)
    return vaults


# ═══════════════════════════════════════════════════════════════
#  TASK 1: Historical Allocation Timeseries (3.1)
# ═══════════════════════════════════════════════════════════════

def query_vault_allocation_history(vault: Dict, toxic_keys: Set[str]) -> List[Dict]:
    """
    Query daily historical allocation for a vault across ALL its markets,
    then filter for toxic market uniqueKeys.

    Returns rows: one per (vault, market, date) with supplyAssetsUsd and supplyCap.
    """
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
              startTimestamp: {TS_SEPT_01}
              endTimestamp: {TS_JAN_31}
              interval: DAY
            }}) {{ x, y }}
            supplyCap(options: {{
              startTimestamp: {TS_SEPT_01}
              endTimestamp: {TS_JAN_31}
              interval: DAY
            }}) {{ x, y }}
            supplyCapUsd(options: {{
              startTimestamp: {TS_SEPT_01}
              endTimestamp: {TS_JAN_31}
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

        # Only keep toxic market allocations
        if unique_key not in toxic_keys:
            continue

        collateral = (market.get("collateralAsset") or {}).get("symbol", "?")
        loan = (market.get("loanAsset") or {}).get("symbol", "?")

        supply_usd_pts = alloc.get("supplyAssetsUsd", []) or []
        supply_cap_pts = alloc.get("supplyCap", []) or []
        supply_cap_usd_pts = alloc.get("supplyCapUsd", []) or []

        # Build lookup dicts by timestamp for cap data
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


# ═══════════════════════════════════════════════════════════════
#  TASK 2: Admin Events (3.2)
# ═══════════════════════════════════════════════════════════════

# Admin event types we care about (used for client-side filtering)
RELEVANT_ADMIN_TYPES = {
    "SetCap", "SubmitCap", "SetSupplyQueue", "SetWithdrawQueue",
    "ReallocateSupply", "ReallocateWithdraw",
    "RevokeCap", "RevokePendingMarketRemoval",
    "SubmitMarketRemoval", "SetTimelock", "SubmitTimelock",
}


def query_vault_admin_events(vault: Dict, toxic_keys: Set[str]) -> List[Dict]:
    """
    Fetch admin events for a vault using a 2-pass approach:
      Pass 1: Lightweight scan — just type, timestamp, hash (no data fragments)
      Pass 2: Enrichment — for vaults with interesting events, try to get market details
    
    This avoids 504 timeouts (complex fragments) and Market.uniqueKey null errors
    (deleted markets referenced by old events).
    """
    address = vault["address"]
    chain_id = vault["chain_id"]

    # ── Pass 1: Lightweight scan ──
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

    # Summarize types found
    type_counts = {}
    for evt in raw_events:
        t = evt.get("type", "?")
        type_counts[t] = type_counts.get(t, 0) + 1
    print(f"      Event types found: {type_counts}")

    # ── Pass 2: Enrichment with data fragments (per event type, with error handling) ──
    # Only attempt enrichment for event types that carry market info
    ENRICHABLE_TYPES = {"SetCap", "SubmitCap", "ReallocateSupply", "ReallocateWithdraw",
                        "RevokeCap", "RevokePendingMarketRemoval",
                        "SetSupplyQueue", "SetWithdrawQueue"}

    has_enrichable = any(evt.get("type") in ENRICHABLE_TYPES for evt in raw_events)

    enriched_data = {}  # hash → data dict
    if has_enrichable:
        # Try enrichment query — smaller page, with error handling
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
                    ) {{
                      items {{
                        hash
                        type
                        data {{
                          ... on CapEventData {{
                            cap
                          }}
                          ... on ReallocateSupplyEventData {{
                            suppliedAssets
                            suppliedShares
                          }}
                          ... on ReallocateWithdrawEventData {{
                            withdrawnAssets
                            withdrawnShares
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
                    # Market.uniqueKey null or timeout — skip enrichment
                    err = result["errors"][0].get("message", "")
                    if attempt_size > 10:
                        break  # Try smaller page size
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
                break  # Don't try smaller page size if we succeeded

    # ── Now try a separate query for queue events (these fail on Market.uniqueKey) ──
    # Queue queries are tricky because they return Market objects that may have null keys.
    # We query them separately with just uniqueKey in a try-catch manner.
    queue_events = [e for e in raw_events if e.get("type") in ("SetWithdrawQueue", "SetSupplyQueue")]
    queue_data = {}  # hash → queue_keys list
    if queue_events:
        for q_skip in range(0, len(raw_events), 25):
            query = f"""
            {{
              vaultByAddress(address: "{address}", chainId: {chain_id}) {{
                adminEvents(
                  first: 25
                  skip: {q_skip}
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
                # Queue market references deleted markets — skip gracefully
                print(f"      ⚠️  Queue enrichment failed (skip={q_skip}), skipping")
                break

            vault_data = result.get("data", {}).get("vaultByAddress")
            if not vault_data:
                break

            items = vault_data.get("adminEvents", {}).get("items", [])
            for item in items:
                h = item.get("hash", "")
                d = item.get("data") or {}
                if item.get("type") == "SetWithdrawQueue" and "withdrawQueue" in d:
                    queue_data[h] = {"type": "withdraw", "keys": [
                        m.get("uniqueKey", "") for m in (d["withdrawQueue"] or [])
                    ]}
                elif item.get("type") == "SetSupplyQueue" and "supplyQueue" in d:
                    queue_data[h] = {"type": "supply", "keys": [
                        m.get("uniqueKey", "") for m in (d["supplyQueue"] or [])
                    ]}

            ct = vault_data.get("adminEvents", {}).get("pageInfo", {}).get("countTotal", 0)
            if q_skip + 25 >= ct:
                break
            time.sleep(REQUEST_DELAY)

    # ── Combine pass 1 + enrichment into final rows ──
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

        if evt_type in ("SetCap", "SubmitCap"):
            cap_val = data.get("cap")
            if cap_val is not None:
                details["cap"] = str(cap_val)
                details["cap_is_zero"] = (int(cap_val) == 0)

        elif evt_type == "ReallocateWithdraw":
            details["withdrawn_assets"] = str(data.get("withdrawnAssets", "0"))
            details["withdrawn_shares"] = str(data.get("withdrawnShares", "0"))

        elif evt_type == "ReallocateSupply":
            details["supplied_assets"] = str(data.get("suppliedAssets", "0"))
            details["supplied_shares"] = str(data.get("suppliedShares", "0"))

        elif evt_type == "SetWithdrawQueue" and q_info:
            queue_keys = q_info.get("keys", [])
            details["queue_size"] = len(queue_keys)
            details["queue_has_toxic"] = any(k in toxic_keys for k in queue_keys)
            details["toxic_in_queue"] = [k for k in queue_keys if k in toxic_keys]

        elif evt_type == "SetSupplyQueue" and q_info:
            queue_keys = q_info.get("keys", [])
            details["queue_size"] = len(queue_keys)
            details["queue_has_toxic"] = any(k in toxic_keys for k in queue_keys)

        touches_toxic = (market_key in toxic_keys) if market_key else False
        if evt_type in ("SetWithdrawQueue", "SetSupplyQueue"):
            touches_toxic = True  # Always relevant — check queue_has_toxic in details

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


# ═══════════════════════════════════════════════════════════════
#  TASK 3: Reallocation Events (3.3)
# ═══════════════════════════════════════════════════════════════

def query_reallocations_for_chain(chain_id: int, chain_name: str,
                                   vault_addresses: List[str],
                                   toxic_keys: Set[str]) -> List[Dict]:
    """
    Query ALL vaultReallocates for affected vaults on a chain,
    then filter for toxic market movements.

    Uses vaultAddress_in + timestamp range to get comprehensive data.
    """
    if not vault_addresses:
        return []

    addrs_str = ", ".join(f'"{a}"' for a in vault_addresses)
    all_rows = []
    skip = 0
    page_size = 1000  # max supported

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
                "realloc_type": item.get("type", ""),  # ReallocateSupply or ReallocateWithdraw
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
    """
    Classify a single vault's curator response based on:
    - When allocation to toxic markets dropped to 0
    - When cap was set to 0
    - When toxic market was removed from queue
    - Timing relative to Nov 4 depeg
    """
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

        # Peak allocation to toxic markets
        peak_row = df_a.loc[df_a["supply_assets_usd"].idxmax()] if len(df_a) > 0 else None
        profile["peak_toxic_supply_usd"] = float(df_a["supply_assets_usd"].max())
        profile["peak_toxic_date"] = peak_row["date"] if peak_row is not None else None

        # First date allocation went to ~0 (< $1)
        after_peak = df_a[df_a["timestamp"] >= df_a.loc[df_a["supply_assets_usd"].idxmax(), "timestamp"]]
        zero_rows = after_peak[after_peak["supply_assets_usd"] < 1.0]
        if len(zero_rows) > 0:
            first_zero = zero_rows.sort_values("timestamp").iloc[0]
            profile["first_zero_alloc_ts"] = int(first_zero["timestamp"])
            profile["first_zero_alloc_date"] = first_zero["date"]
        else:
            profile["first_zero_alloc_ts"] = None
            profile["first_zero_alloc_date"] = None

        # Allocation on depeg day (Nov 4)
        depeg_day = df_a[(df_a["timestamp"] >= DEPEG_TS) & (df_a["timestamp"] < DEPEG_TS + 86400)]
        profile["alloc_at_depeg_usd"] = float(depeg_day["supply_assets_usd"].iloc[0]) if len(depeg_day) > 0 else None

        # Allocation 1 week before depeg (Oct 28)
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

    # Find first SetCap(0) for toxic market
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

    # Find withdraw queue removal of toxic market
    queue_removals = [e for e in vault_admin
                      if e["event_type"] == "SetWithdrawQueue"]
    queue_removed_toxic_ts = None
    for evt in sorted(queue_removals, key=lambda e: e["timestamp"]):
        try:
            d = json.loads(evt["details"])
            if not d.get("queue_has_toxic", True):  # toxic NOT in queue = removal
                queue_removed_toxic_ts = evt["timestamp"]
                break
        except (json.JSONDecodeError, TypeError):
            pass

    profile["queue_removed_toxic_ts"] = queue_removed_toxic_ts
    profile["queue_removed_toxic_date"] = ts_to_date(queue_removed_toxic_ts) if queue_removed_toxic_ts else None

    # Total admin events count
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
    # Use earliest decisive action: first zero alloc, first cap=0, or first realloc withdraw
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

        # No action found — check current status
        if status == "ACTIVE_DEPEG":
            profile["response_class"] = "STAYED_EXPOSED"
        elif status == "HISTORICALLY_EXPOSED":
            # Exited but we couldn't pinpoint exact timing from API data
            profile["response_class"] = "EXITED_TIMING_UNKNOWN"
        elif status in ("FULLY_EXITED", "STOPPED_SUPPLYING"):
            profile["response_class"] = "EXITED_TIMING_UNKNOWN"
        else:
            profile["response_class"] = "UNKNOWN"

    return profile


# ═══════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════

def main():
    print("=" * 80)
    print("Block 3 — Curator Response Analysis")
    print("=" * 80)
    print(f"Timeline: Sept 1 2025 → Jan 31 2026")
    print(f"Depeg:    Nov 4 2025 (Stream Finance / xUSD collapse)")
    print(f"deUSD:    Nov 6 2025 (Elixir crash)")
    print("=" * 80)

    # ── Load Block 1 ──
    df_vaults, df_markets, chain_keys, toxic_keys = load_block1_data()
    vaults = get_unique_vaults(df_vaults)

    print(f"\n📂 Loaded Block 1:")
    print(f"   Vaults:         {len(vaults)}")
    print(f"   Toxic markets:  {len(toxic_keys)}")
    print(f"   Chains:         {list(chain_keys.keys())}")

    out_dir = PROJECT_ROOT / "04-data-exports" / "raw" / "graphql"
    out_dir.mkdir(parents=True, exist_ok=True)

    # ══════════════════════════════════════════════════════════
    #  TASK 1: Historical Allocation Timeseries
    # ══════════════════════════════════════════════════════════
    print(f"\n{'─' * 70}")
    print(f"📊 TASK 1 (3.1): Historical Allocation Timeseries")
    print(f"{'─' * 70}")
    print(f"   Querying daily supply to toxic markets per vault...")

    all_alloc_rows = []
    for idx, v in enumerate(vaults):
        print(f"\n   [{idx+1}/{len(vaults)}] {v['name']} ({v['chain']}) [{v['discovery_method']}]")
        rows = query_vault_allocation_history(v, toxic_keys)
        n_toxic = len(rows)
        if n_toxic > 0:
            # Summarize: date range and peak
            df_tmp = pd.DataFrame(rows)
            peak = df_tmp["supply_assets_usd"].max()
            nonzero = df_tmp[df_tmp["supply_assets_usd"] > 1]
            if len(nonzero) > 0:
                first_d = nonzero["date"].min()
                last_d = nonzero["date"].max()
                print(f"      ✅ {n_toxic} data points, peak ${peak:,.0f}, active {first_d} → {last_d}")
            else:
                print(f"      ✅ {n_toxic} data points, all ~$0 (already exited)")
        else:
            print(f"      ⚠️  No toxic allocation history found")
        all_alloc_rows.extend(rows)
        time.sleep(REQUEST_DELAY)

    if all_alloc_rows:
        df_alloc = pd.DataFrame(all_alloc_rows)
        alloc_path = out_dir / "block3_allocation_timeseries.csv"
        df_alloc.to_csv(alloc_path, index=False)
        print(f"\n✅ Saved {len(all_alloc_rows)} allocation rows to {alloc_path.name}")
    else:
        print(f"\n⚠️  No allocation timeseries data collected")
        df_alloc = pd.DataFrame()

    # ══════════════════════════════════════════════════════════
    #  TASK 2: Admin Events
    # ══════════════════════════════════════════════════════════
    print(f"\n{'─' * 70}")
    print(f"🔧 TASK 2 (3.2): Admin Events (Cap Sets, Queue Changes)")
    print(f"{'─' * 70}")

    all_admin_rows = []
    for idx, v in enumerate(vaults):
        print(f"\n   [{idx+1}/{len(vaults)}] {v['name']} ({v['chain']})")
        rows = query_vault_admin_events(v, toxic_keys)
        toxic_events = [r for r in rows if r["touches_toxic_market"]]
        print(f"      ✅ {len(rows)} total events, {len(toxic_events)} touching toxic markets")

        # Highlight key events
        for evt in toxic_events:
            if evt["event_type"] in ("SetCap", "SubmitCap"):
                try:
                    d = json.loads(evt["details"]) if evt["details"] else {}
                    if d.get("cap_is_zero"):
                        print(f"         🚫 {evt['datetime']}: SetCap → 0 ({evt['collateral_symbol']})")
                except (json.JSONDecodeError, TypeError):
                    pass
            elif evt["event_type"] == "ReallocateWithdraw":
                print(f"         💸 {evt['datetime']}: ReallocateWithdraw ({evt['collateral_symbol']})")
            elif evt["event_type"] == "SetWithdrawQueue":
                try:
                    d = json.loads(evt["details"]) if evt["details"] else {}
                    if not d.get("queue_has_toxic", True):
                        print(f"         ❌ {evt['datetime']}: Toxic market REMOVED from withdraw queue")
                except (json.JSONDecodeError, TypeError):
                    pass

        all_admin_rows.extend(rows)
        time.sleep(REQUEST_DELAY)

    if all_admin_rows:
        df_admin = pd.DataFrame(all_admin_rows)
        admin_path = out_dir / "block3_admin_events.csv"
        df_admin.to_csv(admin_path, index=False)
        print(f"\n✅ Saved {len(all_admin_rows)} admin events to {admin_path.name}")

        toxic_admin = df_admin[df_admin["touches_toxic_market"]]
        print(f"   Toxic-related events: {len(toxic_admin)}")
        if len(toxic_admin) > 0:
            print(f"   Event types: {dict(toxic_admin['event_type'].value_counts())}")
    else:
        print(f"\n⚠️  No admin events collected")

    # ══════════════════════════════════════════════════════════
    #  TASK 3: Reallocation Events
    # ══════════════════════════════════════════════════════════
    print(f"\n{'─' * 70}")
    print(f"🔄 TASK 3 (3.3): Reallocation Events")
    print(f"{'─' * 70}")

    # Build chain_id → name mapping from Block 1 data
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

    # ── Classification breakdown ──
    print(f"\n{'─' * 70}")
    print(f"  RESPONSE CLASSIFICATION")
    print(f"{'─' * 70}")

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

    # ── Key curator timelines ──
    print(f"\n{'─' * 70}")
    print(f"  KEY CURATOR ACTION TIMELINES")
    print(f"{'─' * 70}")

    # Group by curator
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

    # ── Allocation at depeg ──
    print(f"\n{'─' * 70}")
    print(f"  ALLOCATION TO TOXIC MARKETS AT DEPEG (Nov 4)")
    print(f"{'─' * 70}")
    has_depeg_alloc = df_p[df_p["alloc_at_depeg_usd"].notna() & (df_p["alloc_at_depeg_usd"] > 0)]
    if len(has_depeg_alloc) > 0:
        for _, r in has_depeg_alloc.sort_values("alloc_at_depeg_usd", ascending=False).iterrows():
            peak = r.get("peak_toxic_supply_usd", 0)
            at_depeg = r["alloc_at_depeg_usd"]
            week_before = r.get("alloc_week_before_usd")
            print(f"  {r['vault_name']} ({r['curator_name']}): "
                  f"at_depeg=${at_depeg:,.0f}  peak=${peak:,.0f}  "
                  f"week_before=${week_before:,.0f}" if pd.notna(week_before) else "")
    else:
        print(f"  No vaults with measurable toxic allocation on Nov 4")
        print(f"  (Allocation history may not resolve to exact depeg day)")

    # ── Already exited before depeg ──
    print(f"\n{'─' * 70}")
    print(f"  EXITED BEFORE DEPEG (proactive/early)")
    print(f"{'─' * 70}")
    early = df_p[df_p["response_class"].isin(["PROACTIVE", "EARLY_REACTOR"])]
    if len(early) > 0:
        total_saved = early["peak_toxic_supply_usd"].sum()
        print(f"  {len(early)} vaults exited early, peak exposure: ${total_saved:,.0f}")
        for _, r in early.sort_values("peak_toxic_supply_usd", ascending=False).iterrows():
            peak = r.get("peak_toxic_supply_usd", 0)
            print(f"    {r['vault_name']} ({r['curator_name']}): "
                  f"peak=${peak:,.0f}  exited={r.get('first_zero_alloc_date', 'unknown')}")
    else:
        print(f"  No vaults classified as proactive or early reactor")

    # ── Still exposed ──
    print(f"\n{'─' * 70}")
    print(f"  STILL EXPOSED (never fully exited)")
    print(f"{'─' * 70}")
    stayed = df_p[df_p["response_class"] == "STAYED_EXPOSED"]
    if len(stayed) > 0:
        for _, r in stayed.iterrows():
            print(f"  {r['vault_name']} ({r['curator_name']}): "
                  f"TVL=${r['vault_tvl_usd']:,.0f}  status={r['exposure_status']}")
    else:
        print(f"  All vaults have taken some action")

    print(f"\n{'═' * 70}")
    print(f"  ✅ Block 3 complete — 4 CSVs saved")
    print(f"{'═' * 70}")


if __name__ == "__main__":
    main()
