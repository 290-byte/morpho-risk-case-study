"""
Block 7 — Vault TVL Timeseries for Damaged Vaults

Queries daily TVL (totalAssets, totalAssetsUsd) for vaults that suffered
share price drawdowns during the depeg. Used to visualize the bank-run
progression and distinguish TVL decline (withdrawals) from actual bad debt.

The individual deposit/withdraw transactions are NOT fetched — the daily
TVL timeseries from this block, combined with allocation data from block3_A1,
is sufficient to prove the bank-run narrative:

  Relend TVL:     $43.3M → $63K  (block7 TVL timeseries)
  Toxic exposure: $4.4M          (block3 allocation timeseries)
  Safe withdrawals: ~$39M        (subtraction)

Input:  block2_share_price_summary.csv (identifies damaged vaults — drawdown > 1%)
Output: block7_vault_tvl_daily.csv     (daily TVL timeseries for damaged vaults)
"""

import time
import requests
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone
from dotenv import load_dotenv
from typing import List, Dict

# ── Project paths (runner patches PROJECT_ROOT to repo root) ──
PROJECT_ROOT = Path(__file__).parent.parent
env_path = PROJECT_ROOT / '.env'
load_dotenv(dotenv_path=env_path)

GRAPHQL_URL = "https://blue-api.morpho.org/graphql"
REQUEST_DELAY = 0.3

# ── Time window: Oct 1 2025 → Jan 31 2026 (the depeg was Nov 2025) ──
TS_OCT_01   = 1759276800   # 2025-10-01 00:00:00 UTC
TS_JAN_31   = 1769903999   # 2026-01-31 23:59:59 UTC

# ── Fallback: hardcoded damaged vaults if block2 summary unavailable ──
FALLBACK_DAMAGED_VAULTS = [
    # Relend USDC (Ethereum) — 98.4% haircut
    {"address": "0x0f359fd18bda75e9c49bc027e7da59a4b01bf32a", "chain_id": 1, "name": "Relend USDC"},
    # MEV Capital USDC (Ethereum) — 3.5% haircut
    {"address": "0xd63070114470f685b75b74d60eec7c1113d33a3d", "chain_id": 1, "name": "MEV Capital USDC (Eth)"},
    # MEV Capital USDC (Arbitrum) — ~12% unrealized (V1.1)
    {"address": "0xa60643c90a542a95026c0f1dbdb0615ff42019cf", "chain_id": 42161, "name": "MEV Capital USDC (Arb)"},
]

DRAWDOWN_THRESHOLD = 0.01  # 1%


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


def load_damaged_vaults() -> List[Dict]:
    """
    Identify damaged vaults from block2 share price summary.
    Falls back to hardcoded list if block2 data unavailable.
    """
    gql_dir = PROJECT_ROOT / "data"
    summary_path = gql_dir / "block2_share_price_summary.csv"

    if summary_path.exists():
        df = pd.read_csv(summary_path)
        df["max_drawdown_pct"] = pd.to_numeric(df.get("max_drawdown_pct", 0), errors="coerce").fillna(0)
        damaged = df[df["max_drawdown_pct"] > DRAWDOWN_THRESHOLD].copy()

        if not damaged.empty:
            vaults = []
            for _, r in damaged.iterrows():
                addr = str(r.get("vault_address", "")).lower()
                cid = int(r.get("chain_id", 0))
                name = r.get("vault_name", "Unknown")
                vaults.append({"address": addr, "chain_id": cid, "name": name})

            print(f"  📊 Found {len(vaults)} damaged vaults from block2_share_price_summary.csv")
            for v in vaults:
                dd = damaged[damaged["vault_address"].str.lower() == v["address"]]["max_drawdown_pct"].iloc[0]
                print(f"     {v['name']}: {dd:.2%} drawdown")
            return vaults

    print(f"  ⚠ block2_share_price_summary.csv not found — using hardcoded vault list")
    return FALLBACK_DAMAGED_VAULTS


def fetch_vault_tvl_timeseries(address: str, chain_id: int, name: str) -> List[Dict]:
    """
    Fetch daily TVL (totalAssets + totalAssetsUsd) timeseries via vaultByAddress.
    One fast GraphQL call per vault — no pagination needed.
    """
    query = f"""
    {{
      vaultByAddress(address: "{address}", chainId: {chain_id}) {{
        address
        name
        state {{
          totalAssetsUsd
          totalAssets
          totalSupply
        }}
        historicalState {{
          totalAssetsUsd(options: {{
            startTimestamp: {TS_OCT_01}
            endTimestamp: {TS_JAN_31}
            interval: DAY
          }}) {{ x y }}
          totalAssets(options: {{
            startTimestamp: {TS_OCT_01}
            endTimestamp: {TS_JAN_31}
            interval: DAY
          }}) {{ x y }}
        }}
      }}
    }}
    """

    print(f"  Querying TVL timeseries for {name}...")
    data = query_graphql(query)
    time.sleep(REQUEST_DELAY)

    vault = data.get("data", {}).get("vaultByAddress", {})
    if not vault:
        print(f"    ⚠ No data returned")
        return []

    hist = vault.get("historicalState", {})
    tvl_usd_points = hist.get("totalAssetsUsd", []) or []
    raw_points = hist.get("totalAssets", []) or []

    raw_lookup = {int(p["x"]): int(p["y"]) for p in raw_points
                  if p.get("x") and p.get("y") is not None}

    rows = []
    for p in tvl_usd_points:
        ts = int(p["x"])
        tvl_usd = float(p["y"]) if p["y"] else 0
        raw_ta = raw_lookup.get(ts, 0)

        rows.append({
            "vault_address": address,
            "vault_name": name,
            "chain_id": chain_id,
            "timestamp": ts,
            "date": ts_to_date(ts),
            "tvl_usd": tvl_usd,
            "total_assets_raw": raw_ta,
        })

    if rows:
        peak = max(r["tvl_usd"] for r in rows)
        latest = rows[-1]["tvl_usd"]
        print(f"    ✅ {len(rows)} daily points — peak ${peak:,.0f} → latest ${latest:,.0f}")
    else:
        print(f"    ⚠ No TVL data")

    return rows


def main():
    print("=" * 60)
    print("Block 7: Vault TVL Timeseries (Bank-Run Analysis)")
    print("=" * 60)

    damaged_vaults = load_damaged_vaults()
    if not damaged_vaults:
        print("  ❌ No damaged vaults found — nothing to query")
        return

    output_dir = PROJECT_ROOT / "data"
    output_dir.mkdir(parents=True, exist_ok=True)

    all_tvl = []
    for vault in damaged_vaults:
        tvl = fetch_vault_tvl_timeseries(vault["address"], vault["chain_id"], vault["name"])
        all_tvl.extend(tvl)

    if all_tvl:
        df_tvl = pd.DataFrame(all_tvl)
        tvl_path = output_dir / "block7_vault_tvl_daily.csv"
        df_tvl.to_csv(tvl_path, index=False)
        print(f"\n  ✅ Saved: {tvl_path.name} ({len(df_tvl)} rows)")
    else:
        print(f"\n  ⚠ No TVL data found")

    print(f"\n{'═' * 60}")
    print(f"  ✅ Block 7 complete — {len(damaged_vaults)} vaults, {len(all_tvl)} data points")
    print(f"     Combine with block3_allocation_timeseries.csv to decompose:")
    print(f"       TVL decline  = safe withdrawals + trapped capital")
    print(f"{'═' * 60}")


if __name__ == "__main__":
    main()
