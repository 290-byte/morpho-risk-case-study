"""
Block 3b — Liquidity Stress Analysis

Extension of Block 3 (Curator Response): measures the IMPACT on vault liquidity.

TASK 1: Market Utilization Timeseries
  - Hourly utilization, supply, borrow, liquidity per toxic market (Nov 1-15 zoom)
  - Daily utilization for wider context (Sept 1 → Jan 31)
  - Shows ramp to 100% utilization = depositors locked in
  - Output: block3_market_utilization_hourly.csv, block3_market_utilization_daily.csv

TASK 2: Vault Net Flows
  - Derived from Block 2 share price data (totalAssetsUsd daily)
  - Daily net flow = TVL[t] - TVL[t-1]
  - Withdrawal pressure metric = net_flow / TVL[t-1]
  - Output: block3_vault_net_flows.csv

TASK 3: Stress Comparison Table
  - Combines: share price drawdown, TVL drawdown, peak utilization, toxic allocation %
  - Per-vault stress score
  - Output: block3_stress_comparison.csv

Input:  04-data-exports/raw/graphql/block1_markets_graphql.csv
        04-data-exports/raw/graphql/block2_share_prices_daily.csv
        04-data-exports/raw/graphql/block2_share_price_summary.csv
        04-data-exports/raw/graphql/block3_allocation_timeseries.csv
        04-data-exports/raw/graphql/block3_curator_profiles.csv
"""

import time
import requests
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timezone
from dotenv import load_dotenv
from typing import List, Dict, Tuple

# ── Project paths ──
PROJECT_ROOT = Path(__file__).parent.parent
env_path = PROJECT_ROOT / '.env'
load_dotenv(dotenv_path=env_path)

GRAPHQL_URL = "https://blue-api.morpho.org/graphql"
REQUEST_DELAY = 0.3

# ── Time windows ──
TS_OCT_01   = 1759276800
TS_NOV_01   = 1761955200
TS_NOV_04   = 1762214400   # xUSD depeg
TS_NOV_06   = 1762387200   # deUSD crash
TS_NOV_15   = 1763164800
TS_NOV_30   = 1764547199

DEPEG_TS = TS_NOV_04


def query_graphql(query: str) -> dict:
    headers = {"Content-Type": "application/json"}
    for attempt in range(3):
        try:
            resp = requests.post(GRAPHQL_URL, json={"query": query}, headers=headers, timeout=60)
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


# ═══════════════════════════════════════════════════════════════
#  TASK 1: Market Utilization Timeseries
# ═══════════════════════════════════════════════════════════════

def query_market_utilization(market_id: str, chain_id: int, collateral: str,
                              loan: str, chain: str,
                              start_ts: int, end_ts: int,
                              interval: str) -> List[Dict]:
    """
    Query utilization + supply/borrow/liquidity for a market.
    Returns list of data point dicts.
    """
    query = f"""
    {{
      marketByUniqueKey(uniqueKey: "{market_id}", chainId: {chain_id}) {{
        uniqueKey
        historicalState {{
          utilization(options: {{
            startTimestamp: {start_ts}
            endTimestamp: {end_ts}
            interval: {interval}
          }}) {{ x, y }}
          supplyAssetsUsd(options: {{
            startTimestamp: {start_ts}
            endTimestamp: {end_ts}
            interval: {interval}
          }}) {{ x, y }}
          borrowAssetsUsd(options: {{
            startTimestamp: {start_ts}
            endTimestamp: {end_ts}
            interval: {interval}
          }}) {{ x, y }}
          liquidityAssetsUsd(options: {{
            startTimestamp: {start_ts}
            endTimestamp: {end_ts}
            interval: {interval}
          }}) {{ x, y }}
        }}
      }}
    }}
    """

    result = query_graphql(query)

    if "errors" in result:
        err = result["errors"][0].get("message", "")
        print(f"      ❌ Error: {err[:120]}")
        return []

    market_data = result.get("data", {}).get("marketByUniqueKey")
    if not market_data:
        print(f"      ⚠️  Market not found")
        return []

    hist = market_data.get("historicalState", {})
    util_pts = hist.get("utilization", []) or []
    supply_pts = hist.get("supplyAssetsUsd", []) or []
    borrow_pts = hist.get("borrowAssetsUsd", []) or []
    liq_pts = hist.get("liquidityAssetsUsd", []) or []

    # Build lookup dicts
    supply_by_ts = {int(p["x"]): p["y"] for p in supply_pts if p and p.get("y") is not None}
    borrow_by_ts = {int(p["x"]): p["y"] for p in borrow_pts if p and p.get("y") is not None}
    liq_by_ts = {int(p["x"]): p["y"] for p in liq_pts if p and p.get("y") is not None}

    rows = []
    for pt in util_pts:
        if not pt or pt.get("y") is None:
            continue
        ts = int(pt["x"])
        rows.append({
            "market_unique_key": market_id,
            "chain": chain,
            "chain_id": chain_id,
            "collateral_symbol": collateral,
            "loan_symbol": loan,
            "timestamp": ts,
            "date": ts_to_date(ts),
            "datetime": ts_to_datetime(ts),
            "utilization": float(pt["y"]),
            "supply_assets_usd": float(supply_by_ts.get(ts, 0)),
            "borrow_assets_usd": float(borrow_by_ts.get(ts, 0)),
            "liquidity_assets_usd": float(liq_by_ts.get(ts, 0)),
        })

    return rows


# ═══════════════════════════════════════════════════════════════
#  TASK 2: Vault Net Flows (from existing Block 2 data)
# ═══════════════════════════════════════════════════════════════

def compute_vault_net_flows(df_share_prices: pd.DataFrame) -> pd.DataFrame:
    """
    Derive daily net flows from Block 2 share price data.
    net_flow[t] = totalAssetsUsd[t] - totalAssetsUsd[t-1]
    """
    # Block 2 daily has: vault_address, vault_name, timestamp, date, total_assets_usd, share_price
    # Column names may vary — handle both conventions
    if "total_assets_usd" not in df_share_prices.columns:
        # Try alternative column names from Block 2
        for col in ["totalAssetsUsd", "tvl_usd"]:
            if col in df_share_prices.columns:
                df_share_prices = df_share_prices.rename(columns={col: "total_assets_usd"})
                break

    if "total_assets_usd" not in df_share_prices.columns:
        print("  ⚠️  Cannot find TVL column in share price data")
        return pd.DataFrame()

    df = df_share_prices.copy()
    df["total_assets_usd"] = pd.to_numeric(df["total_assets_usd"], errors="coerce")
    df["timestamp"] = pd.to_numeric(df["timestamp"], errors="coerce")
    df = df.sort_values(["vault_address", "timestamp"])

    all_flows = []
    for vault_addr, group in df.groupby("vault_address"):
        g = group.sort_values("timestamp").reset_index(drop=True)

        if len(g) < 2:
            continue

        vault_name = g["vault_name"].iloc[0]

        for i in range(1, len(g)):
            prev_tvl = g.loc[i - 1, "total_assets_usd"]
            curr_tvl = g.loc[i, "total_assets_usd"]

            if pd.isna(prev_tvl) or pd.isna(curr_tvl) or prev_tvl == 0:
                continue

            net_flow = curr_tvl - prev_tvl
            flow_pct = (net_flow / prev_tvl) * 100

            all_flows.append({
                "vault_address": vault_addr,
                "vault_name": vault_name,
                "timestamp": int(g.loc[i, "timestamp"]),
                "date": g.loc[i, "date"],
                "total_assets_usd": curr_tvl,
                "prev_total_assets_usd": prev_tvl,
                "net_flow_usd": net_flow,
                "net_flow_pct": flow_pct,
                "is_withdrawal": net_flow < 0,
            })

    return pd.DataFrame(all_flows)


# ═══════════════════════════════════════════════════════════════
#  TASK 3: Stress Comparison Table
# ═══════════════════════════════════════════════════════════════

def build_stress_comparison(
    df_share_summary: pd.DataFrame,
    df_net_flows: pd.DataFrame,
    df_profiles: pd.DataFrame,
    df_alloc: pd.DataFrame,
    df_util_hourly: pd.DataFrame,
) -> pd.DataFrame:
    """
    Combine per-vault metrics into a single stress comparison table.
    """
    rows = []

    # Index dataframes for fast lookup
    if len(df_net_flows) > 0:
        # Get depeg-week net flows per vault
        df_nf = df_net_flows.copy()
        df_nf["vault_address"] = df_nf["vault_address"].str.lower()
        depeg_flows = df_nf[
            (df_nf["timestamp"] >= TS_NOV_04) &
            (df_nf["timestamp"] <= TS_NOV_15)
        ]
        depeg_flow_by_vault = depeg_flows.groupby("vault_address").agg(
            total_net_flow=("net_flow_usd", "sum"),
            withdrawal_days=("is_withdrawal", "sum"),
            max_daily_outflow_pct=("net_flow_pct", "min"),  # min because withdrawals are negative
        ).to_dict("index")
    else:
        depeg_flow_by_vault = {}

    # Get peak toxic allocation per vault from block3
    if len(df_alloc) > 0:
        df_alloc_copy = df_alloc.copy()
        df_alloc_copy["vault_address"] = df_alloc_copy["vault_address"].str.lower()
        peak_alloc = df_alloc_copy.groupby("vault_address").agg(
            peak_toxic_usd=("supply_assets_usd", "max"),
        ).to_dict("index")
    else:
        peak_alloc = {}

    for _, row in df_share_summary.iterrows():
        vault_addr = str(row.get("vault_address", "")).lower()
        vault_name = row.get("vault_name", "")

        # Share price metrics (from Block 2 summary)
        # max_drawdown_pct is stored as 0-1 decimal (0.984 = 98.4% loss)
        share_drawdown_raw = float(row.get("max_drawdown_pct", 0) or 0)
        share_drawdown_pct = share_drawdown_raw * 100  # convert to percentage

        peak_tvl = float(row.get("tvl_at_peak_usd", 0) or 0)
        trough_tvl = float(row.get("tvl_at_trough_usd", 0) or row.get("vault_tvl_usd", 0) or 0)
        tvl_drawdown_pct = ((peak_tvl - trough_tvl) / peak_tvl * 100) if peak_tvl > 0 else 0

        # Net flow metrics
        flow_info = depeg_flow_by_vault.get(vault_addr, {})
        depeg_net_flow = flow_info.get("total_net_flow", None)
        withdrawal_days = flow_info.get("withdrawal_days", None)
        max_daily_outflow = flow_info.get("max_daily_outflow_pct", None)

        # Toxic allocation as % of TVL
        alloc_info = peak_alloc.get(vault_addr, {})
        peak_toxic_usd = alloc_info.get("peak_toxic_usd", 0)
        toxic_pct_of_tvl = (peak_toxic_usd / peak_tvl * 100) if peak_tvl > 0 else 0

        # Curator profile info
        profile_match = df_profiles[df_profiles["vault_address"].str.lower() == vault_addr]
        if len(profile_match) > 0:
            p = profile_match.iloc[0]
            curator = p.get("curator_name", "Unknown")
            response_class = p.get("response_class", "UNKNOWN")
            alloc_val = p.get("alloc_at_depeg_usd", 0)
            alloc_at_depeg = float(alloc_val) if pd.notna(alloc_val) else 0
        else:
            curator = "Unknown"
            response_class = "UNKNOWN"
            alloc_at_depeg = 0

        # Stress score (0-100): weighted combination of metrics
        # Higher = more stressed
        score_components = []

        # Share price drawdown (max 40 pts)
        sp_score = min(abs(share_drawdown_pct) * 10, 40)
        score_components.append(sp_score)

        # TVL drawdown (max 25 pts)
        tvl_score = min(tvl_drawdown_pct / 4, 25)
        score_components.append(tvl_score)

        # Toxic allocation % (max 20 pts)
        toxic_score = min(toxic_pct_of_tvl / 5, 20)
        score_components.append(toxic_score)

        # Still exposed at depeg (max 15 pts)
        depeg_score = 15 if alloc_at_depeg > 1000 else (5 if alloc_at_depeg > 0 else 0)
        score_components.append(depeg_score)

        stress_score = round(sum(score_components), 1)

        rows.append({
            "vault_address": vault_addr,
            "vault_name": vault_name,
            "chain": row.get("chain", ""),
            "curator_name": curator,
            "response_class": response_class,
            # TVL metrics
            "peak_tvl_usd": peak_tvl,
            "trough_tvl_usd": trough_tvl,
            "tvl_drawdown_pct": round(tvl_drawdown_pct, 2),
            # Share price (now in percentage terms)
            "share_price_drawdown_pct": round(share_drawdown_pct, 2),
            # Net flows (depeg week)
            "depeg_week_net_flow_usd": round(depeg_net_flow, 0) if depeg_net_flow is not None else None,
            "depeg_week_withdrawal_days": int(withdrawal_days) if withdrawal_days is not None else None,
            "max_daily_outflow_pct": round(max_daily_outflow, 2) if max_daily_outflow is not None else None,
            # Toxic exposure
            "peak_toxic_alloc_usd": round(peak_toxic_usd, 0),
            "toxic_pct_of_tvl": round(toxic_pct_of_tvl, 2),
            "alloc_at_depeg_usd": round(alloc_at_depeg, 0),
            # Stress score
            "stress_score": stress_score,
        })

    df_stress = pd.DataFrame(rows)
    df_stress = df_stress.sort_values("stress_score", ascending=False)
    return df_stress


# ═══════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════

def main():
    print("=" * 80)
    print("Block 3b — Liquidity Stress Analysis")
    print("=" * 80)

    gql_dir = PROJECT_ROOT / "data"

    # ── Load existing data ──
    markets_path = gql_dir / "block1_markets_graphql.csv"
    share_daily_path = gql_dir / "block2_share_prices_daily.csv"
    share_summary_path = gql_dir / "block2_share_price_summary.csv"
    alloc_path = gql_dir / "block3_allocation_timeseries.csv"
    profiles_path = gql_dir / "block3_curator_profiles.csv"

    df_markets = pd.read_csv(markets_path) if markets_path.exists() else pd.DataFrame()
    df_share_daily = pd.read_csv(share_daily_path) if share_daily_path.exists() else pd.DataFrame()
    df_share_summary = pd.read_csv(share_summary_path) if share_summary_path.exists() else pd.DataFrame()
    df_alloc = pd.read_csv(alloc_path) if alloc_path.exists() else pd.DataFrame()
    df_profiles = pd.read_csv(profiles_path) if profiles_path.exists() else pd.DataFrame()

    print(f"\n📂 Loaded existing data:")
    print(f"   Markets:            {len(df_markets)} rows")
    print(f"   Share prices daily: {len(df_share_daily)} rows")
    print(f"   Share price summary:{len(df_share_summary)} rows")
    print(f"   Allocation ts:      {len(df_alloc)} rows")
    print(f"   Curator profiles:   {len(df_profiles)} rows")

    # ══════════════════════════════════════════════════════════
    #  TASK 1: Market Utilization Timeseries
    # ══════════════════════════════════════════════════════════
    print(f"\n{'─' * 70}")
    print(f"📊 TASK 1: Market Utilization Timeseries")
    print(f"{'─' * 70}")

    all_hourly = []
    all_daily = []

    for idx, (_, mkt) in enumerate(df_markets.iterrows()):
        market_id = mkt["market_id"]
        chain_id = int(mkt["chain_id"])
        chain = mkt.get("chain", "")
        collateral = mkt.get("collateral_symbol", "?")
        loan = mkt.get("loan_symbol", "?")

        print(f"\n   [{idx+1}/{len(df_markets)}] {collateral}/{loan} ({chain})")
        print(f"      Market: {market_id[:16]}...")

        # Hourly: Nov 1-15 (depeg zoom)
        print(f"      Hourly (Nov 1-15)...")
        hourly = query_market_utilization(
            market_id, chain_id, collateral, loan, chain,
            TS_NOV_01, TS_NOV_15, "HOUR"
        )
        if hourly:
            # Find peak utilization
            peak_util = max(r["utilization"] for r in hourly)
            hrs_at_100 = sum(1 for r in hourly if r["utilization"] >= 0.999)
            print(f"      ✅ {len(hourly)} hourly pts, peak util={peak_util:.4f}, "
                  f"hours at 100%={hrs_at_100}")
        else:
            print(f"      ⚠️  No hourly data")
        all_hourly.extend(hourly)

        time.sleep(REQUEST_DELAY)

        # Daily: Sept 1 → Jan 31 (full context)
        print(f"      Daily (Sept 1 → Jan 31)...")
        daily = query_market_utilization(
            market_id, chain_id, collateral, loan, chain,
            TS_OCT_01, TS_NOV_30, "DAY"
        )
        if daily:
            print(f"      ✅ {len(daily)} daily pts")
        else:
            print(f"      ⚠️  No daily data")
        all_daily.extend(daily)

        time.sleep(REQUEST_DELAY)

    # Save utilization data
    if all_hourly:
        df_hourly = pd.DataFrame(all_hourly)
        hourly_path = gql_dir / "block3_market_utilization_hourly.csv"
        df_hourly.to_csv(hourly_path, index=False)
        print(f"\n✅ Saved {len(all_hourly)} hourly utilization rows to {hourly_path.name}")
    else:
        df_hourly = pd.DataFrame()

    if all_daily:
        df_daily_util = pd.DataFrame(all_daily)
        daily_path = gql_dir / "block3_market_utilization_daily.csv"
        df_daily_util.to_csv(daily_path, index=False)
        print(f"✅ Saved {len(all_daily)} daily utilization rows to {daily_path.name}")
    else:
        df_daily_util = pd.DataFrame()

    # ── Utilization summary ──
    if len(df_hourly) > 0:
        print(f"\n{'─' * 70}")
        print(f"  UTILIZATION SUMMARY (Nov 1-15 hourly)")
        print(f"{'─' * 70}")

        for (mk, chain), grp in df_hourly.groupby(["market_unique_key", "chain"]):
            collateral = grp["collateral_symbol"].iloc[0]
            loan = grp["loan_symbol"].iloc[0]
            peak_util = grp["utilization"].max()
            avg_util = grp["utilization"].mean()
            hrs_100 = (grp["utilization"] >= 0.999).sum()
            peak_supply = grp["supply_assets_usd"].max()
            min_liq = grp["liquidity_assets_usd"].min()

            status = "🔴 LOCKED" if hrs_100 > 0 else ("🟡 HIGH" if peak_util > 0.9 else "🟢 OK")

            print(f"\n  {status} {collateral}/{loan} ({chain})")
            print(f"    Peak util: {peak_util:.4f}  Avg: {avg_util:.4f}  "
                  f"Hours@100%: {hrs_100}")
            print(f"    Peak supply: ${peak_supply:,.0f}  Min liquidity: ${min_liq:,.0f}")

    # ══════════════════════════════════════════════════════════
    #  TASK 2: Vault Net Flows
    # ══════════════════════════════════════════════════════════
    print(f"\n{'─' * 70}")
    print(f"💸 TASK 2: Vault Net Flows")
    print(f"{'─' * 70}")

    if len(df_share_daily) > 0:
        df_net_flows = compute_vault_net_flows(df_share_daily)

        if len(df_net_flows) > 0:
            flows_path = gql_dir / "block3_vault_net_flows.csv"
            df_net_flows.to_csv(flows_path, index=False)
            print(f"\n✅ Saved {len(df_net_flows)} net flow rows to {flows_path.name}")

            # Depeg week summary
            depeg_flows = df_net_flows[
                (df_net_flows["timestamp"] >= TS_NOV_04) &
                (df_net_flows["timestamp"] <= TS_NOV_15)
            ]

            if len(depeg_flows) > 0:
                print(f"\n{'─' * 70}")
                print(f"  NET FLOWS DURING DEPEG WEEK (Nov 4-15)")
                print(f"{'─' * 70}")

                for vault_addr, grp in depeg_flows.groupby("vault_address"):
                    vault_name = grp["vault_name"].iloc[0]
                    total_flow = grp["net_flow_usd"].sum()
                    n_withdrawals = grp["is_withdrawal"].sum()
                    max_outflow = grp["net_flow_pct"].min()  # most negative = biggest outflow
                    start_tvl = grp["prev_total_assets_usd"].iloc[0]

                    # Only show vaults with significant flows
                    if abs(total_flow) < 1000 and start_tvl < 10000:
                        continue

                    flow_pct = (total_flow / start_tvl * 100) if start_tvl > 0 else 0
                    direction = "📉" if total_flow < 0 else "📈"

                    print(f"\n  {direction} {vault_name}")
                    print(f"    Net flow: ${total_flow:+,.0f} ({flow_pct:+.1f}%)  "
                          f"Withdrawal days: {n_withdrawals}/{len(grp)}")
                    print(f"    Max daily outflow: {max_outflow:+.1f}%  "
                          f"Start TVL: ${start_tvl:,.0f}")
        else:
            print(f"  ⚠️  Could not compute net flows")
            df_net_flows = pd.DataFrame()
    else:
        print(f"  ⚠️  No Block 2 share price data to derive flows from")
        df_net_flows = pd.DataFrame()

    # ══════════════════════════════════════════════════════════
    #  TASK 3: Stress Comparison Table
    # ══════════════════════════════════════════════════════════
    print(f"\n{'─' * 70}")
    print(f"📊 TASK 3: Stress Comparison Table")
    print(f"{'─' * 70}")

    if len(df_share_summary) > 0:
        df_stress = build_stress_comparison(
            df_share_summary, df_net_flows, df_profiles, df_alloc, df_hourly
        )

        if len(df_stress) > 0:
            stress_path = gql_dir / "block3_stress_comparison.csv"
            df_stress.to_csv(stress_path, index=False)
            print(f"\n✅ Saved {len(df_stress)} vault stress profiles to {stress_path.name}")

            print(f"\n{'─' * 70}")
            print(f"  VAULT STRESS RANKING")
            print(f"{'─' * 70}")

            for _, r in df_stress.head(15).iterrows():
                score = r["stress_score"]
                name = r["vault_name"]
                curator = r["curator_name"]
                sp_dd = r["share_price_drawdown_pct"]
                tvl_dd = r["tvl_drawdown_pct"]
                toxic_pct = r["toxic_pct_of_tvl"]
                peak_tvl = r["peak_tvl_usd"]
                cls = r["response_class"]

                if score >= 40:
                    icon = "🔴"
                elif score >= 15:
                    icon = "🟡"
                else:
                    icon = "🟢"

                print(f"\n  {icon} Score {score:5.1f} | {name} ({curator})")
                print(f"    SP drawdown: {sp_dd:.2f}%  TVL drawdown: {tvl_dd:.1f}%  "
                      f"Toxic%TVL: {toxic_pct:.1f}%  Peak TVL: ${peak_tvl:,.0f}")
                alloc_depeg = r['alloc_at_depeg_usd']
                alloc_str = f"${alloc_depeg:,.0f}" if pd.notna(alloc_depeg) else "N/A"
                print(f"    Class: {cls}  Alloc@depeg: {alloc_str}")

            # Summary stats
            n_red = (df_stress["stress_score"] >= 40).sum()
            n_yellow = ((df_stress["stress_score"] >= 15) & (df_stress["stress_score"] < 40)).sum()
            n_green = (df_stress["stress_score"] < 15).sum()
            print(f"\n  Summary: 🔴 {n_red} critical  🟡 {n_yellow} moderate  🟢 {n_green} low stress")

    else:
        print(f"  ⚠️  No Block 2 summary data for stress table")

    # ══════════════════════════════════════════════════════════
    #  FINAL SUMMARY
    # ══════════════════════════════════════════════════════════
    print(f"\n{'═' * 70}")
    print(f"  ✅ Block 3b complete")
    print(f"{'═' * 70}")
    print(f"  Outputs:")
    print(f"    block3_market_utilization_hourly.csv")
    print(f"    block3_market_utilization_daily.csv")
    print(f"    block3_vault_net_flows.csv")
    print(f"    block3_stress_comparison.csv")
    print(f"{'═' * 70}")


if __name__ == "__main__":
    main()
