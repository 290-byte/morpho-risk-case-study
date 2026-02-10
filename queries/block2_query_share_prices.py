"""
Block 2.3 — Share Price Impact (Bad Debt Socialization)

Queries historical share prices and TVL for each exposed vault via vaultByAddress.
A sudden share price drop = direct evidence of bad debt socialization.

Key insight: Morpho Vault V1 mechanism — when bad debt realizes, totalAssets decreases
while totalSupply stays the same → share price drops → all depositors absorb loss proportionally.

Input:  04-data-exports/raw/graphql/block1_vaults_graphql.csv
Output: 04-data-exports/raw/graphql/block2_share_prices_daily.csv
        04-data-exports/raw/graphql/block2_share_prices_hourly.csv  (Nov 1-15 zoom)
        04-data-exports/raw/graphql/block2_share_price_summary.csv  (per-vault stats)
"""

import time
import requests
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone
from dotenv import load_dotenv
from typing import List, Dict, Optional

# Script lives at: 03-queries/block2-bad-debt/graphsql/script.py → 4 levels to /app/
PROJECT_ROOT = Path(__file__).parent.parent
env_path = PROJECT_ROOT / '.env'
load_dotenv(dotenv_path=env_path)

GRAPHQL_URL = "https://blue-api.morpho.org/graphql"
REQUEST_DELAY = 0.3  # seconds between API calls

# ── Time windows ──
# Daily: Sept 1 2025 → Jan 31 2026 (full story arc)
DAILY_START = 1756684800   # Sept 1 2025 00:00 UTC
DAILY_END   = 1769817600   # Jan 31 2026 00:00 UTC

# Hourly: Nov 1-15 2025 (zoomed depeg window)
HOURLY_START = 1761955200  # Nov 1 2025 00:00 UTC
HOURLY_END   = 1763164800  # Nov 15 2025 00:00 UTC

# Key event timestamps
DEPEG_TS        = 1762214400  # Nov 4 2025 (Stream Finance collapse)
ELIXIR_CRASH_TS = 1762387200  # Nov 6 2025 (deUSD crash)
MARKET_REMOVAL  = 1762905600  # Nov 12 2025 (MEV Capital completes delisting)


def query_graphql(query: str, variables: dict = None) -> dict:
    """Execute GraphQL query against Morpho API"""
    headers = {"Content-Type": "application/json"}
    payload = {"query": query}
    if variables:
        payload["variables"] = variables
    response = requests.post(GRAPHQL_URL, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def ts_to_date(ts: float) -> str:
    """Convert unix timestamp to ISO date string"""
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")


def ts_to_datetime(ts: float) -> str:
    """Convert unix timestamp to ISO datetime string"""
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")


def fetch_vault_history(address: str, chain_id: int, interval: str,
                        start_ts: int, end_ts: int) -> Optional[Dict]:
    """
    Fetch vault historical state via vaultByAddress.
    Returns current state + timeseries for share price and TVL.
    """
    query = f"""
    {{
      vaultByAddress(address: "{address}", chainId: {chain_id}) {{
        address
        name
        symbol
        listed
        asset {{
          symbol
          decimals
          priceUsd
        }}
        chain {{
          id
          network
        }}
        state {{
          totalAssetsUsd
          totalSupply
          sharePriceNumber
          sharePriceUsd
          curators {{
            name
            verified
          }}
        }}
        historicalState {{
          sharePriceNumber(options: {{
            startTimestamp: {start_ts}
            endTimestamp: {end_ts}
            interval: {interval}
          }}) {{ x y }}
          totalAssetsUsd(options: {{
            startTimestamp: {start_ts}
            endTimestamp: {end_ts}
            interval: {interval}
          }}) {{ x y }}
        }}
      }}
    }}
    """

    try:
        result = query_graphql(query)
        if "errors" in result:
            print(f"      ⚠️  Error: {result['errors'][0].get('message', '')[:100]}")
            return None
        return result.get("data", {}).get("vaultByAddress")
    except Exception as e:
        print(f"      ❌ Exception: {e}")
        return None


def parse_timeseries(data_points: List[Dict], vault_info: Dict,
                     ts_formatter) -> List[Dict]:
    """
    Parse share price + TVL timeseries into flat rows.
    data_points is a list of {x: timestamp, y: value} from sharePriceNumber.
    """
    # Build TVL lookup by timestamp
    tvl_points = vault_info.get("_tvl_points", [])
    tvl_lookup = {p["x"]: p["y"] for p in tvl_points if p.get("y") is not None}

    rows = []
    for pt in data_points:
        ts = pt["x"]
        share_price = pt.get("y")
        if share_price is None:
            continue

        tvl_usd = tvl_lookup.get(ts)

        rows.append({
            "vault_address": vault_info["address"],
            "vault_name": vault_info["name"],
            "chain": vault_info["chain"],
            "chain_id": vault_info["chain_id"],
            "curator_name": vault_info["curator_name"],
            "asset_symbol": vault_info["asset_symbol"],
            "timestamp": ts,
            "date": ts_formatter(ts),
            "share_price": share_price,
            "total_assets_usd": tvl_usd,
        })

    return rows


def compute_vault_stats(daily_rows: List[Dict], vault_info: Dict) -> Dict:
    """
    Compute per-vault summary statistics from daily share price data.
    """
    if not daily_rows:
        return None

    prices = [(r["timestamp"], r["share_price"]) for r in daily_rows
              if r["share_price"] is not None]
    if len(prices) < 2:
        return None

    prices.sort(key=lambda x: x[0])

    # Peak share price (before depeg)
    pre_depeg = [(ts, p) for ts, p in prices if ts < DEPEG_TS]
    peak_price = max(p for _, p in pre_depeg) if pre_depeg else prices[0][1]
    peak_ts = max((ts for ts, p in pre_depeg if p == peak_price),
                  default=prices[0][0]) if pre_depeg else prices[0][0]

    # Trough (after depeg start)
    post_depeg = [(ts, p) for ts, p in prices if ts >= DEPEG_TS]
    if post_depeg:
        trough_price = min(p for _, p in post_depeg)
        trough_ts = min(ts for ts, p in post_depeg if p == trough_price)
    else:
        trough_price = min(p for _, p in prices)
        trough_ts = min(ts for ts, p in prices if p == trough_price)

    # Max drawdown
    max_drawdown_pct = (peak_price - trough_price) / peak_price if peak_price > 0 else 0

    # Current / latest price
    latest_price = prices[-1][1]
    latest_ts = prices[-1][0]

    # Recovery: latest vs trough
    recovery_pct = (latest_price - trough_price) / (peak_price - trough_price) \
        if (peak_price - trough_price) > 0 else None

    # Depeg window: Nov 3-7 specific drop
    nov3 = 1762128000   # Nov 3 2025 00:00 UTC
    nov7 = 1762473600   # Nov 7 2025 00:00 UTC
    depeg_window = [(ts, p) for ts, p in prices if nov3 <= ts <= nov7]
    if depeg_window:
        depeg_start_price = depeg_window[0][1]
        depeg_end_price = depeg_window[-1][1]
        depeg_drop_pct = (depeg_start_price - depeg_end_price) / depeg_start_price \
            if depeg_start_price > 0 else 0
    else:
        depeg_start_price = None
        depeg_end_price = None
        depeg_drop_pct = None

    # TVL at peak vs trough vs pre-depeg
    tvl_points = [(r["timestamp"], r["total_assets_usd"]) for r in daily_rows
                  if r.get("total_assets_usd") is not None]
    tvl_at_peak = None
    tvl_at_trough = None
    tvl_pre_depeg = None
    if tvl_points:
        tvl_lookup = dict(tvl_points)
        # Find closest TVL to peak_ts and trough_ts
        tvl_at_peak = tvl_lookup.get(peak_ts)
        tvl_at_trough = tvl_lookup.get(trough_ts)

        # Pre-depeg TVL: closest point on or before Nov 3 2025 00:00 UTC
        # This is the "what was at stake" number
        pre_depeg_candidates = [(ts, tvl) for ts, tvl in tvl_points if ts <= nov3]
        if pre_depeg_candidates:
            pre_depeg_candidates.sort(key=lambda x: x[0])
            tvl_pre_depeg = pre_depeg_candidates[-1][1]  # latest before depeg
        elif tvl_points:
            # Fallback: earliest available TVL if vault was created after Nov 3
            tvl_points_sorted = sorted(tvl_points, key=lambda x: x[0])
            tvl_pre_depeg = tvl_points_sorted[0][1]

    # Estimated loss from share price drop (use pre-depeg TVL for most accurate estimate)
    estimated_loss_usd = None
    best_tvl = tvl_pre_depeg or tvl_at_peak
    if best_tvl and max_drawdown_pct > 0.001:
        estimated_loss_usd = best_tvl * max_drawdown_pct

    return {
        "vault_address": vault_info["address"],
        "vault_name": vault_info["name"],
        "chain": vault_info["chain"],
        "chain_id": vault_info["chain_id"],
        "curator_name": vault_info["curator_name"],
        "asset_symbol": vault_info["asset_symbol"],
        "listed": vault_info.get("listed"),
        "exposure_status": vault_info.get("exposure_status"),
        "collateral_symbol": vault_info.get("collateral_symbol"),
        "discovery_method": vault_info.get("discovery_method"),
        "vault_tvl_usd": vault_info.get("vault_tvl_usd"),

        # Share price stats
        "peak_price": peak_price,
        "peak_date": ts_to_date(peak_ts),
        "trough_price": trough_price,
        "trough_date": ts_to_date(trough_ts),
        "latest_price": latest_price,
        "latest_date": ts_to_date(latest_ts),
        "max_drawdown_pct": max_drawdown_pct,
        "recovery_pct": recovery_pct,

        # Depeg window (Nov 3-7)
        "depeg_start_price": depeg_start_price,
        "depeg_end_price": depeg_end_price,
        "depeg_drop_pct": depeg_drop_pct,

        # TVL context
        "tvl_at_peak_usd": tvl_at_peak,
        "tvl_at_trough_usd": tvl_at_trough,
        "tvl_pre_depeg_usd": tvl_pre_depeg,
        "estimated_loss_usd": estimated_loss_usd,

        # Data quality
        "daily_data_points": len(prices),
    }


def main():
    print("=" * 80)
    print("Block 2.3: Share Price History — Bad Debt Socialization Detection")
    print("=" * 80)
    print(f"Daily window:  {ts_to_date(DAILY_START)} → {ts_to_date(DAILY_END)}")
    print(f"Hourly window: {ts_to_date(HOURLY_START)} → {ts_to_date(HOURLY_END)}")
    print(f"Depeg event:   {ts_to_date(DEPEG_TS)}")
    print("=" * 80)

    # ── Load Block 1 vault data (3-phase discovery already captures ALL vaults) ──
    graphql_path = PROJECT_ROOT / "data" / "block1_vaults_graphql.csv"
    if not graphql_path.exists():
        print(f"❌ Block 1 vaults CSV not found: {graphql_path}")
        return

    block1 = pd.read_csv(graphql_path)

    # Handle column name variations between script versions
    if "blockchain" in block1.columns and "chain" not in block1.columns:
        block1.rename(columns={"blockchain": "chain"}, inplace=True)
    if "curator" in block1.columns and "curator_name" not in block1.columns:
        block1.rename(columns={"curator": "curator_name"}, inplace=True)

    print(f"\n📂 Loaded {len(block1)} vault-market pairs from Block 1")
    if "discovery_method" in block1.columns:
        method_counts = block1["discovery_method"].value_counts()
        for method, count in method_counts.items():
            print(f"   {method}: {count}")

    # Deduplicate vaults (a vault may appear in multiple markets)
    vault_master = {}
    for _, r in block1.iterrows():
        addr = str(r.get("vault_address", "")).lower()
        cid = int(r.get("chain_id", 0))
        key = (addr, cid)

        if key not in vault_master:
            vault_master[key] = {
                "address": r.get("vault_address", addr),
                "name": r.get("vault_name", "Unknown"),
                "chain": r.get("chain", ""),
                "chain_id": cid,
                "curator_name": r.get("curator_name", "Unknown"),
                "collateral_symbol": str(r.get("collateral_symbol", "")),
                "loan_symbol": r.get("loan_symbol", ""),
                "exposure_status": r.get("exposure_status", "UNKNOWN"),
                "supply_usd": float(r.get("supply_assets_usd", 0) or 0),
                "vault_tvl_usd": float(r.get("vault_total_assets_usd", 0) or 0),
                "discovery_method": r.get("discovery_method", "current_allocation"),
            }
        else:
            # Merge collateral symbols for vaults in multiple markets
            existing = vault_master[key]
            new_col = str(r.get("collateral_symbol", ""))
            if new_col and new_col not in existing["collateral_symbol"]:
                existing["collateral_symbol"] += f"|{new_col}"
            existing["supply_usd"] += float(r.get("supply_assets_usd", 0) or 0)

    # Sort by vault TVL descending (historical vaults have $0 supply but large TVL)
    vaults_list = sorted(vault_master.values(),
                         key=lambda v: v["vault_tvl_usd"], reverse=True)

    print(f"   Unique vaults: {len(vaults_list)}")
    print(f"   Current allocation: {sum(1 for v in vaults_list if v['discovery_method']=='current_allocation')}")
    print(f"   Historical (via reallocations): {sum(1 for v in vaults_list if v['discovery_method']=='historical_reallocation')}")

    # ── Query each vault ──
    all_daily_rows = []
    all_hourly_rows = []
    all_summaries = []

    for idx, v in enumerate(vaults_list):
        address = v["address"]
        chain_id = int(v["chain_id"])
        name = v["name"]
        chain = v["chain"]
        curator = v["curator_name"]
        discovery = v.get("discovery_method", "current_allocation")

        print(f"\n   [{idx+1}/{len(vaults_list)}] {name} ({chain}) [{discovery}]")
        print(f"      address: {address}  chain_id: {chain_id}")
        print(f"      curator: {curator}")
        print(f"      exposure: {v['collateral_symbol']}  status: {v['exposure_status']}")
        print(f"      TVL: ${v.get('vault_tvl_usd', 0):,.0f}  supply: ${v['supply_usd']:,.2f}")

        vault_info = {
            "address": address,
            "name": name,
            "chain": chain,
            "chain_id": chain_id,
            "curator_name": curator,
            "asset_symbol": v.get("loan_symbol", ""),
            "listed": None,
            "exposure_status": v.get("exposure_status"),
            "collateral_symbol": v.get("collateral_symbol"),
            "discovery_method": discovery,
            "vault_tvl_usd": v.get("vault_tvl_usd"),
        }

        # ── DAILY query ──
        print(f"      📊 Fetching daily share prices...")
        daily_data = fetch_vault_history(address, chain_id, "DAY", DAILY_START, DAILY_END)

        if daily_data:
            vault_info["listed"] = daily_data.get("listed")
            vault_info["asset_symbol"] = (daily_data.get("asset") or {}).get("symbol", "")

            # Update name/curator from API if Dune had "Unknown"
            api_name = daily_data.get("name")
            if api_name and (vault_info["name"] in ("Unknown", "", None)):
                vault_info["name"] = api_name
            api_state = daily_data.get("state") or {}
            api_curators = api_state.get("curators") or []
            if api_curators and vault_info["curator_name"] in ("Unknown", "", None):
                verified = [c for c in api_curators if c.get("verified")]
                if verified:
                    vault_info["curator_name"] = verified[0].get("name", vault_info["curator_name"])

            hist = daily_data.get("historicalState") or {}
            sp_points = hist.get("sharePriceNumber") or []
            tvl_points = hist.get("totalAssetsUsd") or []
            vault_info["_tvl_points"] = tvl_points

            daily_rows = parse_timeseries(sp_points, vault_info, ts_to_date)
            all_daily_rows.extend(daily_rows)
            print(f"      ✅ {len(sp_points)} daily price points, {len(tvl_points)} TVL points")

            # ── Compute stats ──
            stats = compute_vault_stats(daily_rows, vault_info)
            if stats:
                all_summaries.append(stats)
                dd = stats["max_drawdown_pct"]
                if dd > 0.001:
                    print(f"      🔴 Max drawdown: {dd*100:.2f}% "
                          f"(peak {stats['peak_price']:.6f} on {stats['peak_date']} → "
                          f"trough {stats['trough_price']:.6f} on {stats['trough_date']})")
                    if stats.get("estimated_loss_usd"):
                        print(f"         Estimated loss: ${stats['estimated_loss_usd']:,.2f}")
                    if stats.get("depeg_drop_pct") and stats["depeg_drop_pct"] > 0.001:
                        print(f"         Nov 3-7 drop: {stats['depeg_drop_pct']*100:.2f}%")
                else:
                    print(f"      ✅ No significant drawdown (max {dd*100:.4f}%)")
        else:
            print(f"      ❌ No daily data returned")

        time.sleep(REQUEST_DELAY)

        # ── HOURLY query (depeg zoom) ──
        print(f"      📊 Fetching hourly share prices (Nov 1-15)...")
        hourly_data = fetch_vault_history(address, chain_id, "HOUR", HOURLY_START, HOURLY_END)

        if hourly_data:
            hist = hourly_data.get("historicalState") or {}
            sp_points = hist.get("sharePriceNumber") or []
            tvl_points = hist.get("totalAssetsUsd") or []
            vault_info["_tvl_points"] = tvl_points

            hourly_rows = parse_timeseries(sp_points, vault_info, ts_to_datetime)
            all_hourly_rows.extend(hourly_rows)
            print(f"      ✅ {len(sp_points)} hourly price points")
        else:
            print(f"      ❌ No hourly data returned")

        time.sleep(REQUEST_DELAY)

    # ══════════════════════════════════════════════════════════════
    # SAVE OUTPUTS
    # ══════════════════════════════════════════════════════════════
    output_dir = PROJECT_ROOT / "data"
    output_dir.mkdir(parents=True, exist_ok=True)

    # Daily timeseries
    if all_daily_rows:
        df_daily = pd.DataFrame(all_daily_rows)
        df_daily = df_daily.sort_values(["vault_name", "timestamp"])
        daily_path = output_dir / "block2_share_prices_daily.csv"
        df_daily.to_csv(daily_path, index=False)
        print(f"\n✅ Saved {len(df_daily)} daily rows to {daily_path}")
    else:
        print("\n❌ No daily data collected")

    # Hourly timeseries
    if all_hourly_rows:
        df_hourly = pd.DataFrame(all_hourly_rows)
        df_hourly = df_hourly.sort_values(["vault_name", "timestamp"])
        hourly_path = output_dir / "block2_share_prices_hourly.csv"
        df_hourly.to_csv(hourly_path, index=False)
        print(f"✅ Saved {len(df_hourly)} hourly rows to {hourly_path}")
    else:
        print("❌ No hourly data collected")

    # Summary stats
    if all_summaries:
        df_summary = pd.DataFrame(all_summaries)
        df_summary = df_summary.sort_values("max_drawdown_pct", ascending=False)
        summary_path = output_dir / "block2_share_price_summary.csv"
        df_summary.to_csv(summary_path, index=False)
        print(f"✅ Saved {len(df_summary)} vault summaries to {summary_path}")

    # ══════════════════════════════════════════════════════════════
    # REPORT
    # ══════════════════════════════════════════════════════════════
    if not all_summaries:
        print("\n❌ No summary data to report")
        return

    df_s = pd.DataFrame(all_summaries)

    print(f"\n{'═' * 70}")
    print(f"  SHARE PRICE IMPACT REPORT — BAD DEBT SOCIALIZATION")
    print(f"{'═' * 70}")

    # ── Vaults with significant drawdown ──
    significant = df_s[df_s['max_drawdown_pct'] > 0.001]
    if len(significant) > 0:
        print(f"\n{'─' * 70}")
        print(f"  🔴 VAULTS WITH SHARE PRICE DROPS (bad debt socialization)")
        print(f"{'─' * 70}")
        for _, r in significant.iterrows():
            print(f"\n  {r['vault_name']} ({r['chain']})")
            print(f"    Curator:     {r['curator_name']}")
            print(f"    Collateral:  {r['collateral_symbol']}")
            print(f"    Peak:        {r['peak_price']:.6f} on {r['peak_date']}")
            print(f"    Trough:      {r['trough_price']:.6f} on {r['trough_date']}")
            print(f"    Drawdown:    {r['max_drawdown_pct']*100:.2f}%")
            if pd.notna(r.get('depeg_drop_pct')) and r['depeg_drop_pct'] > 0.001:
                print(f"    Nov 3-7:     {r['depeg_drop_pct']*100:.2f}% drop")
            if pd.notna(r.get('estimated_loss_usd')):
                print(f"    Est. loss:   ${r['estimated_loss_usd']:,.2f}")
            if pd.notna(r.get('tvl_at_peak_usd')):
                print(f"    TVL at peak: ${r['tvl_at_peak_usd']:,.2f}")
            if pd.notna(r.get('tvl_pre_depeg_usd')):
                print(f"    TVL pre-depeg (Nov 3): ${r['tvl_pre_depeg_usd']:,.2f}")
            if pd.notna(r.get('recovery_pct')):
                print(f"    Recovery:    {r['recovery_pct']*100:.1f}%")
    else:
        print(f"\n  ⚠️  No vaults with significant share price drops detected")
        print(f"      (This may mean bad debt hasn't been realized yet — oracle still masking)")

    # ── Stable vaults (no drawdown) ──
    stable = df_s[df_s['max_drawdown_pct'] <= 0.001]
    if len(stable) > 0:
        print(f"\n{'─' * 70}")
        print(f"  ✅ STABLE VAULTS (no share price drop = no realized bad debt)")
        print(f"{'─' * 70}")
        for _, r in stable.iterrows():
            status = r.get('exposure_status', '')
            print(f"  {r['vault_name']} ({r['chain']}): "
                  f"price={r['latest_price']:.6f}  status={status}")

    # ── Oracle masking detection ──
    print(f"\n{'─' * 70}")
    print(f"  🔮 ORACLE MASKING ANALYSIS")
    print(f"{'─' * 70}")
    # Vaults that are ACTIVE_DEPEG or STOPPED_SUPPLYING but show no drawdown
    # = bad debt exists but oracle hasn't triggered realization
    active_no_drop = df_s[
        (df_s['exposure_status'].isin(['ACTIVE_DEPEG', 'STOPPED_SUPPLYING'])) &
        (df_s['max_drawdown_pct'] <= 0.001)
    ]
    if len(active_no_drop) > 0:
        print(f"  ⚠️  {len(active_no_drop)} vaults with active exposure but NO share price drop:")
        print(f"      → Oracle is likely masking unrealized bad debt")
        for _, r in active_no_drop.iterrows():
            tvl = r.get('tvl_at_peak_usd')
            tvl_str = f"TVL=${tvl:,.2f}" if pd.notna(tvl) else "TVL=N/A"
            print(f"      {r['vault_name']} ({r['chain']}): "
                  f"status={r['exposure_status']}  {tvl_str}")
    else:
        print(f"  ✅ No oracle masking detected — all active vaults show price impact")

    # ── Historical vaults: did they absorb bad debt before exiting? ──
    print(f"\n{'─' * 70}")
    print(f"  🕐 HISTORICALLY EXPOSED VAULTS — BAD DEBT SOCIALIZATION CHECK")
    print(f"{'─' * 70}")
    historical = df_s[df_s['exposure_status'] == 'HISTORICALLY_EXPOSED']
    if len(historical) > 0:
        hist_with_drop = historical[historical['max_drawdown_pct'] > 0.001]
        hist_clean = historical[historical['max_drawdown_pct'] <= 0.001]

        if len(hist_with_drop) > 0:
            print(f"  🔴 {len(hist_with_drop)} historically exposed vaults ABSORBED bad debt:")
            for _, r in hist_with_drop.iterrows():
                loss = r.get('estimated_loss_usd')
                loss_str = f"est. loss=${loss:,.2f}" if pd.notna(loss) else ""
                print(f"      {r['vault_name']} ({r['chain']}): "
                      f"drawdown={r['max_drawdown_pct']*100:.2f}%  "
                      f"price={r['latest_price']:.6f}  {loss_str}")

        if len(hist_clean) > 0:
            print(f"  ✅ {len(hist_clean)} historically exposed vaults exited WITHOUT absorbing bad debt:")
            for _, r in hist_clean.iterrows():
                tvl = r.get('vault_tvl_usd')
                tvl_str = f"TVL=${tvl:,.0f}" if pd.notna(tvl) else ""
                print(f"      {r['vault_name']} ({r['chain']}): "
                      f"price={r['latest_price']:.6f}  {tvl_str}")
    else:
        print(f"  (No historically exposed vaults in dataset)")

    # ── Total estimated losses ──
    total_loss = df_s['estimated_loss_usd'].sum()
    if total_loss > 0:
        print(f"\n{'─' * 70}")
        print(f"  💰 TOTAL ESTIMATED BAD DEBT FROM SHARE PRICE DROPS")
        print(f"{'─' * 70}")
        print(f"  Total: ${total_loss:,.2f}")
        print(f"  (Based on TVL × max drawdown %)")

    print(f"\n{'═' * 70}")


if __name__ == "__main__":
    main()
