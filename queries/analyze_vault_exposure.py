"""
Analyze vault-level exposure to toxic markets from block8 data.

Reads block8_vault_allocation_hourly.csv and block8 market history CSVs.
Produces a clear table: vault A had $X in market Y at time T.

Also investigates mismatches between vault allocation sums and market supply.
"""

import pandas as pd
import numpy as np
from pathlib import Path

DATA_DIR = Path(__file__).parent.parent / "data"

# Toxic market IDs
TOXIC_MARKETS = {
    "0x0f9563442d64ab3bd3bcb27058db0b0d4046a4c46f0acd811dacae9551d2b129": {
        "label": "sdeUSD/USDC Ethereum",
        "history_csv": "block8_eth_market_history.csv",
        "chain_id": 1,
    },
    "0x8d009383866dffaac5fe25af684e93f8dd5a98fed1991c298624ecc3a860f39f": {
        "label": "sdeUSD/pUSD Plume",
        "history_csv": "block8_plume_market_history.csv",
        "chain_id": 98866,
    },
    "0x9e90aec7d768403dacc9dd0d8320307fda3f980eed4df43e3e52168a1c667709": {
        "label": "xUSD/USDC Arbitrum",
        "history_csv": "block8_arb_market_history.csv",
        "chain_id": 42161,
    },
    "0xbd1ad3b968f5f0552dbd8cf1989a62881407c5cccf9e49fb3657c8731caf0c1f": {
        "label": "deUSD/USDC Ethereum",
        "history_csv": "block8_eth_deusd_market_history.csv",
        "chain_id": 1,
    },
    "0x82e7ab8ccabaac59b5f397507ed031ebf19a9a5b2657c00c93bc2423cd0a890d": {
        "label": "xUSD/USDC Plume [Elixir-Stream]",
        "history_csv": "block8_elixir_stream_market_history.csv",
        "chain_id": 98866,
    },
}

DEPEG_TS = 1762128000      # Nov 3 00:00 UTC
PRE_DEPEG_TS = 1762041600  # Nov 2 00:00 UTC
LOCK_TS = 1762214400       # Nov 4 00:00 UTC


def load_allocation():
    path = DATA_DIR / "block8_vault_allocation_hourly.csv"
    print(f"Loading {path} ...")
    df = pd.read_csv(path)
    print(f"  {len(df):,} rows, {df['vault_name'].nunique()} vaults, {df['market_id'].nunique()} markets")
    return df


def load_market_history(csv_name):
    path = DATA_DIR / csv_name
    if not path.exists():
        return None
    return pd.read_csv(path)


def get_snapshot(df, ts, group_col="vault_name", value_col="supply_assets_usd"):
    """Get the last value at or before timestamp ts for each group."""
    pre = df[df["timestamp"] <= ts].copy()
    if pre.empty:
        return pd.DataFrame()
    idx = pre.groupby(group_col)["timestamp"].idxmax()
    return pre.loc[idx]


def analyze_market(alloc_df, mkt_id, mkt_info):
    label = mkt_info["label"]
    print(f"\n{'='*80}")
    print(f"  {label}")
    print(f"  market_id: {mkt_id}")
    print(f"{'='*80}")

    # Filter allocations to this market
    mkt_alloc = alloc_df[alloc_df["market_id"] == mkt_id].copy()

    if mkt_alloc.empty:
        print("  No vault allocations found (private/direct supply market)")
        # Still load market history for context
        mh = load_market_history(mkt_info["history_csv"])
        if mh is not None and "supply_assets_usd" in mh.columns:
            snap = mh[mh["timestamp"] <= DEPEG_TS].iloc[-1] if len(mh[mh["timestamp"] <= DEPEG_TS]) > 0 else None
            if snap is not None:
                print(f"  Market supply at depeg: ${snap['supply_assets_usd']:,.0f}")
        return None

    # --- Snapshot at depeg (Nov 3 00:00) ---
    depeg_snap = get_snapshot(mkt_alloc, DEPEG_TS)
    depeg_snap = depeg_snap[depeg_snap["supply_assets_usd"] > 0].sort_values("supply_assets_usd", ascending=False)

    vault_sum_depeg = depeg_snap["supply_assets_usd"].sum()

    # --- Market supply from market history ---
    mh = load_market_history(mkt_info["history_csv"])
    market_supply_depeg = None
    if mh is not None and "supply_assets_usd" in mh.columns:
        mh_pre = mh[mh["timestamp"] <= DEPEG_TS]
        if len(mh_pre) > 0:
            market_supply_depeg = mh_pre.iloc[-1]["supply_assets_usd"]

    print(f"\n  AT DEPEG (Nov 3 00:00 UTC):")
    if market_supply_depeg is not None:
        print(f"  Market total supply:      ${market_supply_depeg:>14,.0f}  (from market history)")
    print(f"  Sum of vault allocations: ${vault_sum_depeg:>14,.0f}  (from vault allocation data)")
    if market_supply_depeg and market_supply_depeg > 0:
        ratio = vault_sum_depeg / market_supply_depeg
        diff = vault_sum_depeg - market_supply_depeg
        print(f"  Difference:               ${diff:>+14,.0f}  (ratio: {ratio:.2f}x)")
        if ratio < 1:
            direct = market_supply_depeg - vault_sum_depeg
            print(f"  -> ${direct:,.0f} supplied directly to market (not through vaults)")
        elif ratio > 1.1:
            print(f"  -> OVERCOUNT: vault allocations exceed market supply")

    print(f"\n  Vault-by-vault breakdown:")
    print(f"  {'VAULT':<45} {'AT DEPEG':>14} {'PEAK':>14} {'PEAK DATE':>12}")
    print(f"  {'─'*45} {'─'*14} {'─'*14} {'─'*12}")

    results = []
    for _, row in depeg_snap.iterrows():
        vault_addr = row["vault_address"]
        vault_name = row["vault_name"]
        depeg_val = row["supply_assets_usd"]

        # Find peak allocation for this vault to this market
        v_mkt = mkt_alloc[mkt_alloc["vault_address"] == vault_addr]
        peak_idx = v_mkt["supply_assets_usd"].idxmax()
        peak_val = v_mkt.loc[peak_idx, "supply_assets_usd"]
        peak_date = v_mkt.loc[peak_idx, "datetime"] if "datetime" in v_mkt.columns else "?"

        print(f"  {vault_name:<45} ${depeg_val:>12,.0f} ${peak_val:>12,.0f} {peak_date:>12}")

        results.append({
            "market": label,
            "market_id": mkt_id,
            "vault_name": vault_name,
            "vault_address": vault_addr,
            "chain_id": row.get("chain_id", ""),
            "supply_usd_at_depeg": depeg_val,
            "peak_supply_usd": peak_val,
            "peak_date": peak_date,
        })

    # --- Timeline: trace how allocation evolved day by day ---
    print(f"\n  Daily allocation timeline (sum of all vaults):")
    mkt_alloc["date"] = pd.to_datetime(mkt_alloc["datetime"]).dt.date
    daily = mkt_alloc.groupby("date")["supply_assets_usd"].sum()
    # Show just key dates
    for d in daily.index:
        val = daily[d]
        d_str = str(d)
        if d_str >= "2025-10-25" and d_str <= "2025-11-10":
            marker = ""
            if d_str == "2025-11-03":
                marker = " <-- DEPEG"
            elif d_str == "2025-11-04":
                marker = " <-- LOCK"
            print(f"    {d_str}  ${val:>14,.0f}{marker}")

    return results


def main():
    alloc = load_allocation()

    print("\n" + "=" * 80)
    print("  VAULT EXPOSURE TO TOXIC MARKETS - COMPLETE ANALYSIS")
    print("  Data source: block8_vault_allocation_hourly.csv")
    print("=" * 80)

    all_results = []
    for mkt_id, mkt_info in TOXIC_MARKETS.items():
        results = analyze_market(alloc, mkt_id, mkt_info)
        if results:
            all_results.extend(results)

    # Write summary CSV
    if all_results:
        df = pd.DataFrame(all_results)
        out_path = DATA_DIR / "block8_vault_toxic_exposure_summary.csv"
        df.to_csv(out_path, index=False)
        print(f"\n\nWrote summary to {out_path}")

    # --- Grand summary ---
    print(f"\n{'='*80}")
    print("  GRAND SUMMARY: WHO HAD WHAT, WHERE")
    print(f"{'='*80}")
    if all_results:
        df = pd.DataFrame(all_results)
        for mkt in df["market"].unique():
            mdf = df[df["market"] == mkt].sort_values("supply_usd_at_depeg", ascending=False)
            total = mdf["supply_usd_at_depeg"].sum()
            print(f"\n  {mkt}  (vault total: ${total:,.0f})")
            for _, r in mdf.iterrows():
                pct = r["supply_usd_at_depeg"] / total * 100 if total > 0 else 0
                print(f"    {r['vault_name']:<40} ${r['supply_usd_at_depeg']:>12,.0f}  ({pct:5.1f}%)")


if __name__ == "__main__":
    main()
