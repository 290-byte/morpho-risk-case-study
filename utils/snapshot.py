"""
Dashboard Snapshot — dumps all key numbers to a plain text file.

Called once per app reload. Produces data/snapshot.txt with every
metric, table row, and computed value the dashboard displays.

To disable: comment out the single line in app.py:
    # from utils.snapshot import write_snapshot; write_snapshot()
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timezone

DATA_DIR = Path(__file__).parent.parent / "data"
SNAPSHOT_PATH = DATA_DIR / "snapshot.txt"


def _fmt(val, fmt="$"):
    """Format a number for the snapshot."""
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return "—"
    val = float(val)
    if fmt == "$":
        if abs(val) >= 1_000_000:
            return f"${val/1e6:,.2f}M"
        elif abs(val) >= 1_000:
            return f"${val/1e3:,.1f}K"
        else:
            return f"${val:,.2f}"
    elif fmt == "%":
        return f"{val:.2%}"
    elif fmt == "n":
        return f"{val:,.0f}"
    elif fmt == "p":  # price
        return f"${val:.6f}"
    return str(val)


def write_snapshot():
    """Generate snapshot.txt from all loaded CSVs. Safe to call even if files are missing."""
    lines = []
    w = lines.append  # shorthand

    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    w(f"MORPHO RISK CASE STUDY — DASHBOARD SNAPSHOT")
    w(f"Generated: {ts}")
    w("=" * 78)

    # ── Helper to read CSV safely ──
    def read(name):
        p = DATA_DIR / name
        if p.exists():
            return pd.read_csv(p)
        return pd.DataFrame()

    # ══════════════════════════════════════════════════════════════
    # SECTION 1: OVERVIEW
    # ══════════════════════════════════════════════════════════════
    markets = read("block1_markets_graphql.csv")
    vaults_raw = read("block1_vaults_graphql.csv")

    w("\n" + "─" * 78)
    w("SECTION 1: OVERVIEW")
    w("─" * 78)

    if not markets.empty:
        for col in ["total_supply_usd", "total_borrow_usd", "bad_debt_usd", "utilization"]:
            if col in markets.columns:
                markets[col] = pd.to_numeric(markets[col], errors="coerce").fillna(0)

        total_bad_debt = markets["bad_debt_usd"].sum() if "bad_debt_usd" in markets.columns else 0
        n_markets = len(markets)
        chains = markets["chain"].nunique() if "chain" in markets.columns else 0

        w(f"  Toxic Markets:       {n_markets}")
        w(f"  Chains Affected:     {chains}")
        w(f"  Total Bad Debt:      {_fmt(total_bad_debt)}")
        w(f"  Liquidation Events:  0")
    else:
        w("  [block1_markets_graphql.csv NOT FOUND]")

    if not vaults_raw.empty:
        unique_vaults = vaults_raw["vault_address"].nunique() if "vault_address" in vaults_raw.columns else len(vaults_raw)
        w(f"  Affected Vaults:     {unique_vaults} (unique addresses)")
        w(f"  Vault-Market Pairs:  {len(vaults_raw)}")
    else:
        w("  [block1_vaults_graphql.csv NOT FOUND]")

    # ── Asset prices ──
    prices = read("block5_asset_prices.csv")
    if not prices.empty:
        w("\n  Asset Prices (latest in dataset):")
        sym_col = "symbol" if "symbol" in prices.columns else "asset"
        if sym_col in prices.columns:
            prices["price_usd"] = pd.to_numeric(prices.get("price_usd", 0), errors="coerce")
            for asset in ["xUSD", "deUSD", "sdeUSD"]:
                subset = prices[prices[sym_col] == asset]
                if not subset.empty:
                    latest = subset.sort_values("date" if "date" in subset.columns else sym_col).iloc[-1]
                    w(f"    {asset:8s}  latest={_fmt(latest['price_usd'], 'p')}")

    # ══════════════════════════════════════════════════════════════
    # SECTION 2: MARKET EXPOSURE
    # ══════════════════════════════════════════════════════════════
    w("\n" + "─" * 78)
    w("SECTION 2: MARKET EXPOSURE — ALL 18 TOXIC MARKETS")
    w("─" * 78)

    if not markets.empty:
        # Build label
        def _label(r):
            c = r.get("collateral_symbol", r.get("collateral", "?"))
            l = r.get("loan_symbol", r.get("loan", "?"))
            ch = str(r.get("chain", ""))[:3].title()
            return f"{c}/{l} ({ch})"

        at_risk = markets[markets.get("utilization", pd.Series(dtype=float)) >= 0.99] if "utilization" in markets.columns else pd.DataFrame()
        w(f"  At Risk (>=99% util): {len(at_risk)}")
        w(f"  Total Supply:        {_fmt(markets['total_supply_usd'].sum())}" if "total_supply_usd" in markets.columns else "")
        w(f"  Total Borrow:        {_fmt(markets['total_borrow_usd'].sum())}" if "total_borrow_usd" in markets.columns else "")

        w("\n  Market Detail:")
        w(f"  {'Market':<35s} {'Chain':<10s} {'Supply':>12s} {'Borrow':>12s} {'Util':>8s} {'Bad Debt':>12s} {'Status'}")
        w(f"  {'─'*35} {'─'*10} {'─'*12} {'─'*12} {'─'*8} {'─'*12} {'─'*20}")
        for _, r in markets.sort_values("bad_debt_usd", ascending=False).iterrows():
            label = _label(r)
            chain = str(r.get("chain", ""))[:10]
            supply = _fmt(r.get("total_supply_usd", 0))
            borrow = _fmt(r.get("total_borrow_usd", 0))
            util = f"{float(r.get('utilization', 0)):.1%}"
            bd = _fmt(r.get("bad_debt_usd", 0))
            status = str(r.get("bad_debt_status", r.get("status", "")))[:20]
            w(f"  {label:<35s} {chain:<10s} {supply:>12s} {borrow:>12s} {util:>8s} {bd:>12s} {status}")

    # ══════════════════════════════════════════════════════════════
    # SECTION 3: BAD DEBT ANALYSIS
    # ══════════════════════════════════════════════════════════════
    w("\n" + "─" * 78)
    w("SECTION 3: BAD DEBT ANALYSIS")
    w("─" * 78)

    if not markets.empty:
        total_bd = markets["bad_debt_usd"].sum() if "bad_debt_usd" in markets.columns else 0
        markets_with_bd = len(markets[markets["bad_debt_usd"] > 0]) if "bad_debt_usd" in markets.columns else 0
        realized = markets["realized_bad_debt_usd"].sum() if "realized_bad_debt_usd" in markets.columns else 0
        w(f"  Total Bad Debt:      {_fmt(total_bd)} (market-level — all lenders, not vault-attributed)")
        w(f"  Markets with Debt:   {markets_with_bd} / {len(markets)}")
        w(f"  Realized Bad Debt:   {_fmt(realized)}")
        w(f"\n  NOTE: The $3.86M is total bad debt across ALL lenders in each market")
        w(f"  (vault depositors + direct market depositors). Public reports citing ~$700K")
        w(f"  refer only to MEV Capital's vault allocation. The remaining ~$3.2M was")
        w(f"  borne by direct depositors who supplied USDC outside any vault.")

    # ── Load allocation timeseries for correct vault-level TVL ──
    alloc_ts_s3 = read("block3_allocation_timeseries.csv")
    alloc_by_vault = {}
    if not alloc_ts_s3.empty and "supply_assets_usd" in alloc_ts_s3.columns:
        alloc_ts_s3["supply_assets_usd"] = pd.to_numeric(
            alloc_ts_s3["supply_assets_usd"], errors="coerce").fillna(0)
        a_grp_s3 = "vault_address" if "vault_address" in alloc_ts_s3.columns else "vault_name"
        for gid, g in alloc_ts_s3.groupby(a_grp_s3):
            addr = str(gid).lower()
            pre = g[g["date"] <= "2025-11-03"]
            pre_val = 0
            market_allocs = []
            if not pre.empty:
                latest = pre["date"].max()
                day_data = pre[pre["date"] == latest]
                pre_val = day_data["supply_assets_usd"].sum()
                for _, row in day_data.iterrows():
                    if row["supply_assets_usd"] > 0:
                        market_allocs.append({
                            "market_unique_key": str(row.get("market_unique_key", "")).lower(),
                            "supply_usd": row["supply_assets_usd"],
                        })
            peak_val = g.groupby("date")["supply_assets_usd"].sum().max()
            alloc_by_vault[addr] = {
                "pre_depeg": pre_val,
                "peak": peak_val,
                "market_allocs": market_allocs,
            }

    # ── Build bad-debt market lookup ──
    bd_markets_s3 = {}
    if not markets.empty and "market_id" in markets.columns:
        for _, m in markets.iterrows():
            bd = float(m.get("bad_debt_usd", 0) or 0)
            if bd > 100:
                mid = str(m.get("market_id", "")).lower()
                bd_markets_s3[mid] = {
                    "bad_debt_usd": bd,
                    "total_supply_usd": float(m.get("total_supply_usd", 0) or 0),
                    "label": f"{m.get('collateral_symbol','?')}/{m.get('loan_symbol','?')} ({str(m.get('chain',''))[:3]})",
                }

    # ── Share price damage (from block2 daily) ──
    sp_daily = read("block2_share_prices_daily.csv")
    sp_summary = read("block2_share_price_summary.csv")

    damaged_vaults = []   # collect all damaged (SP drop + loss not realized)

    if not sp_daily.empty:
        w("\n  Share Price Damage Detection (from daily timeseries):")
        sp_daily["share_price"] = pd.to_numeric(sp_daily["share_price"], errors="coerce")
        group_key = "vault_address" if "vault_address" in sp_daily.columns else "vault_name"

        for gid, vp in sp_daily.groupby(group_key):
            vp = vp.sort_values("date")
            name = vp["vault_name"].iloc[0] if "vault_name" in vp.columns else str(gid)
            chain = vp["chain"].iloc[0] if "chain" in vp.columns else ""
            addr = str(gid).lower()

            cummax = vp["share_price"].cummax()
            dd = ((vp["share_price"] - cummax) / cummax).min()

            if dd < -0.01:
                dd_idx = ((vp["share_price"] - cummax) / cummax).idxmin()
                peak_val = cummax.loc[dd_idx]
                trough_val = vp.loc[dd_idx, "share_price"]
                trough_date = vp.loc[dd_idx, "date"]
                last_price = vp["share_price"].iloc[-1]

                # Correct vault-level TVL from allocation timeseries
                at = alloc_by_vault.get(addr, {})
                correct_pre = at.get("pre_depeg", 0)
                est_loss = correct_pre * abs(dd) if correct_pre > 0 else 0

                w(f"\n    DAMAGED (SHARE PRICE): {name} ({chain})")
                w(f"      Address:        {addr}")
                w(f"      Haircut:        {dd:.2%}")
                w(f"      Peak SP:        {_fmt(peak_val, 'p')}")
                w(f"      Trough SP:      {_fmt(trough_val, 'p')}  on {trough_date}")
                w(f"      Current SP:     {_fmt(last_price, 'p')}")
                w(f"      Toxic Exposure: {_fmt(correct_pre)} (from allocation timeseries)")
                w(f"      Estimated Loss: {_fmt(est_loss)}")

                if abs(dd) > 0.5:
                    w(f"      → Extreme concentration: near-total allocation to single toxic market")
                    w(f"        (loss realized instantly in share price — check factory_address for version)")

                damaged_vaults.append({
                    "name": name, "chain": chain, "addr": addr,
                    "type": "share_price", "haircut": dd,
                })

    # ── Loss-not-realized damage detection (promoted from diagnostics) ──
    sp_damaged_addrs = {d["addr"] for d in damaged_vaults}
    if alloc_by_vault and bd_markets_s3:
        for addr, at in alloc_by_vault.items():
            if addr in sp_damaged_addrs:
                continue

            vault_bd_share = 0
            exposures = []
            for ma in at.get("market_allocs", []):
                mkey = ma["market_unique_key"]
                supply = ma["supply_usd"]
                for mid, minfo in bd_markets_s3.items():
                    if mkey == mid or mkey.startswith(mid[:10]):
                        tsup = minfo["total_supply_usd"]
                        share = (supply / tsup * minfo["bad_debt_usd"]) if tsup > 0 else 0
                        vault_bd_share += share
                        exposures.append(
                            f"{minfo['label']}: alloc={_fmt(supply)}, bd_share={_fmt(share)}")

            if vault_bd_share < 100_000:
                continue
            pre_total = at.get("pre_depeg", 0)
            if pre_total < 10_000:
                continue

            # Look up vault name from block1
            vname = "?"
            chain = "?"
            current_tvl = 0
            if not vaults_raw.empty and "vault_address" in vaults_raw.columns:
                vmatch = vaults_raw[vaults_raw["vault_address"].str.lower() == addr]
                if not vmatch.empty:
                    vname = vmatch.iloc[0].get("vault_name", "?")
                    chain = vmatch.iloc[0].get("chain",
                                vmatch.iloc[0].get("blockchain", "?"))
                    current_tvl = float(vmatch.iloc[0].get(
                        "vault_total_assets_usd", 0) or 0)

            # Estimate pre-depeg TOTAL vault TVL
            # Uses same fallback chain as bad_debt.py
            pre_depeg_total_tvl = 0
            tvl_source = "none"

            # Attempt 1: block2_share_price_summary → tvl_pre_depeg_native
            #   Raw on-chain totalAssets / 10^decimals — immune to inflation
            sp_sum = read("block2_share_price_summary.csv")
            if not sp_sum.empty and "vault_address" in sp_sum.columns:
                vs = sp_sum[sp_sum["vault_address"].str.lower() == addr]
                if not vs.empty and "tvl_pre_depeg_native" in vs.columns:
                    v = pd.to_numeric(
                        vs.iloc[0]["tvl_pre_depeg_native"], errors="coerce")
                    if pd.notna(v) and v > 0:
                        pre_depeg_total_tvl = float(v)
                        tvl_source = "block2_native"

            # Attempt 2: block2_share_prices_daily → total_assets_native
            if pre_depeg_total_tvl <= 0:
                daily = read("block2_share_prices_daily.csv")
                if (not daily.empty
                        and "vault_address" in daily.columns
                        and "total_assets_native" in daily.columns):
                    vd = daily[daily["vault_address"].str.lower() == addr]
                    if not vd.empty and "date" in vd.columns:
                        vd = vd.copy()
                        vd["total_assets_native"] = pd.to_numeric(
                            vd["total_assets_native"], errors="coerce")
                        pre = vd[vd["date"] <= "2025-11-03"].dropna(
                            subset=["total_assets_native"])
                        if not pre.empty:
                            pre = pre.sort_values("date")
                            v = pre.iloc[-1]["total_assets_native"]
                            if v > 0:
                                pre_depeg_total_tvl = float(v)
                                tvl_source = "block2_daily_native"

            # Attempt 3: block3_vault_net_flows (prefer native column)
            if pre_depeg_total_tvl <= 0:
                nf = read("block3_vault_net_flows.csv")
                if not nf.empty and "vault_address" in nf.columns:
                    vnf = nf[nf["vault_address"].str.lower() == addr]
                    if not vnf.empty and "date" in vnf.columns:
                        vnf = vnf.copy()
                        vnf["date"] = pd.to_datetime(vnf["date"], errors="coerce")
                        pre_nf = vnf[vnf["date"] <= "2025-11-03"].sort_values("date")
                        if not pre_nf.empty:
                            for tcol in ["total_assets_native",
                                         "total_assets_usd",
                                         "vault_total_assets_usd"]:
                                if tcol in pre_nf.columns:
                                    v = pd.to_numeric(
                                        pre_nf.iloc[-1][tcol], errors="coerce")
                                    if pd.notna(v) and v > 0:
                                        if current_tvl <= 0 or v < current_tvl * 100:
                                            pre_depeg_total_tvl = float(v)
                                            tvl_source = f"net_flows_{tcol}"
                                        break

            # Sanity: discard if absurdly high vs current (inflation)
            if (pre_depeg_total_tvl > 0
                    and current_tvl > 0
                    and pre_depeg_total_tvl > current_tvl * 100):
                pre_depeg_total_tvl = 0
                tvl_source = "none (discarded: inflation)"

            vault_haircut = vault_bd_share / pre_depeg_total_tvl if pre_depeg_total_tvl > 0 else 0
            toxic_loss_rate = vault_bd_share / pre_total if pre_total > 0 else 0

            w(f"\n    DAMAGED (LOSS NOT REALIZED): {vname} ({chain})")
            w(f"      Address:             {addr}")
            if pre_depeg_total_tvl > 0:
                w(f"      Vault-Level Haircut: ~{vault_haircut:.1%}  "
                  f"({_fmt(vault_bd_share)} loss / {_fmt(pre_depeg_total_tvl)} pre-depeg vault TVL)"
                  f"  [source: {tvl_source}]")
            else:
                w(f"      Vault-Level Haircut: N/A  "
                  f"(pre-depeg total vault TVL not available — re-run block2 pipeline)")
            w(f"      Toxic Exposure:      {_fmt(pre_total)}")
            w(f"      Est. Bad Debt Share: {_fmt(vault_bd_share)} "
              f"(~{toxic_loss_rate:.0%} of toxic allocation unrecoverable)")
            w(f"      Share Price:         no drop observed in daily timeseries")
            w(f"      → Bad debt exists in underlying market but is not reflected")
            w(f"        in vault share price (loss has not been realized)")
            for exp in exposures:
                w(f"      Market: {exp}")

            damaged_vaults.append({
                "name": vname, "chain": chain, "addr": addr,
                "type": "loss_not_realized", "haircut": -vault_haircut,
            })

    n_sp = sum(1 for d in damaged_vaults if d["type"] == "share_price")
    n_lnr = sum(1 for d in damaged_vaults if d["type"] == "loss_not_realized")
    w(f"\n  Total damaged vaults: {len(damaged_vaults)}"
      f" ({n_sp} share-price drop, {n_lnr} loss not yet realized)")

    # ── Block2 summary stats (for reference — note TVL is inflated) ──
    if not sp_summary.empty:
        w("\n  Block2 Summary Stats (CAUTION: TVL values are INFLATED market-level data):")
        for col in ["max_drawdown_pct", "tvl_at_peak_usd", "tvl_pre_depeg_usd", "estimated_loss_usd"]:
            if col not in sp_summary.columns:
                continue
        sp_summary["max_drawdown_pct"] = pd.to_numeric(sp_summary.get("max_drawdown_pct", 0), errors="coerce").fillna(0)
        sig = sp_summary[sp_summary["max_drawdown_pct"] > 0.001]
        for _, r in sig.iterrows():
            name = r.get("vault_name", "?")
            chain = r.get("chain", "?")
            dd = r.get("max_drawdown_pct", 0)
            tvl_peak = r.get("tvl_at_peak_usd", None)
            tvl_pre = r.get("tvl_pre_depeg_usd", None)
            loss = r.get("estimated_loss_usd", None)
            w(f"    {name} ({chain}): dd={dd:.2%}  tvl_peak={_fmt(tvl_peak)} [INFLATED]  est_loss={_fmt(loss)} [INFLATED]")

        # ── DIAGNOSTIC: Full drawdown table for ALL vaults ──
        w("\n  ── DIAGNOSTIC: All Vault Drawdowns (block2 summary) ──")
        w(f"  {'Vault':<40s} {'Chain':<8s} {'Drawdown':>10s} {'Peak SP':>10s} {'Trough SP':>10s} {'Curr SP':>10s} {'PeakTVL':>14s} {'PreDepegTVL':>14s}")
        w(f"  {'─'*40} {'─'*8} {'─'*10} {'─'*10} {'─'*10} {'─'*10} {'─'*14} {'─'*14}")
        sp_sorted = sp_summary.sort_values("max_drawdown_pct", ascending=False)
        for _, r in sp_sorted.iterrows():
            name = str(r.get("vault_name", "?"))[:40]
            chain = str(r.get("chain", "?"))[:8]
            dd = float(r.get("max_drawdown_pct", 0))
            peak_p = r.get("peak_price", None)
            trough_p = r.get("trough_price", None)
            latest_p = r.get("latest_price", None)
            tvl_peak = r.get("tvl_at_peak_usd", None)
            tvl_pre = r.get("tvl_pre_depeg_usd", None)
            peak_str = f"{float(peak_p):.6f}" if pd.notna(peak_p) else "—"
            trough_str = f"{float(trough_p):.6f}" if pd.notna(trough_p) else "—"
            latest_str = f"{float(latest_p):.6f}" if pd.notna(latest_p) else "—"
            tvl_peak_str = _fmt(tvl_peak) if pd.notna(tvl_peak) else "—"
            tvl_pre_str = _fmt(tvl_pre) if pd.notna(tvl_pre) else "—"
            flag = " <<<" if dd > 0.001 else ""
            w(f"  {name:<40s} {chain:<8s} {dd:>9.4%} {peak_str:>10s} {trough_str:>10s} {latest_str:>10s} {tvl_peak_str:>14s} {tvl_pre_str:>14s}{flag}")

    # ── DIAGNOSTIC: TVL cross-check (block1 current vs block2 peak/pre-depeg) ──
    if not sp_summary.empty and not vaults_raw.empty:
        w("\n  ── DIAGNOSTIC: TVL Cross-Check (block1 current vs block2 historical) ──")
        w(f"  {'Vault':<35s} {'Chain':<8s} {'Block1 TVL':>14s} {'B2 Peak TVL':>14s} {'B2 PreDepeg':>14s} {'Ratio':>8s} {'Flag'}")
        w(f"  {'─'*35} {'─'*8} {'─'*14} {'─'*14} {'─'*14} {'─'*8} {'─'*15}")

        # Build block1 vault TVL lookup
        b1_tvl = {}
        for _, r in vaults_raw.iterrows():
            addr = str(r.get("vault_address", "")).lower()
            cid = int(r.get("chain_id", 0))
            key = (addr, cid)
            if key not in b1_tvl:
                b1_tvl[key] = {
                    "name": r.get("vault_name", "?"),
                    "chain": str(r.get("chain", r.get("blockchain", "")))[:8],
                    "tvl": float(r.get("vault_total_assets_usd", 0) or 0),
                }

        for _, r in sp_summary.iterrows():
            addr = str(r.get("vault_address", "")).lower()
            cid = int(r.get("chain_id", 0))
            key = (addr, cid)
            b1 = b1_tvl.get(key, {})
            b1_tvl_val = b1.get("tvl", 0)
            name = str(r.get("vault_name", b1.get("name", "?")))[:35]
            chain = str(r.get("chain", b1.get("chain", "?")))[:8]
            b2_peak = float(r.get("tvl_at_peak_usd", 0) or 0)
            b2_pre = float(r.get("tvl_pre_depeg_usd", 0) or 0)

            # Flag if block2 TVL is >10x block1 current TVL
            ratio = b2_peak / b1_tvl_val if b1_tvl_val > 100 else 0
            flag = ""
            if ratio > 10:
                flag = f"⚠ {ratio:.0f}x INFLATED"
            elif ratio > 3:
                flag = f"? {ratio:.0f}x high"

            w(f"  {name:<35s} {chain:<8s} {_fmt(b1_tvl_val):>14s} {_fmt(b2_peak):>14s} {_fmt(b2_pre):>14s} {ratio:>7.1f}x {flag}")

    # ── DIAGNOSTIC: MEV Capital Arbitrum specific probe ──
    if not sp_daily.empty:
        w("\n  ── DIAGNOSTIC: MEV Capital USDC (Arbitrum) Price History ──")
        mev_arb = sp_daily[
            (sp_daily["vault_name"].str.contains("MEV Capital USDC", case=False, na=False)) &
            (sp_daily["chain_id"].astype(str).isin(["42161"]) if "chain_id" in sp_daily.columns
             else sp_daily["chain"].str.contains("Arb|arb", na=False))
        ]
        if mev_arb.empty:
            # Try by address
            mev_arb = sp_daily[sp_daily["vault_address"].str.lower().str.startswith("0xa60643", na=False)] if "vault_address" in sp_daily.columns else pd.DataFrame()

        if not mev_arb.empty:
            mev_arb = mev_arb.sort_values("date" if "date" in mev_arb.columns else "timestamp")
            mev_arb["share_price"] = pd.to_numeric(mev_arb["share_price"], errors="coerce")
            w(f"    Data points: {len(mev_arb)}")
            w(f"    Date range: {mev_arb['date'].iloc[0]} → {mev_arb['date'].iloc[-1]}" if "date" in mev_arb.columns else "")
            w(f"    Price range: {mev_arb['share_price'].min():.6f} → {mev_arb['share_price'].max():.6f}")

            cummax = mev_arb["share_price"].cummax()
            dd = ((mev_arb["share_price"] - cummax) / cummax).min()
            w(f"    Max cummax drawdown: {dd:.4%}")

            # Show Nov 1-15 window
            if "date" in mev_arb.columns:
                nov_window = mev_arb[(mev_arb["date"] >= "2025-10-28") & (mev_arb["date"] <= "2025-11-20")]
                if not nov_window.empty:
                    w(f"    Nov window ({len(nov_window)} points):")
                    for _, pt in nov_window.iterrows():
                        tvl_val = pt.get("total_assets_usd", "?")
                        tvl_str = _fmt(float(tvl_val)) if pd.notna(tvl_val) and tvl_val != "?" else "?"
                        w(f"      {pt['date']}  SP={pt['share_price']:.6f}  TVL={tvl_str}")
        else:
            w("    ⚠ NO DATA FOUND for MEV Capital USDC on Arbitrum in block2_share_prices_daily.csv")
            w("    This vault may not have been queried or returned empty from the API")

    # ── NEW: Vault Factory / Version Data ──
    if not vaults_raw.empty and "vault_factory_address" in vaults_raw.columns:
        w("\n  ── VAULT FACTORY ADDRESSES (version classification) ──")
        factory_data = vaults_raw[["vault_name", "vault_address", "vault_factory_address",
                                    "blockchain", "vault_creation_timestamp"]].drop_duplicates(
            subset="vault_address")
        factory_data = factory_data.sort_values("vault_creation_timestamp")

        w(f"  {'Vault':<40s} {'Chain':<8s} {'Factory':<16s} {'Created':<12s}")
        w(f"  {'─'*40} {'─'*8} {'─'*16} {'─'*12}")
        for _, r in factory_data.iterrows():
            fa = str(r.get("vault_factory_address", "N/A"))
            fa_short = f"{fa[:8]}...{fa[-4:]}" if len(fa) > 14 else fa
            ts = r.get("vault_creation_timestamp", "")
            ts_str = ""
            if pd.notna(ts) and ts:
                try:
                    ts_str = datetime.fromtimestamp(int(ts)).strftime("%Y-%m-%d")
                except (ValueError, TypeError, OSError):
                    ts_str = str(ts)[:10]
            vname = str(r.get("vault_name", "?"))[:40]
            chain = str(r.get("blockchain", "?"))[:8]
            w(f"  {vname:<40s} {chain:<8s} {fa_short:<16s} {ts_str:<12s}")

        # Group by factory
        w(f"\n  Factory address summary:")
        for fa, count in factory_data["vault_factory_address"].value_counts().items():
            vault_names = factory_data[factory_data["vault_factory_address"] == fa]["vault_name"].tolist()
            w(f"    {fa}: {count} vault(s) — {', '.join(str(n) for n in vault_names)}")
    else:
        w("\n  ── VAULT FACTORY ADDRESSES ──")
        w("  [vault_factory_address NOT in block1_vaults_graphql.csv — re-run pipeline with updated query]")

    # ── NEW: Admin Events ──
    admin_events = read("block1_admin_events.csv")
    if not admin_events.empty:
        w(f"\n  ── ADMIN EVENTS ({len(admin_events)} total) ──")

        # Toxic-market events only
        toxic_events = admin_events[admin_events["involves_toxic_market"] == True] if "involves_toxic_market" in admin_events.columns else pd.DataFrame()

        if not toxic_events.empty:
            w(f"  Events involving toxic markets: {len(toxic_events)}")
            w(f"\n  {'Vault':<35s} {'Event':<25s} {'DateTime':<20s} {'Market':<20s} {'Cap/Assets'}")
            w(f"  {'─'*35} {'─'*25} {'─'*20} {'─'*20} {'─'*15}")
            for _, e in toxic_events.sort_values("timestamp").iterrows():
                vname = str(e.get("vault_name", "?"))[:35]
                etype = str(e.get("event_type", "?"))[:25]
                dt = str(e.get("datetime", ""))[:20]
                mkt = f"{e.get('collateral_symbol','?')}/{e.get('loan_symbol','?')}"[:20]
                cap = e.get("cap_value", "")
                assets = e.get("assets_moved", "")
                extra = ""
                if pd.notna(cap) and str(cap) != "" and str(cap) != "nan":
                    extra = f"cap={cap}"
                elif pd.notna(assets) and str(assets) != "" and str(assets) != "nan":
                    extra = f"assets={assets}"
                w(f"  {vname:<35s} {etype:<25s} {dt:<20s} {mkt:<20s} {extra}")

            # Key insight: cap-to-zero events
            cap_events = toxic_events[toxic_events["event_type"] == "SetCap"]
            if not cap_events.empty:
                w(f"\n  🔍 SetCap events on toxic markets (cap=0 means curator blocked supply):")
                for _, e in cap_events.iterrows():
                    cap_val = e.get("cap_value", "?")
                    w(f"    {e.get('vault_name','?')} → {e.get('collateral_symbol','?')}/{e.get('loan_symbol','?')}: "
                      f"cap={cap_val} at {e.get('datetime','?')}")

            # Queue changes
            queue_events = toxic_events[toxic_events["event_type"].isin(["SetWithdrawQueue", "SetSupplyQueue"])]
            if not queue_events.empty:
                w(f"\n  🔍 Queue changes involving toxic markets:")
                for _, e in queue_events.iterrows():
                    w(f"    {e.get('vault_name','?')}: {e.get('event_type','')} at {e.get('datetime','?')}")
                    qkeys = str(e.get("queue_market_keys", ""))
                    if qkeys:
                        w(f"      Queue markets: {qkeys[:120]}{'...' if len(qkeys)>120 else ''}")

            # Reallocations
            realloc_events = toxic_events[toxic_events["event_type"].isin(
                ["ReallocateSupply", "ReallocateWithdraw"])]
            if not realloc_events.empty:
                w(f"\n  🔍 Reallocations involving toxic markets:")
                for _, e in realloc_events.iterrows():
                    assets = e.get("assets_moved", "?")
                    w(f"    {e.get('vault_name','?')}: {e.get('event_type','')} "
                      f"assets={assets} at {e.get('datetime','?')}")
        else:
            w("  No events involving toxic markets found in admin events data")
            w("  (This may mean curators haven't taken action, or events use different market keys)")

        # All events summary by vault
        w(f"\n  Admin events summary by vault:")
        for vaddr, g in admin_events.groupby("vault_address"):
            vname = g["vault_name"].iloc[0]
            n_total = len(g)
            n_toxic = g["involves_toxic_market"].sum() if "involves_toxic_market" in g.columns else 0
            types = g["event_type"].value_counts().to_dict()
            types_str = ", ".join(f"{t}:{c}" for t, c in types.items())
            w(f"    {vname}: {n_total} events ({n_toxic} toxic) — {types_str}")
    else:
        w("\n  ── ADMIN EVENTS ──")
        w("  [block1_admin_events.csv NOT FOUND — re-run pipeline with updated query]")

    # ── DIAGNOSTIC: Correct vault-level TVL from allocation timeseries ──
    alloc_ts = read("block3_allocation_timeseries.csv")
    if not alloc_ts.empty and "supply_assets_usd" in alloc_ts.columns:
        alloc_ts["supply_assets_usd"] = pd.to_numeric(alloc_ts["supply_assets_usd"], errors="coerce").fillna(0)

        w("\n  ── DIAGNOSTIC: Allocation Timeseries TVL (CORRECT vault-level data) ──")
        w(f"  Source: block3_allocation_timeseries.csv ({len(alloc_ts)} rows)")
        w(f"  This is the CORRECT vault-level allocation to toxic markets.")
        w(f"  The block2 totalAssetsUsd is MARKET-level and should NOT be used for vault TVL.\n")

        a_grp = "vault_address" if "vault_address" in alloc_ts.columns else "vault_name"
        w(f"  {'Vault':<35s} {'Chain':<8s} {'PreDepeg Alloc':>14s} {'Peak Alloc':>14s} {'B2 Peak TVL':>14s} {'Inflation':>10s}")
        w(f"  {'─'*35} {'─'*8} {'─'*14} {'─'*14} {'─'*14} {'─'*10}")

        for gid, g in alloc_ts.groupby(a_grp):
            addr = str(gid).lower()
            vname = g["vault_name"].iloc[0] if "vault_name" in g.columns else "?"
            chain = g["chain"].iloc[0] if "chain" in g.columns else "?"

            pre = g[g["date"] <= "2025-11-03"]
            pre_val = 0
            if not pre.empty:
                latest = pre["date"].max()
                pre_val = pre[pre["date"] == latest]["supply_assets_usd"].sum()

            peak_val = g.groupby("date")["supply_assets_usd"].sum().max()

            # Compare with block2 inflated TVL
            b2_peak = 0
            if not sp_summary.empty and "vault_address" in sp_summary.columns:
                b2_match = sp_summary[sp_summary["vault_address"].str.lower() == addr]
                if not b2_match.empty:
                    b2_peak = float(b2_match.iloc[0].get("tvl_at_peak_usd", 0) or 0)

            inflation = f"{b2_peak/pre_val:.0f}x" if pre_val > 100 and b2_peak > pre_val * 2 else "OK"

            w(f"  {str(vname)[:35]:<35s} {str(chain)[:8]:<8s} {_fmt(pre_val):>14s} {_fmt(peak_val):>14s} {_fmt(b2_peak):>14s} {inflation:>10s}")

    # ── DIAGNOSTIC: Loss-Not-Realized Damage Detection ──
    # (Now integrated into Section 3 main output above — kept here as reference)
    if not alloc_ts.empty and not markets.empty and "supply_assets_usd" in alloc_ts.columns:
        w("\n  ── DIAGNOSTIC: Loss-Not-Realized Detection (full scan at $100 threshold) ──")
        w(f"  (Main Section 3 uses $100K threshold — diagnostics show all at $100)\n")

        a_grp2 = "vault_address" if "vault_address" in alloc_ts.columns else "vault_name"

        # Build bad-debt market lookup
        bd_markets = {}
        for _, m in markets.iterrows():
            bd = float(m.get("bad_debt_usd", 0) or 0)
            if bd > 100:
                mid = str(m.get("market_id", "")).lower()
                bd_markets[mid] = {
                    "bad_debt_usd": bd,
                    "total_supply_usd": float(m.get("total_supply_usd", 0) or 0),
                    "label": f"{m.get('collateral_symbol','?')}/{m.get('loan_symbol','?')} ({str(m.get('chain',''))[:3]})",
                }

        # Get SP-damaged vault addresses (to exclude)
        sp_damaged = set()
        if not sp_summary.empty:
            sp_summary["max_drawdown_pct"] = pd.to_numeric(sp_summary.get("max_drawdown_pct", 0), errors="coerce").fillna(0)
            for _, r in sp_summary.iterrows():
                if r["max_drawdown_pct"] > 0.001:
                    sp_damaged.add(str(r.get("vault_address", "")).lower())

        for gid, g in alloc_ts.groupby(a_grp2):
            addr = str(gid).lower()
            if addr in sp_damaged:
                continue

            pre = g[g["date"] <= "2025-11-03"]
            if pre.empty:
                continue

            latest = pre["date"].max()
            day_data = pre[pre["date"] == latest]

            vault_bd_share = 0
            exposures = []
            for _, row in day_data.iterrows():
                mkey = str(row.get("market_unique_key", "")).lower()
                supply = float(row["supply_assets_usd"])
                for mid, minfo in bd_markets.items():
                    if mkey == mid or mkey.startswith(mid[:10]):
                        tsup = minfo["total_supply_usd"]
                        share = (supply / tsup * minfo["bad_debt_usd"]) if tsup > 0 else 0
                        vault_bd_share += share
                        exposures.append(f"{minfo['label']}: alloc={_fmt(supply)}, bd_share={_fmt(share)}")

            if vault_bd_share > 100:
                vname = g["vault_name"].iloc[0] if "vault_name" in g.columns else "?"
                chain = g["chain"].iloc[0] if "chain" in g.columns else "?"
                pre_total = day_data["supply_assets_usd"].sum()
                haircut = vault_bd_share / pre_total if pre_total > 0 else 0
                w(f"    LOSS-NOT-REALIZED: {vname} ({chain})")
                w(f"      Address: {addr}")
                w(f"      Pre-depeg allocation: {_fmt(pre_total)}")
                w(f"      Est. bad debt share:  {_fmt(vault_bd_share)} ({haircut:.1%} effective haircut)")
                for exp in exposures:
                    w(f"      Market: {exp}")

    # ── ANALYSIS: Why Liquidations Failed ──
    w("\n" + "─" * 78)
    w("WHY LIQUIDATIONS FAILED")
    w("─" * 78)
    w("  Root cause: Oracle prices diverged massively from market prices")
    w("")
    w("  Morpho Blue liquidations require LTV > LLTV, where:")
    w("    LTV = Borrowed Amount / (Collateral Amount × Oracle Price)")
    w("")
    w("  The oracle's price() return is the SOLE determinant of position health.")

    # Pull actual oracle data from block2_bad_debt_by_market.csv
    bd_oracle = read("block2_bad_debt_by_market.csv")
    if not bd_oracle.empty and "oracle_type" in bd_oracle.columns:
        toxic_bd = bd_oracle[
            (pd.to_numeric(bd_oracle.get("L2_bad_debt_usd", 0), errors="coerce") > 1000) |
            (pd.to_numeric(bd_oracle.get("L2_realized_bad_debt_usd", 0), errors="coerce") > 1000)
        ]
        w("")
        w("  Oracle evidence from data (block2_bad_debt_by_market.csv):")
        for _, r in toxic_bd.iterrows():
            label = f"{r.get('collateral_symbol','?')}/{r.get('loan_symbol','?')} ({str(r.get('chain',''))[:3]})"
            otype = r.get("oracle_type", "?")
            is_hc = r.get("oracle_is_hardcoded", "?")
            arch = r.get("oracle_architecture", "?")
            spot = r.get("collateral_spot_price", "?")
            bd_usd = float(r.get("L2_bad_debt_usd", 0))
            rbd_usd = float(r.get("L2_realized_bad_debt_usd", 0) or 0)
            util = float(r.get("utilization", 0))
            price_norm = r.get("L3_oracle_price_normalized", r.get("oracle_price_normalized", "?"))
            w(f"    {label}:")
            w(f"      oracle_type:         {otype}")
            w(f"      oracle_architecture: {arch}")
            w(f"      oracle_is_hardcoded: {is_hc}")
            w(f"      oracle_price_norm:   {price_norm}")
            w(f"      collateral_spot:     ${spot}")
            w(f"      bad_debt_unrealized: ${bd_usd:,.0f}")
            w(f"      bad_debt_realized:   ${rbd_usd:,.0f}")
            w(f"      utilization:         {util:.0%}")

            # Feed/vault descriptions
            bf1_desc = r.get("oracle_base_feed_one_desc", "") or r.get("feed_base_one_desc", "")
            bv_desc = r.get("oracle_base_vault_vendor", "") or r.get("feed_base_vault_desc", "")
            if pd.notna(bf1_desc) and str(bf1_desc) not in ("", "nan", "None"):
                w(f"      base_feed_desc:     {bf1_desc}")
            if pd.notna(bv_desc) and str(bv_desc) not in ("", "nan", "None"):
                w(f"      base_vault_desc:    {bv_desc}")

            if str(is_hc).lower() == "true":
                w(f"      → TRUE hardcoded: no feed addresses (pure constant return)")
            else:
                feed = r.get("oracle_base_feed_one", "")
                vault = r.get("oracle_base_vault", "")
                if pd.notna(feed) and str(feed) not in ("nan", "", "None",
                        "0x0000000000000000000000000000000000000000"):
                    w(f"      → Has Chainlink feed ({str(feed)[:14]}...) but oracle price")
                    w(f"        diverges from spot — mechanism requires further research")
                elif pd.notna(vault) and str(vault) not in ("nan", "", "None",
                        "0x0000000000000000000000000000000000000000"):
                    w(f"      → Vault-based oracle ({str(vault)[:14]}...)")
                    w(f"        price derived from ERC4626 conversion rate")

        # Realized vs unrealized summary
        total_unreal = pd.to_numeric(bd_oracle.get("L2_bad_debt_usd", 0), errors="coerce").fillna(0).sum()
        total_real = pd.to_numeric(bd_oracle.get("L2_realized_bad_debt_usd", 0), errors="coerce").fillna(0).sum()
        w("")
        w(f"  Bad Debt Summary (from Morpho API):")
        w(f"    Unrealized (market.badDebt):         ${total_unreal:,.0f}")
        w(f"    Realized  (market.realizedBadDebt):  ${total_real:,.0f}")
        w(f"    Combined:                            ${total_unreal + total_real:,.0f}")
    else:
        w("")
        w("  [block2_bad_debt_by_market.csv not found — cannot verify oracle details]")

    w("")
    w("  Result: Oracle prices >> spot prices → protocol computes LTV << LLTV")
    w("    → liquidate() reverts → bad debt accumulates silently")
    w("")
    w("  Market creators (not Morpho governance) chose these oracles.")
    w("  Oracle address is immutable once market is deployed.")
    w("  Even if liquidations fired, liquidators would receive worthless")
    w("  xUSD collateral (spot price near $0).")

    # ── ANALYSIS: Credit vs Liquidity Risk ──
    w("\n" + "─" * 78)
    w("SHARED RISK: CREDIT ISOLATED, LIQUIDITY NOT")
    w("─" * 78)
    w("")
    w("  ✅ Credit Risk: ISOLATED")
    w("     Only 3 vaults out of ~320 suffered actual bad debt.")
    w("     No vault without xUSD/deUSD exposure lost money.")
    w("     Protocol market-level isolation worked as designed.")
    w("")
    w("  ⚠️  Liquidity Risk: NOT ISOLATED")
    w("     Multiple vaults allocate to shared underlying markets.")
    w("     When toxic vaults pulled liquidity to service panic withdrawals,")
    w("     clean vaults sharing those markets lost withdrawal access.")
    w("     Gauntlet (zero toxic exposure) hit near-zero withdrawable")
    w("     liquidity for ~6 hours on Nov 4.")
    w("     Restored within hours — Morpho's adaptive interest rate model")
    w("     raised borrowing costs 4x when utilization hit 100%.")
    w("")
    w("  Morpho Vaults V2 response (per public announcements):")
    w("  in-kind redemptions, Sentinel role, ID-based cap system, configurable timelocks.")

    # ══════════════════════════════════════════════════════════════
    # SECTION 4: CURATOR RESPONSE
    # ══════════════════════════════════════════════════════════════
    w("\n" + "─" * 78)
    w("SECTION 4: CURATOR RESPONSE")
    w("─" * 78)

    profiles = read("block3_curator_profiles.csv")
    if not profiles.empty and "response_class" in profiles.columns:
        counts = profiles["response_class"].value_counts()
        for cls in ["PROACTIVE", "EARLY_REACTOR", "SLOW_REACTOR", "VERY_LATE", "NO_EXIT"]:
            if cls in counts.index:
                w(f"  {cls:<18s}: {counts[cls]}")

        w("\n  Vault Detail:")
        cols = ["vault_name", "response_class", "days_vs_depeg"]
        if "earliest_action_date" in profiles.columns:
            cols.append("earliest_action_date")
        for _, r in profiles[cols].iterrows():
            name = r.get("vault_name", "?")
            rc = r.get("response_class", "?")
            days = r.get("days_vs_depeg", "?")
            date = r.get("earliest_action_date", "")
            w(f"    {name:<40s}  {rc:<16s}  days={days}  date={date}")
    else:
        w("  [block3_curator_profiles.csv NOT FOUND or missing response_class]")

    # ══════════════════════════════════════════════════════════════
    # SECTION 5: LIQUIDITY STRESS
    # ══════════════════════════════════════════════════════════════
    w("\n" + "─" * 78)
    w("SECTION 5: LIQUIDITY STRESS")
    w("─" * 78)

    util = read("block3_market_utilization_hourly.csv")
    if not util.empty and "utilization" in util.columns:
        util["utilization"] = pd.to_numeric(util["utilization"], errors="coerce")
        max_util = util.groupby(
            util.apply(lambda r: f"{r.get('collateral_symbol','?')}/{r.get('loan_symbol','?')}", axis=1)
        )["utilization"].max()
        at_100 = (max_util >= 0.99).sum()
        w(f"  Markets reaching 100% util: {at_100}")
    else:
        w("  [block3_market_utilization_hourly.csv NOT FOUND]")

    flows = read("block3_vault_net_flows.csv")
    if not flows.empty:
        tvl_col = "total_assets_usd" if "total_assets_usd" in flows.columns else "tvl_usd"
        flow_col = "net_flow_usd" if "net_flow_usd" in flows.columns else "daily_flow_usd"
        if flow_col in flows.columns:
            flows[flow_col] = pd.to_numeric(flows[flow_col], errors="coerce").fillna(0)
            w(f"  Peak single-day outflow: {_fmt(flows[flow_col].min())}")
    else:
        w("  [block3_vault_net_flows.csv NOT FOUND]")

    # ══════════════════════════════════════════════════════════════
    # SECTION 6: LIQUIDATION FAILURE
    # ══════════════════════════════════════════════════════════════
    w("\n" + "─" * 78)
    w("SECTION 6: LIQUIDATION FAILURE")
    w("─" * 78)

    ltv = read("block5_ltv_analysis.csv")
    if not ltv.empty:
        for col in ["borrow_usd", "true_ltv_pct", "oracle_ltv_pct", "price_gap_pct"]:
            if col in ltv.columns:
                ltv[col] = pd.to_numeric(ltv[col], errors="coerce").fillna(0)
        total_borrow = ltv["borrow_usd"].sum() if "borrow_usd" in ltv.columns else 0
        w(f"  Liquidation Events:  0")
        w(f"  Trapped Borrow:      {_fmt(total_borrow)}")
        w(f"  Oracle Price:        diverges from spot (see oracle evidence above)")

        w("\n  LTV Detail:")
        for _, r in ltv.iterrows():
            label = f"{r.get('collateral_symbol','?')}/{r.get('loan_symbol','?')}"
            chain = str(r.get("chain", ""))[:3]
            borrow = _fmt(r.get("borrow_usd", 0))
            oracle_ltv = r.get("oracle_ltv_pct", 0)
            true_ltv = r.get("true_ltv_pct", 0)
            gap = r.get("price_gap_pct", 0)
            w(f"    {label:<25s} ({chain})  borrow={borrow:>10s}  oracle_ltv={oracle_ltv:>8.1f}%  true_ltv={true_ltv:>8.1f}%  gap={gap:.1f}%")
    else:
        w("  [block5_ltv_analysis.csv NOT FOUND]")

    borrowers = read("block5_borrower_positions.csv")
    if not borrowers.empty and "position_type" in borrowers.columns:
        borr = borrowers[borrowers["position_type"] == "borrower"]
        w(f"\n  Borrower positions:   {len(borr)}")

    # ══════════════════════════════════════════════════════════════
    # SECTION 7: CONTAGION
    # ══════════════════════════════════════════════════════════════
    w("\n" + "─" * 78)
    w("SECTION 7: CONTAGION ASSESSMENT")
    w("─" * 78)

    bridges = read("block6_contagion_bridges.csv")
    exposure = read("block6_vault_allocation_summary.csv")
    exposure_raw = read("block6_vault_market_exposure.csv")

    if not exposure_raw.empty:
        w(f"  Total vault-market exposures: {len(exposure_raw)}")
    if not bridges.empty:
        bp_col = "bridge_type" if "bridge_type" in bridges.columns else "contagion_path"
        if bp_col in bridges.columns:
            n_bridges = len(bridges[bridges[bp_col] == "BRIDGE"])
        else:
            n_bridges = len(bridges)
        w(f"  Contagion bridges:   {n_bridges}")

        w("\n  Bridge Detail:")
        for _, b in bridges.iterrows():
            name = b.get("vault_name", "?")
            toxic = b.get("n_toxic_markets", b.get("toxic_markets", "?"))
            clean = b.get("n_clean_markets", b.get("clean_markets", "?"))
            toxic_usd = b.get("toxic_supply_usd", b.get("toxic_exposure_usd", 0))
            clean_usd = b.get("clean_supply_usd", b.get("clean_exposure_usd", 0))
            w(f"    {name:<35s}  toxic_mkts={toxic}  toxic$={_fmt(toxic_usd)}  clean_mkts={clean}  clean$={_fmt(clean_usd)}")

    # ══════════════════════════════════════════════════════════════
    # VAULT MASTER LIST
    # ══════════════════════════════════════════════════════════════
    w("\n" + "─" * 78)
    w("VAULT MASTER LIST (from block1_vaults_graphql.csv)")
    w("─" * 78)

    if not vaults_raw.empty:
        vaults_raw["vault_total_assets_usd"] = pd.to_numeric(vaults_raw.get("vault_total_assets_usd", 0), errors="coerce").fillna(0)
        vaults_raw["supply_assets_usd"] = pd.to_numeric(vaults_raw.get("supply_assets_usd", 0), errors="coerce").fillna(0)

        # Deduplicate to one row per vault
        seen = set()
        w(f"\n  {'Vault':<40s} {'Chain':<8s} {'ChainID':>7s} {'Curator':<20s} {'TVL':>14s} {'Exposure':>12s} {'SP':>10s} {'Status':<22s} {'Discovery'}")
        w(f"  {'─'*40} {'─'*8} {'─'*7} {'─'*20} {'─'*14} {'─'*12} {'─'*10} {'─'*22} {'─'*20}")

        for _, r in vaults_raw.sort_values("vault_total_assets_usd", ascending=False).iterrows():
            addr = str(r.get("vault_address", "")).lower()
            cid = int(r.get("chain_id", 0))
            key = (addr, cid)
            if key in seen:
                continue
            seen.add(key)

            name = str(r.get("vault_name", "?"))[:40]
            chain = str(r.get("chain", r.get("blockchain", "")))[:8]
            curator = str(r.get("curator_name", ""))[:20]
            tvl = _fmt(r.get("vault_total_assets_usd", 0))
            exp = _fmt(r.get("supply_assets_usd", 0))
            sp = f"{float(r.get('vault_share_price', 0)):.6f}"
            status = str(r.get("exposure_status", ""))[:22]
            disc = str(r.get("discovery_method", ""))

            w(f"  {name:<40s} {chain:<8s} {cid:>7d} {curator:<20s} {tvl:>14s} {exp:>12s} {sp:>10s} {status:<22s} {disc}")

    # ══════════════════════════════════════════════════════════════
    # DATA FILE INVENTORY
    # ══════════════════════════════════════════════════════════════
    w("\n" + "─" * 78)
    w("DATA FILE INVENTORY")
    w("─" * 78)

    if DATA_DIR.exists():
        for f in sorted(DATA_DIR.glob("*.csv")):
            try:
                rows = sum(1 for _ in open(f)) - 1
                size = f.stat().st_size / 1024
                modified = datetime.fromtimestamp(f.stat().st_mtime).strftime("%Y-%m-%d %H:%M")
                w(f"  {f.name:<45s}  {rows:>6d} rows  {size:>8.1f} KB  {modified}")
            except Exception:
                w(f"  {f.name:<45s}  [error reading]")

    w("\n" + "=" * 78)
    w("END OF SNAPSHOT")
    w("=" * 78)

    # Write
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    SNAPSHOT_PATH.write_text("\n".join(lines), encoding="utf-8")
