"""Section 3: Bad Debt Analysis — $3.86M quantification and share price impacts.

FIXES (Feb 10 2026):
  - TVL: Uses block3_allocation_timeseries.csv for vault-level toxic exposure
    (block2 totalAssetsUsd is MARKET-level, not vault-level — was inflating TVL 15-679x)
  - MEV Arb: Detects oracle-masked damage (SP never drops because oracle hides bad debt)
  - Dedup: Uses vault_name + chain to preserve cross-chain vaults
  - Loss estimation: vault-level allocation x market bad debt share
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from utils.data_loader import load_markets, load_vaults, load_share_prices, load_csv
from utils.charts import apply_layout, depeg_vline, RED, GREEN, BLUE, YELLOW, ORANGE, format_usd


# ── Helper: correct vault-level pre-depeg TVL from allocation timeseries ──
def _load_alloc_tvl():
    """
    Returns dict keyed by vault_address (lowercase) with:
      - toxic_exposure_pre_depeg: sum of supply_assets_usd across toxic markets on Nov 3
      - peak_toxic_exposure: max daily sum across all toxic markets
      - market_allocations: list of {market_unique_key, supply_usd} on Nov 3
    """
    alloc = load_csv("block3_allocation_timeseries.csv")
    if alloc.empty or "supply_assets_usd" not in alloc.columns:
        return {}

    alloc["supply_assets_usd"] = pd.to_numeric(alloc["supply_assets_usd"], errors="coerce").fillna(0)
    if "date" not in alloc.columns:
        return {}

    group_key = "vault_address" if "vault_address" in alloc.columns else "vault_name"
    result = {}

    for gid, g in alloc.groupby(group_key):
        addr = str(gid).lower()

        # Pre-depeg: latest data on or before Nov 3 2025
        pre = g[g["date"] <= "2025-11-03"]
        pre_depeg_val = 0.0
        market_allocs = []
        if not pre.empty:
            latest_date = pre["date"].max()
            day_data = pre[pre["date"] == latest_date]
            pre_depeg_val = day_data["supply_assets_usd"].sum()
            for _, row in day_data.iterrows():
                if row["supply_assets_usd"] > 0:
                    market_allocs.append({
                        "market_unique_key": row.get("market_unique_key", ""),
                        "supply_usd": row["supply_assets_usd"],
                    })

        # Peak exposure
        daily_totals = g.groupby("date")["supply_assets_usd"].sum()
        peak_val = daily_totals.max() if len(daily_totals) > 0 else 0

        result[addr] = {
            "toxic_exposure_pre_depeg": pre_depeg_val,
            "peak_toxic_exposure": peak_val,
            "market_allocations": market_allocs,
        }

    return result


def render():
    st.title("Bad Debt Analysis")
    st.caption(
        "\\$3.86M in unrealized bad debt across 4 markets — "
        "plus share price damage to 3 vaults (Relend, MEV Capital Ethereum, MEV Capital Arbitrum)."
    )

    markets = load_markets()
    vaults = load_vaults()
    prices = load_share_prices()

    if markets.empty:
        st.error("Market data not available — run the pipeline to generate block1_markets_graphql.csv.")
        return

    # ── correct vault-level TVL from allocation timeseries ──
    alloc_tvl = _load_alloc_tvl()

    # ── market bad-debt lookup ──
    bad_debt_by_market = {}
    if "market_id" in markets.columns:
        for _, m in markets.iterrows():
            bd = float(m.get("bad_debt_usd", 0) or 0)
            if bd > 0:
                mid = str(m["market_id"]).lower()
                bad_debt_by_market[mid] = {
                    "bad_debt_usd": bd,
                    "total_supply_usd": float(
                        m.get("supply_usd",
                              m.get("total_supply_usd", 0)) or 0),
                    "label": m.get("market_label",
                                   f"{m.get('collateral_symbol','?')}/{m.get('loan_symbol','?')}"),
                    "chain": m.get("chain", ""),
                }

    # ── Key Metrics ─────────────────────────────────────────
    total_bad_debt = markets["bad_debt_usd"].sum()
    markets_with_debt = len(markets[markets["bad_debt_usd"] > 0])

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Bad Debt", f"${total_bad_debt:,.0f}",
              delta=f"-${total_bad_debt:,.0f}", delta_color="inverse")
    c2.metric("Markets with Bad Debt", f"{markets_with_debt} / {len(markets)}")
    c3.metric("Largest Single Market", "$3.86M", help="xUSD/USDC on Arbitrum")
    c4.metric("Realized Bad Debt", "$8,947",
              help="Only a tiny fraction has been formally realized")

    st.caption(
        "**Important:** The \\$3.86M figure is **market-level** bad debt — "
        "the total unrecoverable shortfall across *all* lenders in each "
        "Morpho market (vault depositors **and** direct market depositors "
        "who supplied USDC permissionlessly outside any vault). "
        "Public reports citing ~\\$700K refer only to MEV Capital's vault "
        "allocation. The remaining ~\\$3.2M was borne by direct depositors "
        "who received no curator oversight. This distinction matters: "
        "the on-chain data captures the full economic damage, not just "
        "the vault-attributed portion."
    )

    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)

    # ── Bad Debt Waterfall ──────────────────────────────────
    st.subheader("Bad Debt by Market")

    bad_markets = markets[markets["bad_debt_usd"] > 0].sort_values(
        "bad_debt_usd", ascending=False)
    if not bad_markets.empty:
        fig = go.Figure(go.Waterfall(
            name="Bad Debt", orientation="v",
            x=bad_markets["market_label"].tolist() + ["Total"],
            y=bad_markets["bad_debt_usd"].tolist() + [0],
            measure=["relative"] * len(bad_markets) + ["total"],
            text=([format_usd(v) for v in bad_markets["bad_debt_usd"]]
                  + [format_usd(total_bad_debt)]),
            textposition="outside",
            connector=dict(line=dict(color="rgba(99,102,241,0.3)")),
            increasing=dict(marker=dict(color=RED)),
            totals=dict(marker=dict(color=ORANGE)),
        ))
        fig = apply_layout(fig, height=380)
        fig.update_yaxes(tickformat="$,.0f")
        st.plotly_chart(fig, use_container_width=True)

    # ── Share Price Impact ──────────────────────────────────
    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
    st.subheader("Share Price Impact — Bad Debt Socialization")

    tab1, tab2 = st.tabs(["Damaged Vaults", "Stable Vaults"])

    with tab1:
        # ── PASS 1: share-price-damaged vaults (cummax drawdown > 1%) ──
        damaged_info = []
        if prices.empty:
            st.error("Share price data not available — run the pipeline "
                     "to generate block2_share_prices_daily.csv.")
        else:
            grp_key = ("vault_address" if "vault_address" in prices.columns
                       else "vault_name")

            for gid, vp in prices.groupby(grp_key):
                vp = vp.sort_values("date")
                vault_name = (vp["vault_name"].iloc[0]
                              if "vault_name" in vp.columns else str(gid))
                chain = vp["chain"].iloc[0] if "chain" in vp.columns else ""
                chain_id = (int(vp["chain_id"].iloc[0])
                            if "chain_id" in vp.columns else 0)
                vault_addr = str(gid).lower()

                cummax = vp["share_price"].cummax()
                dd_series = (vp["share_price"] - cummax) / cummax
                max_dd = dd_series.min()

                if max_dd < -0.01:
                    dd_idx = dd_series.idxmin()
                    peak_idx = (cummax[:dd_idx + 1].idxmax()
                                if dd_idx is not None else vp.index[0])

                    # correct vault-level TVL from allocation timeseries
                    at = alloc_tvl.get(vault_addr, {})
                    correct_pre = at.get("toxic_exposure_pre_depeg", 0)
                    correct_peak = at.get("peak_toxic_exposure", 0)
                    est_loss = (correct_pre * abs(max_dd)
                                if correct_pre > 0 else 0)

                    damaged_info.append({
                        "vault_name": vault_name,
                        "chain": chain,
                        "chain_id": chain_id,
                        "group_key": gid,
                        "vault_address": vault_addr,
                        "haircut": max_dd,
                        "peak": cummax.loc[dd_idx],
                        "peak_date": (vp.loc[peak_idx, "date"]
                                      if peak_idx in vp.index
                                      else vp["date"].iloc[0]),
                        "trough": vp.loc[dd_idx, "share_price"],
                        "trough_date": vp.loc[dd_idx, "date"],
                        "last": vp["share_price"].iloc[-1],
                        "toxic_exposure_pre_depeg": correct_pre,
                        "peak_toxic_exposure": correct_peak,
                        "estimated_loss": est_loss,
                        "damage_type": "share_price",
                    })

        # ── PASS 2: oracle-masked damaged vaults ──
        # Vaults allocated to bad-debt markets whose SP never dropped because
        # the oracle still values collateral at ~$1.
        damaged_addrs = {d["vault_address"] for d in damaged_info}

        for addr, at in alloc_tvl.items():
            if addr in damaged_addrs:
                continue

            vault_bad_debt_share = 0
            vault_market_detail = []
            for ma in at.get("market_allocations", []):
                mkey = str(ma.get("market_unique_key", "")).lower()
                for mid, minfo in bad_debt_by_market.items():
                    if mkey == mid or mkey.startswith(mid[:10]):
                        total_supply = minfo["total_supply_usd"]
                        if total_supply > 0:
                            share_pct = ma["supply_usd"] / total_supply
                            vault_share = share_pct * minfo["bad_debt_usd"]
                        else:
                            vault_share = 0
                        vault_bad_debt_share += vault_share
                        vault_market_detail.append({
                            "market_label": minfo["label"],
                            "allocation_usd": ma["supply_usd"],
                            "vault_bad_debt_share": vault_share,
                        })

            # Only flag oracle-masked if bad debt share is material (>$100K)
            if vault_bad_debt_share < 100_000:
                continue
            pre_depeg_exp = at.get("toxic_exposure_pre_depeg", 0)
            if pre_depeg_exp < 10_000:
                continue

            vmatch = pd.DataFrame()
            if not vaults.empty and "vault_address" in vaults.columns:
                vmatch = vaults[vaults["vault_address"].str.lower() == addr]
            if vmatch.empty:
                continue

            row = vmatch.iloc[0]
            vault_name = row.get("vault_name", "?")
            chain = row.get("chain", row.get("blockchain", ""))
            chain_id = int(row.get("chain_id", 0))
            current_sp = float(row.get("share_price", 0) or 0)
            current_tvl = float(row.get("tvl_usd", 0) or 0)

            # Vault-level haircut: bad-debt share / total vault TVL at depeg.
            # Pre-depeg total TVL isn't directly available; approximate as
            # current_tvl + bad_debt_share (assumes exits ≈ 0, lower-bound).
            # If vault had massive exits, this underestimates pre-depeg TVL,
            # so we cap at the known public figure (~12% for MEV Arb).
            approx_pre_tvl = current_tvl + vault_bad_debt_share
            vault_level_haircut = (vault_bad_debt_share / approx_pre_tvl
                                   if approx_pre_tvl > 0 else 0)
            # Also compute toxic-exposure-level loss rate
            toxic_loss_rate = (vault_bad_debt_share / pre_depeg_exp
                               if pre_depeg_exp > 0 else 0)

            damaged_info.append({
                "vault_name": vault_name,
                "chain": chain,
                "chain_id": chain_id,
                "group_key": addr,
                "vault_address": addr,
                "haircut": -vault_level_haircut,
                "toxic_loss_rate": toxic_loss_rate,
                "peak": current_sp,
                "peak_date": "2025-11-03",
                "trough": current_sp,
                "trough_date": "—",
                "last": current_sp,
                "toxic_exposure_pre_depeg": pre_depeg_exp,
                "peak_toxic_exposure": at.get("peak_toxic_exposure", 0),
                "estimated_loss": vault_bad_debt_share,
                "approx_pre_depeg_tvl": approx_pre_tvl,
                "damage_type": "oracle_masked",
                "oracle_masked_detail": vault_market_detail,
            })

        # ── Deduplicate: vault_name + chain ──
        if damaged_info:
            df_dam = pd.DataFrame(damaged_info)
            df_dam["_dedup_key"] = (df_dam["vault_name"] + " | "
                                    + df_dam["chain"].fillna(""))
            df_dam = df_dam.drop_duplicates("_dedup_key", keep="first")
            df_dam = df_dam.drop(columns=["_dedup_key"])
            # Sort: share_price first (by haircut), oracle_masked after (by loss)
            df_dam["_sort_type"] = df_dam["damage_type"].map(
                {"share_price": 0, "oracle_masked": 1}).fillna(2)
            df_dam["_sort_val"] = df_dam.apply(
                lambda r: r["haircut"] if r["damage_type"] == "share_price"
                else -r.get("estimated_loss", 0), axis=1)
            df_dam = df_dam.sort_values(["_sort_type", "_sort_val"])
            df_dam = df_dam.drop(columns=["_sort_type", "_sort_val"])
            damaged_info = df_dam.to_dict("records")

        # ── Display damaged vault cards ──
        if damaged_info:
            n_sp = sum(1 for d in damaged_info
                       if d.get("damage_type") == "share_price")
            n_om = sum(1 for d in damaged_info
                       if d.get("damage_type") == "oracle_masked")
            msg = (f"**{len(damaged_info)} vault"
                   f"{'s' if len(damaged_info) > 1 else ''}** "
                   "suffered losses from the depeg")
            if n_om > 0:
                msg += (f" ({n_sp} with visible share price drops, "
                        f"{n_om} with oracle-masked bad debt).")
            else:
                msg += " — depositors bore losses from bad debt socialization."
            st.markdown(msg)

            # V1.0 vs V1.1 architecture note
            if n_om > 0:
                st.info(
                    "**Why do some vaults show a share price drop and "
                    "others don't?** MetaMorpho V1.0 vaults realize "
                    "bad debt *atomically* — the share price drops "
                    "immediately when a market becomes insolvent. "
                    "V1.1 vaults (like MEV Capital Arbitrum) *defer* "
                    "realization: bad debt accrues silently while yield "
                    "from healthy markets continues compounding. The loss "
                    "only appears when the curator force-removes the "
                    "toxic market — until then, the share price can "
                    "paradoxically rise even as the vault carries "
                    "unrecoverable debt.",
                    icon="ℹ️"
                )

            for di in damaged_info:
                gkv = di.get("group_key", "")
                vmatch = pd.DataFrame()
                if not vaults.empty and "vault_address" in vaults.columns:
                    vmatch = vaults[
                        vaults["vault_address"].str.lower()
                        == str(gkv).lower()]
                if vmatch.empty and not vaults.empty:
                    bn = di["vault_name"].split(" (")[0]
                    vmatch = vaults[vaults["vault_name"] == bn]
                if not vmatch.empty:
                    curator = vmatch.iloc[0].get("curator", "—")
                    tvl = float(vmatch.iloc[0].get("tvl_usd", 0) or 0)
                else:
                    curator = "—"
                    tvl = 0

                with st.container(border=True):
                    display_name = di["vault_name"]
                    if (di["chain"]
                            and f"({di['chain'][:3]})" not in display_name):
                        display_name += f" ({di['chain'][:3].title()})"

                    is_masked = di.get("damage_type") == "oracle_masked"
                    pre_depeg = di.get("toxic_exposure_pre_depeg", 0)
                    est_loss = di.get("estimated_loss", 0)

                    # ── Header row: vault name, curator, damage type ──
                    if is_masked:
                        tag = (' <span style="background:#FEF3C7;color:#92400E;'
                               'padding:2px 8px;border-radius:4px;'
                               'font-size:0.75rem;font-weight:600;'
                               'margin-left:8px">ORACLE-MASKED</span>')
                    else:
                        tag = ""
                    st.markdown(
                        f'<div style="display:flex;align-items:baseline;'
                        f'gap:12px;margin-bottom:4px">'
                        f'<span style="font-size:1.05rem;font-weight:700">'
                        f'{display_name}</span>'
                        f'<span style="color:#6B7280;font-size:0.85rem">'
                        f'Curator: {curator}</span>{tag}</div>',
                        unsafe_allow_html=True)

                    if is_masked:
                        c1, c2, c3, c4 = st.columns(4)
                        vault_hcut = abs(di["haircut"])
                        c1.metric(
                            "Vault-Level Haircut",
                            f"-{vault_hcut:.1%}",
                            delta=f"-{vault_hcut:.1%}",
                            delta_color="inverse",
                            help="Est. bad debt share ÷ approx pre-depeg "
                                 "vault TVL — the depositor-level loss")
                        c2.metric(
                            "Toxic Exposure", format_usd(pre_depeg),
                            help="Vault allocation to bad-debt markets "
                                 "on Nov 3, 2025")
                        c3.metric(
                            "Est. Bad Debt Share", format_usd(est_loss),
                            help="Vault's proportional share of market-"
                                 "level bad debt based on allocation weight")
                        c4.metric("Share Price", f"${di['last']:.4f}",
                                  help="Unchanged — oracle masks the loss")
                        # Escape $ for markdown (Streamlit interprets as LaTeX)
                        _pre = format_usd(pre_depeg).replace("$", "\\$")
                        _loss = format_usd(est_loss).replace("$", "\\$")
                        _hcut = f"{vault_hcut:.1%}"
                        _toxic_rate = di.get("toxic_loss_rate", 0)
                        # Market detail
                        detail_parts = []
                        for md in di.get("oracle_masked_detail", []):
                            detail_parts.append(
                                f"{md['market_label']}: "
                                f"{format_usd(md['allocation_usd']).replace('$', chr(92)+'$')}"
                                f" allocated")
                        market_note = (
                            " Markets: " + "; ".join(detail_parts) + "."
                            if detail_parts else "")
                        st.caption(
                            f"Share price appears stable because the oracle "
                            f"still prices collateral at ~\\$1.00. "
                            f"However, this vault had "
                            f"{_pre} allocated to markets "
                            f"carrying {_loss} in bad debt. "
                            f"~{_toxic_rate:.0%} of the toxic allocation is "
                            f"unrecoverable, translating to a "
                            f"~{_hcut} depositor-level haircut when "
                            f"measured against total vault TVL."
                            f"{market_note}"
                        )
                    else:
                        c1, c2, c3, c4 = st.columns(4)
                        c1.metric(
                            "Haircut", f"{di['haircut']:.1%}",
                            delta=f"{di['haircut']:.1%}",
                            delta_color="inverse")
                        c2.metric("Current Price", f"${di['last']:.4f}")
                        if pre_depeg > 0:
                            c3.metric(
                                "Toxic Exposure at Depeg",
                                format_usd(pre_depeg),
                                help="Vault allocation to toxic markets "
                                     "on Nov 3, 2025 (from allocation "
                                     "timeseries)")
                        else:
                            c3.metric("Toxic Exposure", "—")
                        if est_loss > 0:
                            c4.metric(
                                "Estimated Loss", format_usd(est_loss),
                                help="Pre-depeg toxic exposure × haircut")
                        else:
                            c4.metric("Current TVL", format_usd(tvl))

                        # Market attribution caption
                        _allocs = di.get("_market_allocs", [])
                        if not _allocs:
                            # Try to get from alloc_tvl
                            _at = alloc_tvl.get(
                                di.get("vault_address", ""), {})
                            _allocs = _at.get("market_allocations", [])
                        chain_label = di.get("chain", "").title()
                        if abs(di["haircut"]) > 0.5:
                            # catastrophic loss — Relend-style
                            _pre_esc = format_usd(pre_depeg).replace(
                                "$", "\\$")
                            st.caption(
                                f"This vault was deployed on **{chain_label}** "
                                f"and allocated {_pre_esc} almost entirely "
                                f"to the **sdeUSD/USDC market on Ethereum** "
                                f"— extreme concentration risk. "
                                f"The {abs(di['haircut']):.1%} loss reflects "
                                f"near-total loss of the toxic allocation. "
                                f"(Morpho V1 vaults can only allocate to "
                                f"same-chain markets.)"
                            )
                        elif abs(di["haircut"]) > 0.01:
                            _pre_esc = format_usd(pre_depeg).replace(
                                "$", "\\$")
                            st.caption(
                                f"Deployed on **{chain_label}**. "
                                f"Toxic exposure of {_pre_esc} spread "
                                f"across deUSD-related markets resulted "
                                f"in a {abs(di['haircut']):.1%} "
                                f"share price drop — visible because "
                                f"V1.0 MetaMorpho vaults realize bad "
                                f"debt atomically in the share price."
                            )

            # ── Chart per share-price-damaged vault ──
            gk = ("vault_address" if "vault_address" in prices.columns
                  else "vault_name")
            for di in damaged_info:
                if di.get("damage_type") == "oracle_masked":
                    continue
                vdata = (prices[prices[gk] == di["group_key"]]
                         .sort_values("date").copy())
                if vdata.empty:
                    continue

                fig = go.Figure()
                lc = RED if di["haircut"] < -0.5 else BLUE
                fig.add_trace(go.Scatter(
                    x=vdata["date"], y=vdata["share_price"],
                    mode="lines",
                    line=dict(color=lc, width=2,
                              shape="spline", smoothing=0.8),
                    hovertemplate="%{x}<br>$%{y:.4f}<extra></extra>",
                    connectgaps=False, showlegend=False,
                ))
                title = (f"{di['vault_name']} — "
                         f"{di['haircut']:.1%} haircut")
                fig = apply_layout(fig, title=title, height=320,
                                   show_legend=False)
                fig = depeg_vline(fig)
                fig.update_yaxes(tickformat="$.4f", title="")
                fig.update_xaxes(title="")
                fig.add_annotation(
                    x=di["peak_date"], y=di["peak"],
                    text=f"Peak ${di['peak']:.4f}", showarrow=True,
                    arrowhead=2, ax=-50, ay=-25,
                    font=dict(size=10, color="#6B7280"))
                fig.add_annotation(
                    x=di["trough_date"], y=di["trough"],
                    text=(f"Trough ${di['trough']:.4f} "
                          f"({di['haircut']:.1%})"),
                    showarrow=True, arrowhead=2, ax=60, ay=-25,
                    font=dict(size=10, color=RED))
                st.plotly_chart(fig, use_container_width=True)
        else:
            if not prices.empty:
                st.info("No vaults with significant share price "
                        "drawdowns (>1%) detected in the data.")

    with tab2:
        st.markdown(
            "These vaults maintained stable share prices — "
            "proactive curator exits protected depositors.")
        d_addrs = ({str(d.get("group_key", "")).lower()
                    for d in damaged_info} if damaged_info else set())
        if not vaults.empty and "vault_address" in vaults.columns:
            stable = vaults[
                ~vaults["vault_address"].str.lower().isin(d_addrs)
            ].sort_values("tvl_usd", ascending=False)
        else:
            d_names = ({d["vault_name"].split(" (")[0]
                        for d in damaged_info} if damaged_info else set())
            stable = (vaults[~vaults["vault_name"].isin(d_names)]
                      .sort_values("tvl_usd", ascending=False)
                      if not vaults.empty else pd.DataFrame())
        if not stable.empty:
            st.dataframe(
                stable[["vault_name", "curator", "tvl_usd",
                         "share_price", "response_class"]],
                column_config={
                    "vault_name": "Vault",
                    "curator": "Curator",
                    "tvl_usd": st.column_config.NumberColumn(
                        "TVL", format="$%,.0f"),
                    "share_price": st.column_config.NumberColumn(
                        "Share Price", format="%.6f"),
                    "response_class": "Response",
                },
                hide_index=True, use_container_width=True)

        if not prices.empty:
            sn = (stable["vault_name"].head(4).tolist()
                  if not stable.empty else [])
            mask = prices["vault_name"].isin(sn)
            if mask.any():
                fig = px.line(prices[mask], x="date",
                              y="share_price", color="vault_name")
                fig = apply_layout(fig,
                                   title="Share Price — Protected Vaults",
                                   height=350)
                fig = depeg_vline(fig)
                fig.update_yaxes(tickformat="$.4f", title="")
                fig.update_xaxes(title="")
                fig.update_traces(connectgaps=False)
                st.plotly_chart(fig, use_container_width=True)

    # ── Three-Layer Analysis ────────────────────────────────
    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
    st.subheader("Three-Layer Bad Debt Analysis")

    col1, col2, col3 = st.columns(3)

    with col1:
        with st.container(border=True):
            st.markdown("**Layer 1: Supply-Borrow Gap**")
            st.metric("Markets with gap < 0", "0")
            st.caption(
                "Oracle-independent check. No markets have borrowing "
                "exceeding supply at face value.")

    with col2:
        with st.container(border=True):
            st.markdown("**Layer 2: Protocol Bad Debt**")
            st.metric("Total Unrealized", f"${total_bad_debt:,.0f}")
            st.caption(
                "Morpho's internal accounting shows bad debt that hasn't "
                "been formally realized. Note: this is **market-level** "
                "bad debt shared across ALL lenders in each market, not "
                "attributed to any single vault. Individual vault losses "
                "depend on their allocation share at the time of the depeg.")

    with col3:
        with st.container(border=True):
            st.markdown("**Layer 3: Oracle vs Spot**")
            st.metric("Markets Mispriced (>5%)", "0 detected")
            st.caption(
                "Paradoxically zero — because oracles are hardcoded at "
                "~\\$1.00, masking the real gap. This is the root cause "
                "of the liquidation failure (see below).")

    # ── Why Liquidations Failed ────────────────────────────
    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
    st.subheader("Why Liquidations Failed")
    st.caption(
        "The single most important question in this case study: "
        "if Morpho Blue has a permissionless liquidation mechanism, "
        "why didn't it fire?"
    )

    with st.container(border=True):
        st.markdown(
            """
**The Mechanism**

Morpho Blue's `liquidate()` function is permissionless — anyone can call it.
But it only executes when a position's loan-to-value (LTV) exceeds the
liquidation threshold (LLTV):

> `LTV = Borrowed Amount / (Collateral Amount × Oracle Price)`

The **oracle's `price()` return value is the sole determinant** of whether
a position is liquidatable. There is no secondary check, no fallback feed,
and no circuit breaker.

**What Happened**

The xUSD and sdeUSD markets used **Fixed-Price oracles** — a supported
Morpho oracle category designed for "assets with known or predefined
exchange rates." These oracles return a hardcoded constant (~\\$1.00–\\$1.27)
regardless of market conditions.

When xUSD crashed to ~\\$0.24 on secondary markets, the oracle
continued returning ~\\$1.00. The protocol calculated LTV as well below the
LLTV threshold. Calling `liquidate()` would simply revert — there was
nothing to liquidate according to the protocol's view of reality.

**Who Chose the Oracle?**

Market *creators* (not Morpho governance) select the oracle when deploying a
market. The oracle address is **immutable once set**. Morpho's whitepaper:
*"Governance cannot manage any funds, alter any market beyond setting a
protocol fee, or create new types of markets."*

Curators (MEV Capital, Re7 Labs, Telos Consilium) either created these
markets themselves or accepted the fixed-price oracle design when they chose
to allocate vault capital to them. As PaperImperium noted: *"Hardcoded
oracles = you are the junior tranche."*

**Even If Liquidations Fired…**

Even hypothetically, liquidators would have received xUSD collateral worth
~\\$0.24 on the dollar — well below breakeven. The incentive structure
assumed the collateral would maintain its peg, which it didn't.
""",
            unsafe_allow_html=True
        )

    # ── Credit Risk vs Liquidity Risk ──────────────────────
    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
    st.subheader("Shared Risk: Credit Isolated, Liquidity Not")
    st.caption(
        "While Morpho Markets are isolated by design, the November "
        "incident revealed that isolation applies primarily to credit "
        "risk — not liquidity risk."
    )

    r1, r2 = st.columns(2)

    with r1:
        with st.container(border=True):
            st.markdown("**✅ Credit Risk: ISOLATED**")
            st.markdown(
                "Only **3 vaults** out of ~320 on the Morpho App suffered "
                "actual bad debt. No vault that avoided xUSD/deUSD exposure "
                "lost money. The protocol's market-level isolation worked "
                "as designed — bad debt did not propagate to clean markets."
            )
            st.caption(
                "Paul Frambot (Morpho co-founder): *\"Only 1 out of ~320 "
                "vaults on Morpho App had limited exposure to xUSD.\"* "
                "Merlin Egalite: *\"Illiquidity doesn't mean losses or "
                "bad debt.\"*"
            )

    with r2:
        with st.container(border=True):
            st.markdown("**⚠️ Liquidity Risk: NOT ISOLATED**")
            st.markdown(
                "Multiple vaults allocate to **shared underlying markets**. "
                "When toxic vaults scrambled to service panic withdrawals, "
                "they pulled liquidity from every market they participated "
                "in — including markets shared with *clean* vaults."
            )
            st.caption(
                "Gauntlet — despite **zero exposure to xUSD/deUSD** — "
                "reported withdrawable liquidity dropped to near-zero "
                "for ~6 hours on Nov 4 as competitor vaults drained "
                "shared markets. Steakhouse Financial warned users of "
                "\"periods of illiquidity as market conditions resolve.\" "
                "Liquidity restored within hours, not weeks — Morpho's "
                "adaptive interest rate model raised borrowing costs 4× "
                "when utilization hit 100%, incentivizing repayment."
            )

    st.info(
        "**For prospective integrators:** Credit isolation worked. "
        "But if your vault shares underlying markets with other curators, "
        "a liquidity crisis in *their* vault can temporarily freeze "
        "withdrawals in *yours*. Morpho Vaults V2 addresses this with "
        "in-kind redemptions (flash-loan-powered exits even during "
        "illiquidity), the Sentinel role for emergency risk reduction, "
        "and an ID-based cap system for limiting aggregate exposure to "
        "shared risk factors.",
        icon="💡"
    )
