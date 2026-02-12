"""Section 1: Overview & Timeline — Case study introduction and key metrics."""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from utils.data_loader import load_markets, load_vaults, load_timeline, load_asset_prices
from utils.charts import apply_layout, SEVERITY_COLORS, RED, GREEN, BLUE, format_usd


def render():
    st.title("Overview — xUSD / deUSD Depeg Event")

    # ── Key Metrics ─────────────────────────────────────────
    markets = load_markets()
    vaults = load_vaults()

    if markets.empty and vaults.empty:
        st.error("⚠️ Core data not available — run the pipeline to generate `block1_markets_graphql.csv` and `block1_vaults_graphql.csv`.")
        return

    total_bad_debt = markets["bad_debt_usd"].sum() if not markets.empty else 0
    n_chains = markets["chain"].nunique() if not markets.empty and "chain" in markets.columns else 0
    chains_list = ", ".join(sorted(markets["chain"].dropna().unique())) if n_chains > 0 else ""

    st.caption(
        f"November 2025: A stablecoin depeg cascaded through Morpho Blue markets, "
        f"exposing {format_usd(total_bad_debt)} in toxic collateral across "
        f"{len(markets)} markets and {len(vaults)} vaults on {n_chains} chains.".replace("$", "\\$")
    )

    # Compute liquidation event count from data if available
    from utils.data_loader import load_csv
    liq_events = load_csv("block5_liquidation_events.csv")
    if not liq_events.empty and "event_count" in liq_events.columns:
        n_liquidations = int(liq_events["event_count"].sum())
    elif not liq_events.empty:
        n_liquidations = len(liq_events)
    else:
        n_liquidations = 0

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Toxic Markets", f"{len(markets)}" if not markets.empty else "—", help="Markets using xUSD, deUSD, or sdeUSD as collateral")
    c2.metric("Affected Vaults", f"{len(vaults)}" if not vaults.empty else "—", help="Unique vaults with current or historical exposure")
    c3.metric("Total Bad Debt", format_usd(total_bad_debt), delta=f"-{format_usd(total_bad_debt)}", delta_color="inverse")
    c4.metric("Chains Affected", str(n_chains), help=chains_list)
    c5.metric("Liquidation Events", str(n_liquidations),
              help="Oracle masking prevented liquidations — see Liquidation Failure page")

    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)

    # ── Asset Price Collapse ────────────────────────────────
    st.subheader("Token Price History")

    prices = load_asset_prices()
    if prices.empty:
        st.error("⚠️ Asset price data not available — run the pipeline to generate `block5_asset_prices.csv`.")
    else:
        # Check which assets are present
        available_assets = set(prices["asset"].unique()) if "asset" in prices.columns else set()
        expected_assets = {"xUSD", "deUSD", "sdeUSD"}
        missing_assets = expected_assets - available_assets
        if missing_assets:
            st.warning(f"⚠️ Price data missing for: {', '.join(sorted(missing_assets))}. Only showing available assets.")

        fig = go.Figure()
        colors = {"xUSD": RED, "deUSD": "#f97316", "sdeUSD": "#eab308"}
        for asset in ["xUSD", "deUSD", "sdeUSD"]:
            mask = prices["asset"] == asset
            fig.add_trace(go.Scatter(
                x=prices.loc[mask, "timestamp"],
                y=prices.loc[mask, "price_usd"],
                name=asset,
                line=dict(color=colors.get(asset, BLUE), width=2),
                connectgaps=False,
                hovertemplate="%{x}<br>%{y:$.4f}<extra>" + asset + "</extra>",
            ))

        fig.add_vline(x=pd.Timestamp("2025-11-04"), line_dash="dash", line_color=RED, opacity=0.5)
        fig.add_annotation(x=pd.Timestamp("2025-11-04"), y=1, yref="paper", text="Depeg Start",
                           showarrow=False, font=dict(size=10, color=RED), yshift=10)
        fig.add_vline(x=pd.Timestamp("2025-11-06"), line_dash="dot", line_color="#f97316", opacity=0.5)
        fig.add_annotation(x=pd.Timestamp("2025-11-06"), y=0, yref="paper", text="deUSD Sunset",
                           showarrow=False, font=dict(size=10, color="#f97316"), yshift=-10)
        fig = apply_layout(fig, title="xUSD, deUSD & sdeUSD Price (USD)", height=480)
        fig.update_layout(
            margin=dict(t=50, b=60, l=50, r=20),
            legend=dict(
                orientation="h",
                yanchor="top",
                y=-0.12,
                xanchor="center",
                x=0.5,
                font=dict(size=12),
            ),
        )
        fig.update_yaxes(tickformat="$.2f", range=[0, 1.2])
        st.plotly_chart(fig, use_container_width=True)

    # ── Timeline ────────────────────────────────────────────
    st.subheader("Event Timeline")
    st.caption("See **Background** page for full narrative and source links.")

    timeline = load_timeline()
    if not timeline.empty:
        timeline = timeline.sort_values("date").reset_index(drop=True)

        dot_map = {
            "critical": "🔴",
            "warning": "🟠",
            "positive": "🟢",
            "info": "🔵",
        }

        has_link = "link" in timeline.columns
        has_source = "source" in timeline.columns

        for _, row in timeline.iterrows():
            dot = dot_map.get(row.get("severity", "info"), "⚪")
            date_str = str(row["date"])[:10]
            event_text = row["event"].replace("$", "\\$")
            category = row.get("category", "").replace("_", " ")

            st.markdown(f"**{date_str}** &nbsp; {dot} &nbsp; {event_text}")

            # Source line
            parts = [category]
            if has_source and str(row.get("source", "")).strip() and str(row["source"]) != "nan":
                parts.append(str(row["source"]))
            if has_link and str(row.get("link", "")).strip() and str(row["link"]) != "nan":
                parts.append(f"[Link]({row['link']})")
            st.caption(" · ".join(parts))
            st.markdown("")

    # ── Exposure Breakdown ──────────────────────────────────
    st.subheader("Exposure Breakdown")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**By Collateral Type**")
        if not markets.empty:
            collateral_summary = markets.groupby("collateral").agg(
                markets_count=("chain", "count"),
                total_bad_debt=("bad_debt_usd", "sum"),
            ).reset_index()
            st.dataframe(
                collateral_summary,
                column_config={
                    "collateral": "Collateral",
                    "markets_count": st.column_config.NumberColumn("Markets", format="%d"),
                    "total_bad_debt": st.column_config.NumberColumn("Bad Debt", format="$%,.0f"),
                },
                hide_index=True,
                use_container_width=True,
            )

    with col2:
        st.markdown("**By Chain**")
        if not markets.empty:
            chain_summary = markets.groupby("chain").agg(
                markets_count=("collateral", "count"),
                total_bad_debt=("bad_debt_usd", "sum"),
                total_supply=("supply_usd", "sum"),
            ).reset_index()
            st.dataframe(
                chain_summary,
                column_config={
                    "chain": "Chain",
                    "markets_count": st.column_config.NumberColumn("Markets", format="%d"),
                    "total_bad_debt": st.column_config.NumberColumn("Bad Debt", format="$%,.0f"),
                    "total_supply": st.column_config.NumberColumn("Supply", format="$%,.0f"),
                },
                hide_index=True,
                use_container_width=True,
            )

    # ── Q1 & Q2 Summary ────────────────────────────────────
    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
    st.subheader("Research Questions")

    with st.expander("**Q1:** What was the damage from the deUSD/xUSD situation? How much bad debt, and which vaults?"):
        # Compute Q1 values from data
        _total_bd = markets["bad_debt_usd"].sum() if not markets.empty else 0
        _n_mkts = len(markets) if not markets.empty else 0
        _n_chains = markets["chain"].nunique() if not markets.empty and "chain" in markets.columns else 0
        _largest_mkt = markets.loc[markets["bad_debt_usd"].idxmax(), "market_label"] if _total_bd > 0 else "?"
        _largest_bd = markets["bad_debt_usd"].max() if _total_bd > 0 else 0

        # Damaged vaults from share price summary
        _n_vaults = len(vaults) if not vaults.empty else 0
        _damaged = vaults[vaults["share_price_drawdown"].abs() > 0.01] if not vaults.empty and "share_price_drawdown" in vaults.columns else pd.DataFrame()

        _q1_text = (
            f"**{format_usd(_total_bd)} in protocol-level bad debt** across {_n_mkts} markets "
            f"on {_n_chains} chains, with the largest concentration ({format_usd(_largest_bd)}) in "
            f"{_largest_mkt}. The critical finding: **{n_liquidations} liquidation events** occurred despite "
            f"collateral values declining 95–99% — hardcoded Chainlink oracles continued reporting "
            f"approximately \\$1.00, masking the true risk from the liquidation engine.\n\n"
        )
        if not _damaged.empty:
            _dam_details = []
            for _, _d in _damaged.sort_values("share_price_drawdown").iterrows():
                _dd_pct = abs(_d["share_price_drawdown"])
                _loss = _d.get("tvl_pre_depeg_usd", _d.get("tvl_at_peak_usd", 0)) - _d.get("tvl_usd", 0)
                _dam_details.append(f"**{_d['vault_name']}** ({_dd_pct:.1%} drawdown, {format_usd(_loss)} estimated loss)")
            _q1_text += f"{len(_damaged)} vault{'s' if len(_damaged) > 1 else ''} suffered share-price damage: " + " and ".join(_dam_details)
            _q1_text += f". The remaining {_n_vaults - len(_damaged)} vaults emerged without share-price impact, largely due to proactive curator exits."
        _q1_text = _q1_text.replace("$", "\\$")
        st.markdown(_q1_text)

    with st.expander("**Q2:** How could the Morpho protocol itself have been more resilient?"):
        # Compute Q2 values from data
        _n_instant_tl = len(vaults[vaults["timelock_days"] == 0]) if not vaults.empty and "timelock_days" in vaults.columns else 0
        st.markdown(f"""
        Four structural areas identified: **(1) Oracle architecture** — Chainlink adapters lacked circuit-breakers
        or deviation thresholds, allowing stale \\$1.00 prices during collateral value declines of 95%+.
        **(2) Timelock configuration** — {_n_instant_tl} of {_n_vaults}
        vaults had instant (0-day) timelocks, enabling unchecked exposure changes, while longer timelocks
        delayed emergency exits during the crisis. **(3) No automated exit
        triggers** — curator-dependent response meant speed varied significantly across vaults.
        **(4) Liquidity contagion paths** — vaults bridging toxic and clean markets meant
        depositors in unaffected markets could experience reduced liquidity during stress events.
        """)
