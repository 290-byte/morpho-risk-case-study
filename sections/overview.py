"""Section 1: Overview & Timeline — Case study introduction and key metrics."""

import streamlit as st
import plotly.graph_objects as go
from utils.data_loader import load_markets, load_vaults, load_timeline, load_asset_prices
from utils.charts import apply_layout, SEVERITY_COLORS, RED, GREEN, BLUE, format_usd


def render():
    st.title("📋 Overview — xUSD / deUSD Depeg Event")
    st.caption(
        "November 2025: A stablecoin depeg cascaded through Morpho Blue markets, "
        "exposing $4.2M in toxic collateral across 18 markets and 33 vaults on 3 chains."
    )

    # ── Key Metrics ─────────────────────────────────────────
    markets = load_markets()
    vaults = load_vaults()

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Toxic Markets", f"{len(markets)}", help="Markets using xUSD, deUSD, or sdeUSD as collateral")
    c2.metric("Affected Vaults", f"{len(vaults)}", help="Unique vaults with current or historical exposure")
    c3.metric("Total Bad Debt", "$3.64M", delta="-$3.64M", delta_color="inverse")
    c4.metric("Chains Affected", "3", help="Ethereum, Arbitrum, Plume")
    c5.metric("Liquidation Events", "0", help="Oracle masking prevented liquidations")

    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)

    # ── Asset Price Collapse ────────────────────────────────
    st.subheader("Token Price Collapse")

    prices = load_asset_prices()
    if not prices.empty:
        fig = go.Figure()
        colors = {"xUSD": RED, "deUSD": "#f97316", "sdeUSD": "#eab308"}
        for asset in ["xUSD", "deUSD", "sdeUSD"]:
            mask = prices["asset"] == asset
            fig.add_trace(go.Scatter(
                x=prices.loc[mask, "timestamp"],
                y=prices.loc[mask, "price_usd"],
                name=asset,
                line=dict(color=colors.get(asset, BLUE), width=2),
                hovertemplate="%{x}<br>%{y:$.4f}<extra>" + asset + "</extra>",
            ))

        fig.add_vline(x="2025-11-04", line_dash="dash", line_color=RED, opacity=0.5,
                       annotation_text="Depeg Start", annotation_position="top")
        fig.add_vline(x="2025-11-06", line_dash="dot", line_color="#f97316", opacity=0.5,
                       annotation_text="deUSD Sunset", annotation_position="bottom")
        fig = apply_layout(fig, title="xUSD, deUSD & sdeUSD Price (USD)", height=420)
        fig.update_yaxes(tickformat="$.2f", range=[0, 1.2])
        st.plotly_chart(fig, use_container_width=True)

    # ── Timeline ────────────────────────────────────────────
    st.subheader("Event Timeline")

    timeline = load_timeline()
    if not timeline.empty:
        fig = go.Figure()
        for _, row in timeline.iterrows():
            color = SEVERITY_COLORS.get(row["severity"], BLUE)
            fig.add_trace(go.Scatter(
                x=[row["date"]],
                y=[row["category"]],
                mode="markers+text",
                marker=dict(size=14, color=color, symbol="diamond"),
                text=[row["event"][:50] + "..." if len(row["event"]) > 50 else row["event"]],
                textposition="middle right",
                textfont=dict(size=9, color=color),
                showlegend=False,
                hovertext=row["event"],
                hoverinfo="text",
            ))

        fig = apply_layout(fig, height=450)
        fig.update_xaxes(title="Date", range=["2025-08-20", "2025-12-20"])
        fig.update_yaxes(title="")
        st.plotly_chart(fig, use_container_width=True)

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
        st.markdown("""
        **$3.64M in protocol-level bad debt** across 18 markets on 3 chains, with the largest concentration
        ($3.64M) in a single xUSD/USDC market on Arbitrum. The critical finding: **zero liquidation events** occurred
        despite collateral values collapsing 95–99% — hardcoded Chainlink oracles continued reporting ≈$1.00,
        completely masking the true risk.

        Two vaults suffered share-price damage: **MEV Capital USDC** (3.13% drawdown, $9.2M estimated loss)
        and **Relend USDC** (98.4% drawdown, $42.6M estimated loss from peak TVL). The remaining 31 vaults
        emerged without share-price impact, largely due to proactive curator exits before the depeg.
        """)

    with st.expander("**Q2:** How could the Morpho protocol itself have been more resilient?"):
        st.markdown("""
        Four structural vulnerabilities: **(1) Oracle architecture** — Chainlink adapters lacked circuit-breakers
        or deviation thresholds, allowing stale $1.00 prices during 95% collapses. **(2) Timelock gaps** — 14 of 33
        vaults had instant (0-day) timelocks, enabling unchecked exposure changes. **(3) No automated exit
        triggers** — curator-dependent response meant speed varied from 63 days early (Gauntlet) to 88 days
        late (7 vaults still exposed). **(4) Contagion paths** — 4 vaults bridged toxic and clean markets,
        meaning depositors in "safe" markets unknowingly shared toxic exposure.
        """)
