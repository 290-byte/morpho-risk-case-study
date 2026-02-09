"""Section 2: Market Exposure — 18 toxic markets across 3 chains."""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from utils.data_loader import load_markets, load_vaults
from utils.charts import apply_layout, donut_chart, RED, GREEN, YELLOW, ORANGE, BLUE, format_usd


def render():
    st.title("🎯 Market Exposure")
    st.caption("18 markets using xUSD, deUSD, or sdeUSD as collateral — discovered across 3,133 total Morpho markets.")

    markets = load_markets()
    vaults = load_vaults()

    if markets.empty:
        st.warning("No market data available. Run `python generate_data.py` first.")
        return

    # ── Key Metrics ─────────────────────────────────────────
    at_risk = markets[markets["status"].str.contains("AT_RISK")]
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Markets", len(markets))
    c2.metric("At Risk (100% Util)", len(at_risk))
    c3.metric("Total Collateral Exposure", "$13,004")
    c4.metric("Total Supply Locked", f"${markets['supply_usd'].sum():,.0f}")

    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)

    # ── Market Table ────────────────────────────────────────
    st.subheader("All 18 Toxic Markets")

    display_cols = ["market_label", "chain", "collateral", "loan", "lltv", "supply_usd",
                    "borrow_usd", "utilization", "bad_debt_usd", "status"]

    st.dataframe(
        markets[display_cols].sort_values("bad_debt_usd", ascending=False),
        column_config={
            "market_label": "Market",
            "chain": "Chain",
            "collateral": "Collateral",
            "loan": "Loan Asset",
            "lltv": st.column_config.NumberColumn("LLTV", format="%.1f%%"),
            "supply_usd": st.column_config.NumberColumn("Supply", format="$%,.0f"),
            "borrow_usd": st.column_config.NumberColumn("Borrow", format="$%,.0f"),
            "utilization": st.column_config.NumberColumn("Utilization", format="%.1f%%"),
            "bad_debt_usd": st.column_config.NumberColumn("Bad Debt", format="$%,.0f"),
            "status": "Status",
        },
        hide_index=True,
        use_container_width=True,
        height=500,
    )

    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)

    # ── Charts Row ──────────────────────────────────────────
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**Markets by Chain**")
        chain_counts = markets.groupby("chain").size().reset_index(name="count")
        fig = donut_chart(
            chain_counts["count"].tolist(),
            chain_counts["chain"].tolist(),
            colors=[BLUE, ORANGE, GREEN],
            height=300,
        )
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.markdown("**Markets by Collateral**")
        coll_counts = markets.groupby("collateral").size().reset_index(name="count")
        fig = donut_chart(
            coll_counts["count"].tolist(),
            coll_counts["collateral"].tolist(),
            colors=[RED, YELLOW, ORANGE],
            height=300,
        )
        st.plotly_chart(fig, use_container_width=True)

    # ── Bad Debt Distribution ───────────────────────────────
    st.subheader("Bad Debt Distribution")

    bad_debt_markets = markets[markets["bad_debt_usd"] > 0].sort_values("bad_debt_usd", ascending=True)
    if not bad_debt_markets.empty:
        fig = px.bar(
            bad_debt_markets,
            y="market_label",
            x="bad_debt_usd",
            color="chain",
            orientation="h",
            text=bad_debt_markets["bad_debt_usd"].apply(format_usd),
            color_discrete_map={"ethereum": BLUE, "Arbitrum": ORANGE, "Plume": GREEN},
        )
        fig = apply_layout(fig, height=250)
        fig.update_traces(textposition="outside", textfont_size=10)
        fig.update_xaxes(title="Bad Debt (USD)", tickformat="$,.0f")
        fig.update_yaxes(title="")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No bad debt recorded in these markets.")

    # ── Vault Exposure ──────────────────────────────────────
    st.subheader("Vault Exposure Summary")

    if not vaults.empty:
        vault_display = vaults[["vault_name", "chain", "curator", "tvl_usd", "exposure_usd",
                                 "collateral", "status", "response_class"]].sort_values("tvl_usd", ascending=False)

        st.dataframe(
            vault_display,
            column_config={
                "vault_name": "Vault",
                "chain": "Chain",
                "curator": "Curator",
                "tvl_usd": st.column_config.NumberColumn("TVL", format="$%,.0f"),
                "exposure_usd": st.column_config.NumberColumn("Exposure", format="$%,.0f"),
                "collateral": "Collateral",
                "status": "Status",
                "response_class": "Response",
            },
            hide_index=True,
            use_container_width=True,
        )
