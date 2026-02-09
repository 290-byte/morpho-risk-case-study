"""Section 3: Bad Debt Analysis — $3.64M quantification and share price impacts."""

import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
from utils.data_loader import load_markets, load_vaults, load_share_prices
from utils.charts import apply_layout, depeg_vline, RED, GREEN, BLUE, YELLOW, ORANGE, format_usd


def render():
    st.title("💀 Bad Debt Analysis")
    st.caption("$3.64M in unrealized bad debt across 4 markets — plus share price damage to 2 vaults.")

    markets = load_markets()
    vaults = load_vaults()
    prices = load_share_prices()

    # ── Key Metrics ─────────────────────────────────────────
    total_bad_debt = markets["bad_debt_usd"].sum()
    markets_with_debt = len(markets[markets["bad_debt_usd"] > 0])
    
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Bad Debt", f"${total_bad_debt:,.0f}", delta=f"-${total_bad_debt:,.0f}", delta_color="inverse")
    c2.metric("Markets with Bad Debt", f"{markets_with_debt} / {len(markets)}")
    c3.metric("Largest Single Market", "$3.64M", help="xUSD/USDC on Arbitrum")
    c4.metric("Realized Bad Debt", "$8,947", help="Only a tiny fraction has been formally realized")

    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)

    # ── Bad Debt Waterfall ──────────────────────────────────
    st.subheader("Bad Debt by Market")

    bad_markets = markets[markets["bad_debt_usd"] > 0].sort_values("bad_debt_usd", ascending=False)
    if not bad_markets.empty:
        fig = go.Figure(go.Waterfall(
            name="Bad Debt",
            orientation="v",
            x=bad_markets["market_label"].tolist() + ["Total"],
            y=bad_markets["bad_debt_usd"].tolist() + [0],
            measure=["relative"] * len(bad_markets) + ["total"],
            text=[format_usd(v) for v in bad_markets["bad_debt_usd"]] + [format_usd(total_bad_debt)],
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

    tab1, tab2 = st.tabs(["🔴 Damaged Vaults", "✅ Stable Vaults"])

    with tab1:
        st.markdown("""
        Two vaults suffered share price drops, meaning depositors bore losses from bad debt socialization.
        """)

        damaged = vaults[vaults["share_price_drawdown"] < -0.01]
        if not damaged.empty:
            for _, v in damaged.iterrows():
                with st.container(border=True):
                    col1, col2, col3, col4 = st.columns(4)
                    col1.metric("Vault", v["vault_name"])
                    col2.metric("Curator", v["curator"])
                    col3.metric("Drawdown", f"{v['share_price_drawdown']:.1%}", delta=f"{v['share_price_drawdown']:.1%}", delta_color="inverse")
                    col4.metric("Current Price", f"{v['share_price']:.6f}")

        # Chart: share price of damaged vaults
        if not prices.empty:
            damaged_names = ["MEV Capital USDC (ETH)", "Relend USDC"]
            mask = prices["vault_name"].isin(damaged_names)
            if mask.any():
                fig = px.line(
                    prices[mask], x="date", y="share_price", color="vault_name",
                    color_discrete_map={"MEV Capital USDC (ETH)": ORANGE, "Relend USDC": RED},
                )
                fig = apply_layout(fig, title="Share Price — Damaged Vaults", height=400)
                fig = depeg_vline(fig)
                fig.update_yaxes(tickformat="$.4f")
                st.plotly_chart(fig, use_container_width=True)

    with tab2:
        st.markdown("These vaults maintained stable share prices — proactive curator exits protected depositors.")

        stable = vaults[vaults["share_price_drawdown"] >= -0.01].sort_values("tvl_usd", ascending=False)
        if not stable.empty:
            st.dataframe(
                stable[["vault_name", "curator", "tvl_usd", "share_price", "response_class"]],
                column_config={
                    "vault_name": "Vault",
                    "curator": "Curator",
                    "tvl_usd": st.column_config.NumberColumn("TVL", format="$%,.0f"),
                    "share_price": st.column_config.NumberColumn("Share Price", format="%.6f"),
                    "response_class": "Response",
                },
                hide_index=True,
                use_container_width=True,
            )

        # Chart: stable vaults share price
        if not prices.empty:
            stable_names = ["Gauntlet USDC Frontier", "Gauntlet USDC Core",
                           "Smokehouse USDC", "Hyperithm USDC Degen"]
            mask = prices["vault_name"].isin(stable_names)
            if mask.any():
                fig = px.line(prices[mask], x="date", y="share_price", color="vault_name")
                fig = apply_layout(fig, title="Share Price — Protected Vaults", height=350)
                fig = depeg_vline(fig)
                fig.update_yaxes(tickformat="$.4f")
                st.plotly_chart(fig, use_container_width=True)

    # ── Three-Layer Analysis ────────────────────────────────
    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
    st.subheader("Three-Layer Bad Debt Analysis")

    col1, col2, col3 = st.columns(3)

    with col1:
        with st.container(border=True):
            st.markdown("**Layer 1: Supply-Borrow Gap**")
            st.metric("Markets with gap < 0", "0")
            st.caption("Oracle-independent check. No markets have borrowing exceeding supply at face value.")

    with col2:
        with st.container(border=True):
            st.markdown("**Layer 2: Protocol Bad Debt**")
            st.metric("Total Unrealized", f"${total_bad_debt:,.0f}")
            st.caption("Morpho's internal accounting shows bad debt that hasn't been formally realized.")

    with col3:
        with st.container(border=True):
            st.markdown("**Layer 3: Oracle vs Spot**")
            st.metric("Markets Mispriced (>5%)", "0 detected")
            st.caption("Paradoxically zero — because oracles are hardcoded at ≈$1.00, masking the real gap.")
