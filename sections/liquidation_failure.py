"""Section 6: Liquidation Failure — Oracle masking prevented all liquidations."""

import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
from utils.data_loader import load_ltv, load_borrowers, load_asset_prices
from utils.charts import apply_layout, depeg_vline, RED, BLUE, ORANGE, GREEN, YELLOW, format_usd


def render():
    st.title("Liquidation Failure Analysis")
    st.caption(
        "Zero liquidations occurred despite 95–99% collateral value collapse. "
        "Hardcoded Chainlink oracles continued reporting ≈\\$1.00, completely masking the true risk."
    )

    ltv = load_ltv()
    borrowers = load_borrowers()
    prices = load_asset_prices()

    if ltv.empty and prices.empty:
        st.error("⚠️ Data not available — run the pipeline to generate `block5_ltv_analysis.csv` and `block5_asset_prices.csv`.")
        return

    # ── Key Metrics ─────────────────────────────────────────
    # Compute trapped borrow value from markets data
    from utils.data_loader import load_markets as _load_mkts, load_bad_debt_detail
    mkts = _load_mkts()
    trapped_borrow = 0
    if not mkts.empty and "borrow_usd" in mkts.columns and "utilization" in mkts.columns:
        trapped = mkts[mkts["utilization"] > 0.99]
        trapped_borrow = trapped["borrow_usd"].sum()

    # Get collateral spot prices from bad debt detail
    bd_detail = load_bad_debt_detail()
    min_spot = None
    max_spot = None
    if not bd_detail.empty and "collateral_spot_price" in bd_detail.columns:
        spots = bd_detail[bd_detail["L2_total_bad_debt_usd"] > 100]["collateral_spot_price"]
        spots = spots[spots > 0]
        if not spots.empty:
            min_spot = spots.min()
            max_spot = spots.max()

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Liquidation Events", "0", help=f"Zero across all {len(mkts)} markets")
    c2.metric("Oracle Price Reported", "≈$1.00", help="Hardcoded oracle still reports peg")
    if min_spot is not None:
        c3.metric("Actual Market Price", f"${min_spot:.3f}–{max_spot:.3f}",
                  help=f"{(1 - min_spot) * 100:.0f}–{(1 - max_spot) * 100:.0f}% below oracle price")
    else:
        c3.metric("Actual Market Price", "≈$0 (collapsed)",
                  help="Spot prices collapsed to near-zero")
    c4.metric("Trapped Borrow Value", format_usd(trapped_borrow),
              help="Debt at >99% utilization that should have been liquidated")

    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)

    # ── Oracle vs Reality ───────────────────────────────────
    st.subheader("Oracle Price vs Market Reality")

    if not prices.empty:
        # Show xUSD as the clearest example
        xusd = prices[prices["asset"] == "xUSD"].copy()
        if not xusd.empty:
            fig = go.Figure()
            
            # Actual market price
            fig.add_trace(go.Scatter(
                x=xusd["timestamp"], y=xusd["price_usd"],
                name="Market Price (DEX)",
                line=dict(color=RED, width=2),
                connectgaps=False,
            ))
            
            # Oracle price (hardcoded ~$1)
            fig.add_trace(go.Scatter(
                x=xusd["timestamp"], y=[1.0] * len(xusd),
                name="Oracle Price (Chainlink)",
                line=dict(color=GREEN, width=2, dash="dash"),
            ))

            # LLTV threshold
            fig.add_trace(go.Scatter(
                x=xusd["timestamp"], y=[0.86] * len(xusd),
                name="Liquidation Threshold (LLTV 86%)",
                line=dict(color=YELLOW, width=1, dash="dot"),
            ))

            fig = apply_layout(fig, title="xUSD: Oracle vs Market Price", height=420)
            fig = depeg_vline(fig)
            fig.update_yaxes(tickformat="$.2f", range=[0, 1.3])
            
            # Annotate the gap
            fig.add_annotation(
                x="2025-11-15", y=0.5,
                text="Oracle blind spot<br>Market ≈$0.05<br>Oracle ≈$1.00",
                showarrow=True, arrowhead=2, arrowcolor=RED,
                font=dict(color=RED, size=11),
                ax=60, ay=-40,
            )
            st.plotly_chart(fig, use_container_width=True)

    # ── LTV Analysis ────────────────────────────────────────
    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
    st.subheader("LTV Analysis — Why Liquidations Failed")

    if not ltv.empty:
        # Select and rename columns for clean display
        display_cols = []
        if "market" in ltv.columns:
            display_cols.append("market")
        for c in ["oracle_mechanism", "collateral_spot_price", "lltv_pct",
                   "oracle_ltv_pct", "true_ltv_pct", "price_gap_pct",
                   "borrow_usd", "liquidation_status", "liquidations_count"]:
            if c in ltv.columns:
                display_cols.append(c)

        ltv_display = ltv[display_cols].copy()

        # Cap 9999 to readable text
        for col in ["oracle_ltv_pct", "true_ltv_pct"]:
            if col in ltv_display.columns:
                ltv_display[col] = ltv_display[col].apply(
                    lambda x: x if x < 9000 else None
                )

        st.dataframe(
            ltv_display,
            column_config={
                "market": "Market",
                "oracle_mechanism": "Oracle Type",
                "collateral_spot_price": st.column_config.NumberColumn("Spot Price", format="$%.4f"),
                "lltv_pct": st.column_config.NumberColumn("LLTV", format="%.1f%%"),
                "oracle_ltv_pct": st.column_config.NumberColumn("Oracle LTV", format="%.1f%%"),
                "true_ltv_pct": st.column_config.NumberColumn("True LTV", format="%.1f%%"),
                "price_gap_pct": st.column_config.NumberColumn("Price Gap", format="%.1f%%"),
                "borrow_usd": st.column_config.NumberColumn("Borrow Value", format="$%,.0f"),
                "liquidation_status": "Status",
                "liquidations_count": st.column_config.NumberColumn("Liquidations", format="%d"),
            },
            hide_index=True,
            use_container_width=True,
        )

        st.info(
            "**Why Oracle LTV shows as empty for some markets:** "
            "LTV = Borrow Value / Collateral Value. When the oracle still reports collateral near \\$1 "
            "but real market price is ~\\$0, the oracle-computed LTV stays extremely low — the protocol "
            "thinks positions are healthy. Meanwhile, using real spot prices, these positions are deeply "
            "underwater. This gap between oracle and reality is exactly why no liquidations fired."
        )

    # ── Mechanism Explanation ────────────────────────────────
    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
    st.subheader("The Oracle Masking Mechanism")

    col1, col2, col3 = st.columns(3)

    with col1:
        with st.container(border=True):
            st.markdown("**1. Normal Operation**")
            st.markdown(
                "Oracle price = \\$1.00\n\n"
                "Market price = \\$1.00\n\n"
                "LTV < LLTV → positions healthy"
            )
            st.caption("Both prices agree. No liquidations needed.")

    with col2:
        with st.container(border=True):
            st.markdown("**2. Depeg Occurs**")
            st.markdown(
                "Oracle price = \\$1.00 (unchanged)\n\n"
                "Market price → \\$0.05\n\n"
                "Oracle LTV still < LLTV"
            )
            st.caption("Oracle doesn't see the crash. Protocol thinks everything is fine.")

    with col3:
        with st.container(border=True):
            st.markdown("**3. Consequence**")
            st.markdown(
                "Zero liquidations fire\n\n"
                "Bad debt accumulates silently\n\n"
                "Vault depositors absorb all losses"
            )
            st.caption("The liquidation safety net was completely bypassed by stale oracle data.")

    # ── Borrower Concentration ──────────────────────────────
    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
    st.subheader("Borrower Concentration")

    if not borrowers.empty:
        st.dataframe(
            borrowers,
            column_config={
                "market": "Market",
                "num_borrowers": st.column_config.NumberColumn("Borrowers", format="%d"),
                "total_borrow_usd": st.column_config.NumberColumn("Total Borrow", format="$%,.0f"),
                "top_borrower_pct": st.column_config.NumberColumn("Top Borrower %", format="%.1f%%"),
                "concentration": "Concentration",
            },
            hide_index=True,
            use_container_width=True,
        )

        st.caption(
            "High concentration means a single borrower controls most of the debt in several markets. "
            "In the largest market (\\$49.8M sdeUSD/USDC), a single address holds 100% of borrows."
        )
