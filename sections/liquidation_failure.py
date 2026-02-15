"""Section 6: Liquidation Failure: Oracle masking prevented all liquidations."""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from utils.data_loader import load_ltv, load_borrowers, load_asset_prices
from utils.charts import apply_layout, depeg_vline, RED, BLUE, ORANGE, GREEN, YELLOW, format_usd


def render():
    st.title("Liquidation Failure Analysis")

    ltv = load_ltv()
    borrowers = load_borrowers()
    prices = load_asset_prices()

    if ltv.empty and prices.empty:
        st.error("⚠️ Data not available. Run the pipeline to generate `block5_ltv_analysis.csv` and `block5_asset_prices.csv`.")
        return

    # ── Key Metrics (computed from data) ─────────────────────
    from utils.data_loader import load_markets as _load_mkts, load_bad_debt_detail, load_csv
    mkts = _load_mkts()

    # Liquidation event count from data
    liq_events = load_csv("block5_liquidation_events.csv")
    if not liq_events.empty and "event_count" in liq_events.columns:
        n_liquidations = int(liq_events["event_count"].sum())
    elif not liq_events.empty:
        n_liquidations = len(liq_events)
    else:
        n_liquidations = 0

    # Compute totals from event-level data for the caption
    for col in ["seized_assets_usd", "repaid_assets_usd", "bad_debt_assets_usd"]:
        if col in liq_events.columns:
            liq_events[col] = pd.to_numeric(liq_events[col], errors="coerce").fillna(0)
    _total_repaid = liq_events["repaid_assets_usd"].sum() if "repaid_assets_usd" in liq_events.columns else 0
    _total_bd_created = liq_events["bad_debt_assets_usd"].sum() if "bad_debt_assets_usd" in liq_events.columns else 0

    # Trapped borrow value from PUBLIC markets at >99% utilization
    # Use depeg-time supply (not current interest-inflated values)
    trapped_borrow = 0
    trapped_borrow_now = 0
    if not mkts.empty and "borrow_usd" in mkts.columns and "utilization" in mkts.columns:
        _public = mkts[~mkts.get("is_private_market", False)] if "is_private_market" in mkts.columns else mkts
        trapped = _public[_public["utilization"] > 0.99]
        trapped_borrow_now = trapped["borrow_usd"].sum()
        # Use depeg-time supply as proxy for depeg-time borrows (same at 100% util)
        if "supply_at_depeg" in trapped.columns and trapped["supply_at_depeg"].sum() > 0:
            trapped_borrow = trapped["supply_at_depeg"].sum()
        else:
            trapped_borrow = trapped_borrow_now

    # Data-driven caption
    caption_parts = []
    if n_liquidations > 0:
        caption_parts.append(
            f"Only {n_liquidations} small liquidation events occurred"
        )
        if _total_repaid > 0 or _total_bd_created > 0:
            caption_parts[-1] += (
                f" ({format_usd(_total_repaid)} repaid, {format_usd(_total_bd_created)} realized bad debt)"
            )
        caption_parts.append(
            f". Negligible relative to {format_usd(trapped_borrow)} in trapped borrows."
        )
    else:
        caption_parts.append("Zero liquidations occurred despite 95–99% collateral value decline.")
    caption_parts.append(
        " Chainlink oracle adapters continued reporting approximately \\$1.00, "
        "preventing the liquidation engine from clearing underwater positions."
    )
    st.caption("".join(caption_parts).replace("$", r"\$"))

    # Oracle price from LTV data (what the oracle reports)
    oracle_price_str = "≈\\$1.00"
    if not ltv.empty and "oracle_price" in ltv.columns:
        oracle_prices = ltv["oracle_price"].dropna()
        if not oracle_prices.empty:
            op_min = oracle_prices.min()
            op_max = oracle_prices.max()
            if op_min == op_max:
                oracle_price_str = f"≈\\${op_min:.2f}"
            else:
                oracle_price_str = f"\\${op_min:.2f}–\\${op_max:.2f}"

    # ── Depeg-period prices ───────────────────────────────────
    # Current spot prices are near $0.001, meaningless for explaining the crisis.
    # During the depeg (Nov 4–6), xUSD traded $0.05–$0.30 on DEXs.
    # We show the crisis-window range to explain why liquidations SHOULD have fired.
    bd_detail = load_bad_debt_detail()

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Liquidation Events", str(n_liquidations), help=f"Across all {len(mkts)} affected markets")
    c2.metric("Oracle Price Reported", oracle_price_str, help="Oracle adapters continued reporting near-peg values")
    c3.metric("Actual Price (Nov 4-6)", "$0.05-$0.30",
              help="xUSD traded $0.05-$0.30 during the depeg crisis; now near $0.001")
    c4.metric("Trapped at Depeg (public)", format_usd(trapped_borrow),
              help="Supply in public 100% util markets at the time of the depeg. Interest has since inflated this to " + format_usd(trapped_borrow_now))

    # Private market note
    _private = mkts[mkts.get("is_private_market", False)] if "is_private_market" in mkts.columns else pd.DataFrame()
    if not _private.empty:
        _pm_capital = _private["original_capital_lost"].sum() if "original_capital_lost" in _private.columns else 0
        _pm_at_depeg = _pm_capital if _pm_capital > 0 else 68_000_000
        st.caption(
            f"Separately, the private Elixir-Stream market on Plume had ~{format_usd(_pm_at_depeg)} "
            f"in borrows at the time of the depeg, also unaffected by liquidation due to the same "
            f"oracle masking.".replace("$", r"\$")
        )

    # ── Liquidation Events Detail ────────────────────────────
    if not liq_events.empty:
        st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
        st.subheader("Liquidation Events Detail")

        # CSV has per-event rows: date, collateral_symbol, loan_symbol,
        # seized_assets_usd, repaid_assets_usd, bad_debt_assets_usd
        for col in ["seized_assets_usd", "repaid_assets_usd", "bad_debt_assets_usd"]:
            if col in liq_events.columns:
                liq_events[col] = pd.to_numeric(liq_events[col], errors="coerce").fillna(0)

        display_cols = [c for c in ["date", "collateral_symbol", "loan_symbol",
                                     "seized_assets_usd", "repaid_assets_usd",
                                     "bad_debt_assets_usd"] if c in liq_events.columns]
        if display_cols:
            st.dataframe(
                liq_events[display_cols],
                column_config={
                    "date": "Date",
                    "collateral_symbol": "Collateral",
                    "loan_symbol": "Loan",
                    "seized_assets_usd": st.column_config.NumberColumn("Seized (USD)", format="$%,.0f"),
                    "repaid_assets_usd": st.column_config.NumberColumn("Repaid (USD)", format="$%,.0f"),
                    "bad_debt_assets_usd": st.column_config.NumberColumn("Bad Debt Created", format="$%,.0f"),
                },
                hide_index=True, use_container_width=True,
            )
            total_seized = liq_events["seized_assets_usd"].sum() if "seized_assets_usd" in liq_events.columns else 0
            total_repaid = liq_events["repaid_assets_usd"].sum() if "repaid_assets_usd" in liq_events.columns else 0
            total_bd = liq_events["bad_debt_assets_usd"].sum() if "bad_debt_assets_usd" in liq_events.columns else 0
            st.caption(
                f"Total across {n_liquidations} events: {format_usd(total_seized)} seized, "
                f"{format_usd(total_repaid)} repaid, {format_usd(total_bd)} bad debt created. "
                f"Negligible. Most underwater positions were "
                f"never liquidated due to oracle masking.".replace("$", "\\$")
            )
        else:
            # Fallback: just show whatever columns exist
            st.dataframe(liq_events, hide_index=True, use_container_width=True)

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

            # LLTV threshold: pull from data if available
            lltv_threshold = 0.86  # fallback
            if not ltv.empty and "lltv_pct" in ltv.columns:
                max_lltv = ltv["lltv_pct"].max()
                if max_lltv > 0 and max_lltv < 100:
                    lltv_threshold = max_lltv / 100.0
            elif not mkts.empty and "lltv" in mkts.columns:
                max_lltv = mkts["lltv"].max()
                if max_lltv > 0:
                    lltv_threshold = max_lltv / 100.0 if max_lltv > 1 else max_lltv

            fig.add_trace(go.Scatter(
                x=xusd["timestamp"], y=[lltv_threshold] * len(xusd),
                name=f"Liquidation Threshold (LLTV {lltv_threshold:.0%})",
                line=dict(color=YELLOW, width=1, dash="dot"),
            ))

            fig = apply_layout(fig, title="xUSD: Oracle vs Market Price", height=420)
            fig = depeg_vline(fig)
            fig.update_yaxes(tickformat="$.2f", range=[0, 1.3])

            # Annotate the gap
            fig.add_annotation(
                x="2025-11-15", y=0.5,
                text="Oracle divergence<br>Market ≈$0.05<br>Oracle ≈$1.00",
                showarrow=True, arrowhead=2, arrowcolor=RED,
                font=dict(color=RED, size=11),
                ax=60, ay=-40,
            )
            st.plotly_chart(fig, use_container_width=True)

    # ── LTV Analysis ────────────────────────────────────────
    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
    st.subheader("LTV Analysis: Why Liquidations Failed")

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
            "but the actual market price is near \\$0, the oracle-computed LTV remains extremely low, "
            "the protocol considers positions healthy. Using real spot prices, these positions are deeply "
            "underwater. This divergence between oracle and market prices is the mechanism by which "
            "liquidations were prevented."
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
            st.caption("Oracle does not reflect the price decline. Protocol considers positions healthy.")

    with col3:
        with st.container(border=True):
            st.markdown("**3. Consequence**")
            st.markdown(
                f"Only {n_liquidations} minor liquidation{'s' if n_liquidations != 1 else ''}\n\n"
                "Bad debt accumulates unrealized\n\n"
                "Vault depositors absorb all losses"
            )
            st.caption("Stale oracle data meant the liquidation mechanism never engaged.")

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

        # Compute caption dynamically from borrowers data
        if "total_borrow_usd" in borrowers.columns and "top_borrower_pct" in borrowers.columns:
            largest_row = borrowers.loc[borrowers["total_borrow_usd"].idxmax()]
            largest_borrow = format_usd(largest_row["total_borrow_usd"])
            largest_market = largest_row.get("market", "largest market")
            top_pct = largest_row["top_borrower_pct"]
            st.caption(
                f"High concentration indicates a single borrower controls the majority of debt. "
                f"In the largest market ({largest_market}, {largest_borrow} total borrow), "
                f"the top borrower accounts for {top_pct:.0f}% of outstanding debt.".replace("$", "\\$")
            )
        else:
            st.caption(
                "High concentration indicates a single borrower controls the majority of debt in several markets."
            )
