"""Section 2: Market Exposure: Toxic markets across multiple chains."""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from utils.data_loader import load_markets, load_vaults
from utils.charts import apply_layout, donut_chart, RED, GREEN, YELLOW, ORANGE, BLUE, format_usd


def render():
    st.title("Market Exposure")

    markets = load_markets()
    vaults = load_vaults()

    if markets.empty:
        st.error("⚠️ Market data not available. Run the pipeline to generate `block1_markets_graphql.csv`.")
        return

    # Dynamic caption from data
    n_markets = len(markets)
    n_chains = markets["chain"].nunique() if "chain" in markets.columns else 0
    collateral_types = ", ".join(sorted(markets["collateral"].dropna().unique())) if "collateral" in markets.columns else "xUSD, deUSD, sdeUSD"
    st.caption(
        f"{n_markets} markets using {collateral_types} as collateral, "
        f"identified across {n_chains} chains via the Morpho GraphQL API."
    )

    # Separate public and private markets
    public_markets = markets[~markets.get("is_private_market", False)] if "is_private_market" in markets.columns else markets
    private_markets = markets[markets.get("is_private_market", False)] if "is_private_market" in markets.columns else pd.DataFrame()

    # ── Key Metrics ─────────────────────────────────────────
    at_risk = public_markets[public_markets["status"].str.contains("AT_RISK")]

    # Compute collateral exposure from data
    collat_col = "total_collat_usd" if "total_collat_usd" in public_markets.columns else None
    if collat_col:
        total_collat = public_markets[collat_col].sum()
    else:
        total_collat = 0

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Public Markets", len(public_markets))
    c2.metric("At Risk (100% Util)", len(at_risk))
    c3.metric("Total Collateral (spot)", format_usd(total_collat),
              help="Collateral valued at current spot prices (near \\$0 for collapsed assets)")
    # Use depeg-time supply for locked markets
    _locked_pub = public_markets[public_markets["utilization"] > 0.99]
    _has_depeg = "supply_at_depeg" in _locked_pub.columns and _locked_pub["supply_at_depeg"].sum() > 0
    _locked_val = _locked_pub["supply_at_depeg"].sum() if _has_depeg else _locked_pub["supply_usd"].sum()
    c4.metric("Supply at Depeg (locked)", format_usd(_locked_val),
              help="Capital in 100% utilization markets at the time of the depeg (Nov 4)")

    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)

    # ── Private Market: On-Chain Confirmation ────────────────
    if not private_markets.empty:
        _pm = private_markets.iloc[0]
        _pm_coll = float(_pm.get("original_capital_lost", 0) or 0)
        _pm_supply = float(_pm.get("supply_usd", 0) or 0)
        _pm_bd = float(_pm.get("bad_debt_usd", 0) or 0)
        st.error(
            f"**Private market confirmed on-chain.** "
            f"An unlisted xUSD/USDC market (86% LLTV) on Plume holds "
            f"65.8M xUSD as collateral, matching the ~68M USD Elixir-to-Stream "
            f"exposure from lawsuit filings. At the time of the depeg, roughly "
            f"**~{format_usd(_pm_coll)}** in xUSD collateral was lost. "
            f"Interest has since inflated the nominal supply to "
            f"{format_usd(_pm_supply)} and bad debt to {format_usd(_pm_bd)}, "
            f"but these figures are unrecoverable and the relevant loss "
            f"is the original ~68M USD in capital. "
            f"*(Source: Morpho GraphQL API raw underlying; Dune Analytics; "
            f"Stream Trading Corp. v. McMeans, Case No. 3:25-cv-10524)*".replace("$", "\\$")
        )
    else:
        st.info(
            "**Additional context: private market exposure.** "
            f"In addition to the {n_markets} public markets shown below, Elixir Network lent "
            "approximately **~68M USDC** (65% of deUSD's total backing) to Stream Finance through "
            "**private, non-whitelisted Morpho markets** where Stream was the sole borrower, using "
            "xUSD as collateral. These private markets are not captured by the public API and "
            "represent the largest single Morpho-related exposure to this event. "
            "*(Source: Stream Trading Corp. v. McMeans, Case No. 3:25-cv-10524)*"
        )

    # ── Market Table ────────────────────────────────────────
    st.subheader(f"Public Markets ({len(public_markets)})")

    display_cols = ["market_label", "chain", "collateral", "loan", "lltv", "supply_usd",
                    "borrow_usd", "utilization", "bad_debt_usd", "status"]

    display_df = public_markets[display_cols].sort_values("bad_debt_usd", ascending=False).copy()
    # Pre-format for clean display in both app and export
    display_df["lltv"] = (display_df["lltv"] * 100).round(1)
    display_df["utilization"] = (display_df["utilization"] * 100).round(1)

    st.dataframe(
        display_df,
        column_config={
            "market_label": "Market",
            "chain": "Chain",
            "collateral": "Collateral",
            "loan": "Loan Asset",
            "lltv": st.column_config.NumberColumn("LLTV %", format="%.1f%%"),
            "supply_usd": st.column_config.NumberColumn("Supply", format="$%,.0f"),
            "borrow_usd": st.column_config.NumberColumn("Borrow", format="$%,.0f"),
            "utilization": st.column_config.NumberColumn("Utilization %", format="%.1f%%"),
            "bad_debt_usd": st.column_config.NumberColumn("Bad Debt", format="$%,.0f"),
            "status": "Status",
        },
        hide_index=True,
        use_container_width=True,
        height=500,
    )

    # Private market detail table
    if not private_markets.empty:
        st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
        st.subheader("Private Market (Plume)")
        st.caption(
            "Unlisted, non-whitelisted. Nominal values are interest-inflated; "
            "original capital lost was ~68M USD (65.8M xUSD collateral at pre-depeg price)."
        )
        _pm_cols = ["market_label", "chain", "collateral", "loan", "lltv",
                    "supply_usd", "borrow_usd", "bad_debt_usd"]
        if "original_capital_lost" in private_markets.columns:
            _pm_cols.append("original_capital_lost")
        _pm_display = private_markets[_pm_cols].copy()
        _pm_display["lltv"] = (_pm_display["lltv"] * 100).round(1)
        if "original_capital_lost" in _pm_display.columns:
            _pm_display = _pm_display.rename(columns={"original_capital_lost": "capital_lost_at_depeg"})
        st.dataframe(
            _pm_display,
            column_config={
                "market_label": "Market",
                "chain": "Chain",
                "collateral": "Collateral",
                "loan": "Loan Asset",
                "lltv": st.column_config.NumberColumn("LLTV %", format="%.1f%%"),
                "supply_usd": st.column_config.NumberColumn("Supply (nominal)", format="$%,.0f"),
                "borrow_usd": st.column_config.NumberColumn("Borrow (nominal)", format="$%,.0f"),
                "bad_debt_usd": st.column_config.NumberColumn("Bad Debt (nominal)", format="$%,.0f"),
                "capital_lost_at_depeg": st.column_config.NumberColumn("Capital Lost at Depeg", format="$%,.0f"),
            },
            hide_index=True,
            use_container_width=True,
        )

    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)

    # ── Charts Row ──────────────────────────────────────────
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**Markets by Chain**")
        chain_counts = public_markets.groupby("chain").size().reset_index(name="count")
        fig = donut_chart(
            chain_counts["count"].tolist(),
            chain_counts["chain"].tolist(),
            colors=[BLUE, ORANGE, GREEN],
            height=300,
        )
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.markdown("**Markets by Collateral**")
        coll_counts = public_markets.groupby("collateral").size().reset_index(name="count")
        fig = donut_chart(
            coll_counts["count"].tolist(),
            coll_counts["collateral"].tolist(),
            colors=[RED, YELLOW, ORANGE],
            height=300,
        )
        st.plotly_chart(fig, use_container_width=True)

    # ── Bad Debt Distribution ───────────────────────────────
    st.subheader("Bad Debt Distribution")

    bad_debt_markets = public_markets[public_markets["bad_debt_usd"] > 0].sort_values("bad_debt_usd", ascending=True)
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
        display_cols = ["vault_name", "chain", "curator", "tvl_usd", "exposure_usd",
                        "collateral", "status", "response_class"]
        col_config = {
            "vault_name": "Vault",
            "chain": "Chain",
            "curator": "Curator",
            "tvl_usd": st.column_config.NumberColumn("Current TVL", format="$%,.0f"),
            "exposure_usd": st.column_config.NumberColumn("Exposure", format="$%,.0f"),
            "collateral": "Collateral",
            "status": "Status",
            "response_class": "Response",
        }

        # Filter to available columns only
        display_cols = [c for c in display_cols if c in vaults.columns]
        vault_display = vaults[display_cols].sort_values("tvl_usd", ascending=False)

        st.dataframe(
            vault_display,
            column_config=col_config,
            hide_index=True,
            use_container_width=True,
        )
