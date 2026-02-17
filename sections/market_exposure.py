"""Section 2: Market Exposure -- Toxic market supply, vault allocations, and cross-reference."""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from utils.data_loader import (
    load_market_supply_at_depeg,
    load_vault_toxic_exposure_at_depeg,
    load_vault_tvl_at_depeg,
    load_market_history_combined,
    load_vault_allocation_hourly,
    TOXIC_MARKETS,
    DEPEG_TS,
)
from utils.charts import apply_layout, RED, GREEN, YELLOW, ORANGE, BLUE, PURPLE, format_usd


def _esc(text: str) -> str:
    """Escape dollar signs for Streamlit markdown (avoids LaTeX rendering)."""
    return text.replace("$", "\\$")


def render():
    st.title("Market Exposure")

    # -- Load all data from block8 CSVs ---
    mkt_depeg = load_market_supply_at_depeg()
    vault_exposure = load_vault_toxic_exposure_at_depeg()
    vault_tvl = load_vault_tvl_at_depeg()

    if mkt_depeg.empty:
        st.error(
            "Block8 market history data not available. "
            "Run `python queries/block8_query_plume_deep_dive.py` to generate the CSVs."
        )
        return

    # -- Top Metrics ---
    total_supply = mkt_depeg["supply_usd"].sum()
    total_borrow = mkt_depeg["borrow_usd"].sum()
    n_markets = len(mkt_depeg)
    n_chains = mkt_depeg["chain"].nunique()

    # Vault exposure totals
    vault_total = vault_exposure["supply_usd_at_depeg"].sum() if not vault_exposure.empty else 0
    direct_supply = total_supply - vault_total
    n_vaults_exposed = vault_exposure["vault_address"].nunique() if not vault_exposure.empty else 0

    st.caption(_esc(
        f"Verified hourly data from Morpho Blue GraphQL API. "
        f"All figures are snapshots at Nov 3 00:00 UTC (depeg onset). "
        f"{format_usd(total_supply)} total supply across {n_markets} toxic markets on {n_chains} chains."
    ))

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Toxic Supply", _esc(format_usd(total_supply)),
              help="Sum of supply across all 5 toxic markets at depeg")
    c2.metric("Total Borrow", _esc(format_usd(total_borrow)))
    c3.metric("Via Vaults", _esc(format_usd(vault_total)),
              help="Supply routed through curator vaults")
    c4.metric("Direct Supply", _esc(format_usd(direct_supply)),
              help="Supply deposited directly to markets (not through vaults)")

    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)

    # =========================================================
    #  VIEW 1: Market Supply at Depeg
    # =========================================================
    st.subheader("View 1: Market Supply at Depeg")
    st.caption("How much capital was in each toxic market when the depeg hit.")

    # Sort by supply descending
    mkt_chart = mkt_depeg.sort_values("supply_usd", ascending=True).copy()
    mkt_chart["supply_label"] = mkt_chart["supply_usd"].apply(format_usd)
    mkt_chart["util_label"] = (mkt_chart["utilization"] * 100).round(1).astype(str) + "%"

    chain_colors = {"Ethereum": BLUE, "Arbitrum": ORANGE, "Plume": GREEN}
    mkt_chart["chain"] = mkt_chart["chain"].str.title()

    fig = px.bar(
        mkt_chart,
        y="market_label",
        x="supply_usd",
        color="chain",
        orientation="h",
        text="supply_label",
        color_discrete_map=chain_colors,
    )
    fig = apply_layout(fig, height=300)
    fig.update_traces(textposition="outside", textfont_size=11)
    fig.update_xaxes(title="Supply (USD)", tickformat="$,.0f")
    fig.update_yaxes(title="")
    st.plotly_chart(fig, use_container_width=True)

    # Detail table
    with st.expander("Market detail table"):
        detail = mkt_depeg[["market_label", "chain", "supply_usd", "borrow_usd", "utilization"]].sort_values(
            "supply_usd", ascending=False
        ).copy()
        # Pre-format for reliable display
        detail["Supply (USD)"] = detail["supply_usd"].map(format_usd)
        detail["Borrow (USD)"] = detail["borrow_usd"].map(format_usd)
        detail["Utilization"] = (detail["utilization"] * 100).round(1).astype(str) + "%"
        detail["chain"] = detail["chain"].str.title()
        st.dataframe(
            detail[["market_label", "chain", "Supply (USD)", "Borrow (USD)", "Utilization"]],
            column_config={
                "market_label": "Market",
                "chain": "Chain",
            },
            hide_index=True,
            use_container_width=True,
        )

    # Context callout for Elixir-Stream
    es_row = mkt_depeg[mkt_depeg["market_label"].str.contains("ES", na=False)]
    if not es_row.empty:
        es_supply = es_row.iloc[0]["supply_usd"]
        st.info(_esc(
            f"The xUSD/USDC Plume (Elixir-Stream) market held {format_usd(es_supply)} "
            f"at depeg, representing {es_supply / total_supply * 100:.0f}% of total toxic supply. "
            f"This was a private, non-whitelisted market with direct supply by Elixir "
            f"and a single borrower (Stream Finance). No curator vaults were exposed."
        ))

    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)

    # =========================================================
    #  VIEW 2: Vault Exposure to Toxic Markets
    # =========================================================
    st.subheader("View 2: Vault Allocation to Toxic Markets at Depeg")
    st.caption("How much each vault had allocated to toxic collateral markets at the moment of depeg.")

    if vault_exposure.empty:
        st.warning("Vault allocation data not available.")
    else:
        # Aggregate per vault (sum across toxic markets)
        vault_agg = vault_exposure.groupby(
            ["vault_address", "vault_name"], as_index=False
        ).agg(
            total_toxic_usd=("supply_usd_at_depeg", "sum"),
            peak_toxic_usd=("peak_supply_usd", "sum"),
            n_toxic_markets=("market_id", "nunique"),
        )

        # Merge vault TVL
        if not vault_tvl.empty:
            vault_agg = vault_agg.merge(
                vault_tvl[["vault_address", "tvl_at_depeg"]],
                on="vault_address",
                how="left",
            )
            vault_agg["tvl_at_depeg"] = vault_agg["tvl_at_depeg"].fillna(0)
            vault_agg["toxic_pct"] = vault_agg.apply(
                lambda r: (r["total_toxic_usd"] / r["tvl_at_depeg"] * 100)
                if r["tvl_at_depeg"] > 0 else 0,
                axis=1,
            )
        else:
            vault_agg["tvl_at_depeg"] = 0
            vault_agg["toxic_pct"] = 0

        # Filter to vaults with material exposure at depeg
        active = vault_agg[vault_agg["total_toxic_usd"] > 100].sort_values(
            "total_toxic_usd", ascending=True
        ).copy()

        if active.empty:
            st.info("No vaults had material toxic exposure at depeg.")
        else:
            active["label"] = active["total_toxic_usd"].apply(format_usd)

            fig = px.bar(
                active,
                y="vault_name",
                x="total_toxic_usd",
                orientation="h",
                text="label",
                color_discrete_sequence=[RED],
            )
            fig = apply_layout(fig, height=max(250, len(active) * 35))
            fig.update_traces(textposition="outside", textfont_size=10)
            fig.update_xaxes(title="Toxic Market Allocation (USD)", tickformat="$,.0f")
            fig.update_yaxes(title="")
            st.plotly_chart(fig, use_container_width=True)

            # Detail table
            display = active.sort_values("total_toxic_usd", ascending=False).copy()
            display["Toxic at Depeg"] = display["total_toxic_usd"].map(format_usd)
            display["Vault TVL at Depeg"] = display["tvl_at_depeg"].map(format_usd)
            display["Toxic % of TVL"] = display["toxic_pct"].round(1).astype(str) + "%"
            display["Peak Toxic"] = display["peak_toxic_usd"].map(format_usd)
            st.dataframe(
                display[["vault_name", "Toxic at Depeg", "Vault TVL at Depeg",
                          "Toxic % of TVL", "Peak Toxic", "n_toxic_markets"]],
                column_config={
                    "vault_name": "Vault",
                    "n_toxic_markets": st.column_config.NumberColumn(
                        "Toxic Markets", format="%d"
                    ),
                },
                hide_index=True,
                use_container_width=True,
            )

    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)

    # =========================================================
    #  VIEW 3: Market-to-Vault Breakdown (who supplied each market)
    # =========================================================
    st.subheader("View 3: Who Supplied Each Toxic Market")
    st.caption(
        "For each toxic market, which vaults (and direct suppliers) provided the capital. "
        "This is the key chart for understanding loss attribution."
    )

    if vault_exposure.empty or mkt_depeg.empty:
        st.warning("Data not available for market-vault cross-reference.")
    else:
        # For each market: vault breakdown + direct supply remainder
        # Exclude Elixir-Stream (no vault exposure, all direct)
        public_markets = mkt_depeg[
            ~mkt_depeg["market_label"].str.contains("ES", na=False)
        ].sort_values("supply_usd", ascending=False)

        for _, mkt_row in public_markets.iterrows():
            mkt_id = mkt_row["market_id"]
            mkt_label = mkt_row["market_label"]
            mkt_supply = mkt_row["supply_usd"]

            # Get vault allocations to this market
            mkt_vaults = vault_exposure[
                (vault_exposure["market_id"] == mkt_id)
                & (vault_exposure["supply_usd_at_depeg"] > 100)
            ].sort_values("supply_usd_at_depeg", ascending=False).copy()

            vault_sum = mkt_vaults["supply_usd_at_depeg"].sum() if not mkt_vaults.empty else 0
            direct = max(0, mkt_supply - vault_sum)

            st.markdown(f"**{mkt_label}** - total supply {_esc(format_usd(mkt_supply))}")

            # Build stacked bar data
            bar_data = []
            for _, vrow in mkt_vaults.iterrows():
                bar_data.append({
                    "source": vrow["vault_name"],
                    "amount": vrow["supply_usd_at_depeg"],
                    "type": "Vault",
                })
            if direct > 100:
                bar_data.append({
                    "source": "Direct suppliers",
                    "amount": direct,
                    "type": "Direct",
                })

            if bar_data:
                bdf = pd.DataFrame(bar_data).sort_values("amount", ascending=True)
                bdf["label"] = bdf["amount"].apply(format_usd)
                bdf["pct"] = (bdf["amount"] / mkt_supply * 100).round(1)
                bdf["display"] = bdf.apply(
                    lambda r: f"{r['label']} ({r['pct']:.0f}%)", axis=1
                )

                colors = []
                for _, r in bdf.iterrows():
                    if r["type"] == "Direct":
                        colors.append("#94A3B8")
                    else:
                        colors.append(RED)

                fig = go.Figure(go.Bar(
                    y=bdf["source"],
                    x=bdf["amount"],
                    orientation="h",
                    text=bdf["display"],
                    textposition="outside",
                    marker_color=colors,
                ))
                fig = apply_layout(fig, height=max(180, len(bdf) * 40), show_legend=False)
                fig.update_xaxes(title="", tickformat="$,.0f")
                fig.update_yaxes(title="")
                st.plotly_chart(fig, use_container_width=True)

            st.markdown("")

        # Elixir-Stream callout
        es_markets = mkt_depeg[mkt_depeg["market_label"].str.contains("ES", na=False)]
        if not es_markets.empty:
            es_val = es_markets.iloc[0]["supply_usd"]
            st.markdown(
                f"**xUSD/USDC Plume (Elixir-Stream)** - "
                + _esc(f"{format_usd(es_val)} supplied directly by Elixir Network. ")
                + "No curator vaults involved. Single borrower (Stream Finance)."
            )

    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)

    # =========================================================
    #  BONUS: Allocation Timeline for sdeUSD/USDC Ethereum
    # =========================================================
    st.subheader("Allocation Timeline: sdeUSD/USDC Ethereum")
    st.caption(
        "The largest public market by vault exposure. "
        "Shows how each vault's allocation evolved in the weeks around the depeg."
    )

    alloc = load_vault_allocation_hourly()
    if alloc.empty:
        st.warning("Vault allocation hourly data not available.")
    else:
        eth_mkt_id = "0x0f9563442d64ab3bd3bcb27058db0b0d4046a4c46f0acd811dacae9551d2b129"
        eth_alloc = alloc[
            (alloc["market_id"] == eth_mkt_id)
            & (alloc["supply_assets_usd"] > 100)
        ].copy()

        if eth_alloc.empty:
            st.info("No allocation data for sdeUSD/USDC Ethereum.")
        else:
            eth_alloc["dt"] = pd.to_datetime(eth_alloc["datetime"])
            # Filter to Oct 15 - Nov 15 for clarity
            mask = (eth_alloc["dt"] >= "2025-10-15") & (eth_alloc["dt"] <= "2025-11-15")
            eth_alloc = eth_alloc[mask]

            if not eth_alloc.empty:
                fig = px.area(
                    eth_alloc,
                    x="dt",
                    y="supply_assets_usd",
                    color="vault_name",
                    color_discrete_sequence=px.colors.qualitative.Set2,
                )
                fig = apply_layout(fig, height=450)
                fig.update_yaxes(title="Allocation (USD)", tickformat="$,.0f")
                fig.update_xaxes(title="")

                # Add depeg line
                fig.add_vline(
                    x=pd.Timestamp("2025-11-03"),
                    line_dash="dash", line_color=RED, opacity=0.7,
                )
                fig.add_annotation(
                    x=pd.Timestamp("2025-11-03"), y=1, yref="paper",
                    text="Depeg (Nov 3)", showarrow=False,
                    font=dict(size=10, color=RED), yshift=14,
                )

                st.plotly_chart(fig, use_container_width=True)

    # -- Key Finding ---
    if not vault_exposure.empty and not mkt_depeg.empty:
        # Compute from data
        eth_id = "0x0f9563442d64ab3bd3bcb27058db0b0d4046a4c46f0acd811dacae9551d2b129"
        eth_vaults = vault_exposure[
            (vault_exposure["market_id"] == eth_id)
            & (vault_exposure["supply_usd_at_depeg"] > 100)
        ]
        eth_vault_total = eth_vaults["supply_usd_at_depeg"].sum()
        all_vault_total = vault_exposure["supply_usd_at_depeg"].sum()
        pct = (eth_vault_total / all_vault_total * 100) if all_vault_total > 0 else 0

        st.info(_esc(
            f"Key finding: {format_usd(eth_vault_total)} of {format_usd(all_vault_total)} "
            f"total vault exposure ({pct:.0f}%) was concentrated in sdeUSD/USDC Ethereum. "
            f"This single market is where the vast majority of vault depositor losses originated."
        ))
