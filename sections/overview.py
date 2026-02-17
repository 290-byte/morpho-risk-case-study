"""Section 1: Overview & Timeline - Case study introduction and key metrics."""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from utils.data_loader import (
    load_markets, load_vaults, load_timeline, load_asset_prices,
    load_market_supply_at_depeg, load_vault_toxic_exposure_at_depeg,
)
from utils.charts import apply_layout, SEVERITY_COLORS, RED, GREEN, BLUE, format_usd


def _esc(text: str) -> str:
    """Escape dollar signs for Streamlit markdown (avoids LaTeX rendering)."""
    return text.replace("$", "\\$")


def render():
    st.title("Overview - xUSD / deUSD Depeg Event")

    # - Key Metrics (prefer block8 verified data, fall back to block1) ---
    markets = load_markets()
    vaults = load_vaults()
    mkt_depeg = load_market_supply_at_depeg()
    vault_toxic = load_vault_toxic_exposure_at_depeg()

    if markets.empty and vaults.empty and mkt_depeg.empty:
        st.error(
            "Core data not available. Run the pipeline to generate block1 and/or block8 CSVs."
        )
        return

    # Use block8 for accurate supply figures if available
    if not mkt_depeg.empty:
        total_toxic_supply = mkt_depeg["supply_usd"].sum()
        n_toxic_markets = len(mkt_depeg)
        n_chains = mkt_depeg["chain"].nunique()
        chains_list = ", ".join(sorted(mkt_depeg["chain"].dropna().unique()))
    else:
        total_toxic_supply = markets["supply_usd"].sum() if not markets.empty else 0
        n_toxic_markets = len(markets) if not markets.empty else 0
        n_chains = markets["chain"].nunique() if not markets.empty and "chain" in markets.columns else 0
        chains_list = ", ".join(sorted(markets["chain"].dropna().unique())) if n_chains > 0 else ""

    # Bad debt from block1 (block8 does not duplicate this)
    total_bad_debt = markets["bad_debt_usd"].sum() if not markets.empty else 0

    n_vaults = len(vaults) if not vaults.empty else 0
    n_vaults_exposed = vault_toxic["vault_address"].nunique() if not vault_toxic.empty else n_vaults

    st.caption(_esc(
        f"November 2025: A stablecoin depeg cascaded through Morpho Blue markets, "
        f"exposing {format_usd(total_toxic_supply)} in toxic collateral across "
        f"{n_toxic_markets} markets and {n_vaults_exposed} vaults on {n_chains} chains."
    ))

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
    c1.metric("Toxic Markets", f"{n_toxic_markets}",
              help="Markets using xUSD, deUSD, or sdeUSD as collateral")
    c2.metric("Total Toxic Supply", _esc(format_usd(total_toxic_supply)),
              help="Combined supply across all toxic markets at depeg (Nov 3)")
    c3.metric("Total Bad Debt", _esc(format_usd(total_bad_debt)),
              delta=_esc(f"-{format_usd(total_bad_debt)}"), delta_color="inverse",
              help="Current protocol state. Accrues over time as interest compounds on unrepayable positions.")
    c4.metric("Chains Affected", str(n_chains), help=chains_list)
    c5.metric("Liquidation Events", str(n_liquidations),
              help="Oracle masking prevented liquidations. See Liquidation Failure page")

    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)

    # ── Asset Price Collapse ────────────────────────────────
    st.subheader("Token Price History")

    prices = load_asset_prices()
    if prices.empty:
        st.error("Asset price data not available. Run the pipeline to generate `block5_asset_prices.csv`.")
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
            collateral_summary["total_bad_debt"] = collateral_summary["total_bad_debt"].apply(format_usd)
            st.dataframe(
                collateral_summary,
                column_config={
                    "collateral": "Collateral",
                    "markets_count": st.column_config.NumberColumn("Markets", format="%d"),
                    "total_bad_debt": "Bad Debt",
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
            chain_summary["chain"] = chain_summary["chain"].str.title()
            chain_summary["total_bad_debt"] = chain_summary["total_bad_debt"].apply(format_usd)
            chain_summary["total_supply"] = chain_summary["total_supply"].apply(format_usd)
            st.dataframe(
                chain_summary,
                column_config={
                    "chain": "Chain",
                    "markets_count": st.column_config.NumberColumn("Markets", format="%d"),
                    "total_bad_debt": "Bad Debt",
                    "total_supply": "Supply",
                },
                hide_index=True,
                use_container_width=True,
            )
            st.caption(
                "Note: Plume supply excludes the unlisted Elixir-Stream market "
                "($70.9M at depeg) which is not reflected in the Morpho API's supply figures. "
                "See Market Exposure for the full picture including private markets."
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
            f"collateral values declining 95-99%. Hardcoded Chainlink oracles continued reporting "
            f"approximately \\$1.00, masking the true risk from the liquidation engine.\n\n"
        )
        if not _damaged.empty:
            _dam_details = []
            for _, _d in _damaged.sort_values("share_price_drawdown").iterrows():
                _dd_pct = abs(_d["share_price_drawdown"])
                # Use toxic exposure (trapped capital) as loss estimate, not TVL delta
                _toxic_exp = float(_d.get("exposure_usd", 0) or 0)
                if _toxic_exp > 0:
                    _dam_details.append(f"**{_d['vault_name']}** ({_dd_pct:.1%} share price drop, ~{format_usd(_toxic_exp)} trapped capital)")
                else:
                    _dam_details.append(f"**{_d['vault_name']}** ({_dd_pct:.1%} share price drop)")
            _q1_text += f"{len(_damaged)} vault{'s' if len(_damaged) > 1 else ''} suffered share-price damage: " + " and ".join(_dam_details)
            _q1_text += f". The remaining {_n_vaults - len(_damaged)} vaults emerged without share-price impact, largely due to proactive curator exits."
        _q1_text = _q1_text.replace("$", "\\$")
        st.markdown(_q1_text)

    with st.expander("**Q2:** How could the Morpho protocol itself have been more resilient?"):
        # Compute Q2 values from data
        _n_instant_tl = len(vaults[vaults["timelock_days"] == 0]) if not vaults.empty and "timelock_days" in vaults.columns else 0
        st.markdown(_esc(f"""
        Four structural areas identified: **(1) Oracle architecture** - Chainlink adapters lacked circuit-breakers
        or deviation thresholds, allowing stale $1.00 prices during collateral value declines of 95%+.
        **(2) Timelock configuration** - {_n_instant_tl} of {_n_vaults}
        vaults had instant (0-day) timelocks, enabling unchecked exposure changes, while longer timelocks
        delayed emergency exits during the crisis. **(3) No automated exit
        triggers** - curator-dependent response meant speed varied significantly across vaults.
        **(4) Liquidity contagion paths** - vaults bridging toxic and clean markets meant
        depositors in unaffected markets could experience reduced liquidity during stress events.
        """))
