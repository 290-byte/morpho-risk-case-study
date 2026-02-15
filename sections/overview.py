"""Section 1: Overview & Timeline: Case study introduction and key metrics."""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from utils.data_loader import load_markets, load_vaults, load_timeline, load_asset_prices
from utils.charts import apply_layout, SEVERITY_COLORS, RED, GREEN, BLUE, format_usd


def render():
    st.title("Overview: xUSD / deUSD Depeg Event")

    # ── Key Metrics ─────────────────────────────────────────
    markets = load_markets()
    vaults = load_vaults()

    if markets.empty and vaults.empty:
        st.error("⚠️ Core data not available. Run the pipeline to generate `block1_markets_graphql.csv` and `block1_vaults_graphql.csv`.")
        return

    # Separate public and private markets
    public_markets = markets[~markets.get("is_private_market", False)] if "is_private_market" in markets.columns else markets
    private_markets = markets[markets.get("is_private_market", False)] if "is_private_market" in markets.columns else pd.DataFrame()

    total_bad_debt = public_markets["bad_debt_usd"].sum() if not public_markets.empty else 0
    private_capital_lost = private_markets["original_capital_lost"].sum() if not private_markets.empty and "original_capital_lost" in private_markets.columns else 0
    n_chains = markets["chain"].nunique() if not markets.empty and "chain" in markets.columns else 0
    chains_list = ", ".join(sorted(markets["chain"].dropna().unique())) if n_chains > 0 else ""

    st.caption(
        f"November 2025: A stablecoin depeg cascaded through Morpho markets, "
        f"creating {format_usd(total_bad_debt)} in public bad debt plus ~{format_usd(private_capital_lost)} "
        f"lost in a private Elixir-Stream market across "
        f"{len(markets)} markets and {len(vaults)} vaults on {n_chains} chains.".replace("$", "\\$")
    )

    # ── Dashboard Framing ───────────────────────────────────
    with st.container(border=True):
        st.markdown(
            "**This dashboard answers two questions for a prospective integrator:**"
        )
        st.markdown(
            "**Q1.** What was the damage from the xUSD/deUSD depeg? "
            "How much bad debt, which vaults, and why didn't liquidations work?"
        )
        st.markdown(
            "**Q2.** Are liquidity risks shared across the protocol? "
            "How could Morpho be more resilient?"
        )
        st.caption(
            "Market Exposure through Damage Summary build the answer to Q1. "
            "Curator Response through Contagion Assessment build Q2. "
            "Recommendations tie it together."
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
    c1.metric("Toxic Markets", f"{len(markets)}" if not markets.empty else "-", help="Markets using xUSD, deUSD, or sdeUSD as collateral")
    c2.metric("Affected Vaults", f"{len(vaults)}" if not vaults.empty else "-", help="Unique vaults with current or historical exposure")
    c3.metric("Total Bad Debt", format_usd(total_bad_debt), delta=f"-{format_usd(total_bad_debt)}", delta_color="inverse")
    c4.metric("Chains Affected", str(n_chains), help=chains_list)
    c5.metric("Liquidation Events", str(n_liquidations),
              help="Oracle masking prevented liquidations. See Liquidation Failure page")

    # Second row: the real economic picture at time of depeg
    if not markets.empty:
        full_util = public_markets[public_markets["utilization"] > 0.99]
        # Use depeg-time supply (not current interest-inflated values)
        has_depeg = "supply_at_depeg" in full_util.columns and full_util["supply_at_depeg"].sum() > 0
        locked_supply = full_util["supply_at_depeg"].sum() if has_depeg else full_util["supply_usd"].sum()
        locked_supply_now = full_util["supply_usd"].sum()
        oracle_gap = locked_supply - total_bad_debt if locked_supply > total_bad_debt else 0

        if locked_supply > 0:
            # Compute trapped capital same way as Damage Summary
            from utils.data_loader import load_bridges
            _bridges = load_bridges()
            _trapped_total = 0.0
            if not _bridges.empty and not vaults.empty:
                _tox_col = "toxic_exposure_usd" if "toxic_exposure_usd" in _bridges.columns else "toxic_supply_usd"
                _addr_col = "vault_address" if "vault_address" in _bridges.columns else None
                _toxic_by_addr = {}
                if _addr_col and _tox_col in _bridges.columns:
                    for _, _b in _bridges.iterrows():
                        _va = str(_b.get(_addr_col, "")).lower().strip()
                        _tv = float(_b.get(_tox_col, 0) or 0)
                        if _va and _tv > 0:
                            _toxic_by_addr[_va] = _tv

                _damaged = vaults[vaults["share_price_drawdown"].abs() > 0.01] if "share_price_drawdown" in vaults.columns else pd.DataFrame()
                for _, v in _damaged.iterrows():
                    dd = abs(v.get("share_price_drawdown", 0))
                    va = str(v.get("vault_address", "")).lower().strip()
                    toxic_alloc = _toxic_by_addr.get(va, 0)
                    if toxic_alloc > 0:
                        _trapped_total += toxic_alloc
                    elif dd > 0.50:
                        ctv = float(v.get("tvl_usd", 0) or 0)
                        if ctv > 0 and dd < 1.0:
                            _trapped_total += ctv * (dd / (1.0 - dd))

            _private_loss_str = format_usd(private_capital_lost) if private_capital_lost > 0 else "68M USD"

            c6, c7, c8, c9, c10 = st.columns(5)
            _locked_help = "USDC supplied to public markets at depeg (Nov 4). Interest has since inflated this to " + format_usd(locked_supply_now) if has_depeg else "Current USDC in 100% util markets"
            c6.metric("Supply at Depeg (locked)", format_usd(locked_supply), help=_locked_help)
            c7.metric("Oracle-Masked Loss", format_usd(oracle_gap),
                      delta="not recognized", delta_color="off",
                      help="Gap between depeg-time supply and protocol-recognized bad debt")
            c8.metric("Trapped in Vaults", format_usd(_trapped_total) if _trapped_total > 0 else "-",
                      help="Capital locked in damaged vault allocations (from toxic market exposure)")
            c9.metric("Private Market Loss", _private_loss_str,
                      help="Elixir to Stream via unlisted Plume xUSD/USDC market. 65.8M xUSD at ~$1.03 pre-depeg")
            c10.metric("Damaged Vaults",
                       f"{len(vaults[vaults['share_price_drawdown'].abs() > 0.01])} / {len(vaults)}" if not vaults.empty and "share_price_drawdown" in vaults.columns else "-",
                       help="Vaults with >1% share price loss")

    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)

    # ── Asset Price Collapse ────────────────────────────────
    st.subheader("Token Price History")

    prices = load_asset_prices()
    if prices.empty:
        st.error("⚠️ Asset price data not available. Run the pipeline to generate `block5_asset_prices.csv`.")
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
        st.markdown("**Public Markets by Collateral Type**")
        if not public_markets.empty:
            collateral_summary = public_markets.groupby("collateral").agg(
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
        st.markdown("**Public Markets by Chain**")
        if not public_markets.empty:
            chain_summary = public_markets.groupby("chain").agg(
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

    # Private market callout
    if not private_markets.empty:
        _pm = private_markets.iloc[0]
        _pm_coll = float(_pm.get("original_capital_lost", 0) or 0)
        _pm_supply = float(_pm.get("supply_usd", 0) or 0)
        st.warning(
            f"**Private market (Plume): ~{format_usd(_pm_coll)} lost at the time of the depeg.** "
            f"An unlisted xUSD/USDC market on Plume, confirmed on-chain, matches the "
            f"~68M USD Elixir-to-Stream exposure cited in lawsuit filings. The 65.8M xUSD collateral "
            f"(worth ~68M USD pre-depeg) is now worthless. Interest has since inflated the "
            f"nominal supply to {format_usd(_pm_supply)}, but this is unrecoverable and "
            f"the relevant figure is the original capital lost."
        )

    # ── Q1 & Q2 Summary ────────────────────────────────────
    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
    st.subheader("Research Questions")

    with st.expander("**Q1:** What was the damage from the deUSD/xUSD situation? How much bad debt, and which vaults?"):
        # Compute Q1 values from public markets only
        _total_bd = public_markets["bad_debt_usd"].sum() if not public_markets.empty else 0
        _n_mkts = len(public_markets) if not public_markets.empty else 0
        _n_chains = public_markets["chain"].nunique() if not public_markets.empty and "chain" in public_markets.columns else 0
        _largest_mkt = public_markets.loc[public_markets["bad_debt_usd"].idxmax(), "market_label"] if _total_bd > 0 else "?"
        _largest_bd = public_markets["bad_debt_usd"].max() if _total_bd > 0 else 0

        # Damaged vaults from share price summary
        _n_vaults = len(vaults) if not vaults.empty else 0
        _damaged = vaults[vaults["share_price_drawdown"].abs() > 0.01] if not vaults.empty and "share_price_drawdown" in vaults.columns else pd.DataFrame()

        _q1_text = (
            f"**{format_usd(_total_bd)} in public-market bad debt** across {_n_mkts} markets "
            f"on {_n_chains} chains, with the largest concentration ({format_usd(_largest_bd)}) in "
            f"{_largest_mkt}. Only **{n_liquidations} liquidation events** occurred despite "
            f"collateral values declining 95-99%. Hardcoded Chainlink oracles continued reporting "
            f"approximately \\$1.00, masking the true risk from the liquidation engine.\n\n"
            f"Separately, an unlisted Plume xUSD/USDC market confirmed on-chain matches the "
            f"~68M USD Elixir-to-Stream private exposure cited in lawsuit filings. This private "
            f"market loss is tracked independently.\n\n"
        )
        if not _damaged.empty:
            _dam_details = []
            for _, _d in _damaged.sort_values("share_price_drawdown").iterrows():
                # Use toxic exposure (trapped capital) as loss estimate
                _toxic_exp = float(_d.get("exposure_usd", 0) or 0)
                if _toxic_exp > 0:
                    _dam_details.append(f"**{_d['vault_name']}** (~{format_usd(_toxic_exp)} trapped capital)")
                else:
                    _dam_details.append(f"**{_d['vault_name']}**")
            _q1_text += (
                f"{len(_damaged)} vault{'s' if len(_damaged) > 1 else ''} suffered permanent losses: "
                + " and ".join(_dam_details)
                + ". Most vault TVL was safely withdrawn by depositors before force-removal; "
                  "the actual damage was limited to trapped capital in toxic market allocations. "
                f"The remaining {_n_vaults - len(_damaged)} vaults emerged without impact, "
                "largely due to proactive curator exits."
            )
        _q1_text = _q1_text.replace("$", "\\$")
        st.markdown(_q1_text)

    with st.expander("**Q2:** How could the Morpho protocol itself have been more resilient?"):
        # Compute Q2 values from data
        _n_instant_tl = len(vaults[vaults["timelock_days"] == 0]) if not vaults.empty and "timelock_days" in vaults.columns else 0
        st.markdown(f"""
        Four structural areas identified: **(1) Oracle architecture**: Chainlink adapters lacked circuit-breakers
        or deviation thresholds, allowing stale \\$1.00 prices during collateral value declines of 95%+.
        **(2) Timelock configuration**: {_n_instant_tl} of {_n_vaults}
        vaults had instant (0-day) timelocks, enabling unchecked exposure changes, while longer timelocks
        delayed emergency exits during the crisis. **(3) No automated exit
        triggers**: curator-dependent response meant speed varied significantly across vaults.
        **(4) Liquidity contagion paths**: vaults bridging toxic and clean markets meant
        depositors in unaffected markets could experience reduced liquidity during stress events.
        """)
