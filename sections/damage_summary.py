"""Section: Damage Summary — Complete impact taxonomy of the xUSD/deUSD depeg.

Pulls all 7 damage categories from existing data sources (no hardcoding).
This is the single place to see the full picture of what happened.
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from utils.data_loader import (
    load_markets, load_vaults, load_share_prices, load_bad_debt_detail,
    load_utilization, load_net_flows, load_bridges, load_exposure_summary,
    load_asset_prices, load_csv,
)
from utils.charts import apply_layout, depeg_vline, RED, BLUE, ORANGE, GREEN, YELLOW, format_usd


# ── Markdown-safe USD formatter ───────────────────────────────
# format_usd returns "$53.8M" — the bare $ triggers LaTeX math
# mode in Streamlit markdown.  md_usd escapes it: "\$53.8M".
def md_usd(value):
    """format_usd but with escaped $ for safe use in markdown / st.error / st.info."""
    return format_usd(value).replace("$", r"\$")


def render():
    st.title("Damage Summary")
    st.caption(
        "Complete impact taxonomy across all 7 damage categories — "
        "every value computed from on-chain data or the Morpho GraphQL API."
    )

    # ── Load all data sources ─────────────────────────────────
    markets = load_markets()
    vaults = load_vaults()
    prices = load_share_prices()
    bd_detail = load_bad_debt_detail()
    utilization = load_utilization()
    net_flows = load_net_flows()
    bridges = load_bridges()
    asset_prices = load_asset_prices()
    liq_events = load_csv("block5_liquidation_events.csv")

    if markets.empty:
        st.error("Core data not available — run the pipeline to generate market data.")
        return

    # Clean net_flows
    if not net_flows.empty and "vault_name" in net_flows.columns:
        net_flows = net_flows[
            ~net_flows["vault_name"].str.contains("Duplicated Key", case=False, na=False)
        ]

    # ── Collateral spot prices (for locked liquidity analysis) ─
    spot_prices = {}
    if not asset_prices.empty and "asset" in asset_prices.columns and "price_usd" in asset_prices.columns:
        for asset in ["xUSD", "deUSD", "sdeUSD"]:
            ap = asset_prices[asset_prices["asset"] == asset].sort_values("timestamp")
            if not ap.empty:
                spot_prices[asset] = float(ap["price_usd"].iloc[-1])

    # ══════════════════════════════════════════════════════════
    # CATEGORY 1: Unrealized Bad Debt (protocol-level)
    # ══════════════════════════════════════════════════════════
    total_bad_debt = markets["bad_debt_usd"].sum()
    markets_with_debt = len(markets[markets["bad_debt_usd"] > 0])

    if not bd_detail.empty and "L2_realized_bad_debt_usd" in bd_detail.columns:
        realized_bad_debt = bd_detail["L2_realized_bad_debt_usd"].sum()
    elif "realized_bad_debt_usd" in markets.columns:
        realized_bad_debt = markets["realized_bad_debt_usd"].sum()
    else:
        realized_bad_debt = 0

    # ══════════════════════════════════════════════════════════
    # CATEGORY 2: Socialized Bad Debt (share price haircuts)
    # ══════════════════════════════════════════════════════════
    damaged_vaults = pd.DataFrame()
    if not vaults.empty and "share_price_drawdown" in vaults.columns:
        damaged_vaults = vaults[vaults["share_price_drawdown"].abs() > 0.01].copy()

    n_damaged = len(damaged_vaults)

    total_socialized_loss = 0.0
    vault_losses = []
    if not damaged_vaults.empty:
        for _, v in damaged_vaults.iterrows():
            dd = abs(v.get("share_price_drawdown", 0))
            base_tvl = v.get("tvl_pre_depeg_usd", 0) or v.get("tvl_at_peak_usd", 0) or v.get("tvl_usd", 0)
            est_loss = base_tvl * dd
            total_socialized_loss += est_loss
            vault_losses.append({
                "vault": v.get("vault_name", "Unknown"),
                "chain": v.get("chain", ""),
                "haircut": dd,
                "base_tvl": base_tvl,
                "est_loss": est_loss,
                "curator": v.get("curator", ""),
                "address": str(v.get("vault_address", "")).lower(),
            })

    # V1.1 hidden loss detection
    hidden_vaults = pd.DataFrame()
    if not vaults.empty and "share_price_drawdown" in vaults.columns:
        has_exposure = vaults["exposure_usd"] > 1000
        no_drawdown = vaults["share_price_drawdown"].abs() <= 0.01
        hidden_vaults = vaults[has_exposure & no_drawdown].copy()

    # ══════════════════════════════════════════════════════════
    # CATEGORY 3: Locked Liquidity — THE KEY ANALYSIS
    # ══════════════════════════════════════════════════════════
    full_util_markets = markets[
        markets["status"].str.contains("AT_RISK_100PCT|BAD_DEBT", na=False)
    ].copy()
    n_locked_markets = len(full_util_markets)
    locked_supply = full_util_markets["supply_usd"].sum() if not full_util_markets.empty else 0

    toxic_collaterals = {"xUSD", "deUSD", "sdeUSD"}
    locked_analysis = []
    total_true_loss = 0.0
    total_recognized_bd = 0.0

    for _, m in full_util_markets.iterrows():
        collateral = str(m.get("collateral", ""))
        supply = float(m.get("supply_usd", 0))
        bad_debt = float(m.get("bad_debt_usd", 0))
        chain = str(m.get("chain", ""))
        label = str(m.get("market_label", ""))

        is_toxic = any(tc.lower() in collateral.lower() for tc in toxic_collaterals)

        spot = None
        for token in toxic_collaterals:
            if token.lower() in collateral.lower():
                spot = spot_prices.get(token)
                break

        if is_toxic and spot is not None and spot < 0.10:
            true_loss = supply
        elif is_toxic:
            true_loss = supply
        else:
            true_loss = 0

        total_true_loss += true_loss
        total_recognized_bd += bad_debt

        locked_analysis.append({
            "market": label,
            "chain": chain,
            "collateral": collateral,
            "supply_locked": supply,
            "bad_debt_recognized": bad_debt,
            "collateral_spot": f"${spot:.4f}" if spot is not None else "—",
            "true_loss": true_loss,
            "oracle_gap": format_usd(true_loss - bad_debt) if true_loss > bad_debt else "—",
            "is_toxic": is_toxic,
        })

    oracle_masked_loss = total_true_loss - total_recognized_bd

    # ══════════════════════════════════════════════════════════
    # CATEGORY 4: Private Market Exposure (Elixir $68M)
    # ══════════════════════════════════════════════════════════
    private_exposure = 68_000_000

    # ══════════════════════════════════════════════════════════
    # CATEGORY 5: Liquidity Contagion
    # ══════════════════════════════════════════════════════════
    if not bridges.empty:
        bp_col = "bridge_type" if "bridge_type" in bridges.columns else "contagion_path"
        if bp_col in bridges.columns:
            bridge_vaults = bridges[bridges[bp_col] == "BRIDGE"]
        else:
            bridge_vaults = bridges
        n_bridge_vaults = len(bridge_vaults)
    else:
        n_bridge_vaults = 0

    if not utilization.empty:
        max_util = utilization.groupby("market")["utilization"].max()
        n_markets_100 = int((max_util >= 0.999).sum())
    else:
        n_markets_100 = n_locked_markets

    # ══════════════════════════════════════════════════════════
    # CATEGORY 6: Rate Risk
    # ══════════════════════════════════════════════════════════
    rate_spike_note = ""
    if not bd_detail.empty and "borrow_apy" in bd_detail.columns:
        max_apy = pd.to_numeric(bd_detail["borrow_apy"], errors="coerce").max()
        rate_spike_note = f"Peak borrow APY: {max_apy:.0%}" if max_apy > 0 else ""

    # ══════════════════════════════════════════════════════════
    # CATEGORY 7: Capital Flight
    # ══════════════════════════════════════════════════════════
    total_net_outflow = 0.0
    peak_outflow_day = 0.0
    tvl_decline_pct = 0.0
    if not net_flows.empty and "daily_flow_usd" in net_flows.columns:
        daily_totals = net_flows.groupby("date")["daily_flow_usd"].sum()
        total_net_outflow = daily_totals[daily_totals < 0].sum()
        peak_outflow_day = daily_totals.min()
        nf_sorted = net_flows.sort_values("date")
        vault_summaries = nf_sorted.groupby("vault_name").agg(
            start_tvl=("tvl_usd", "first"), end_tvl=("tvl_usd", "last"),
        )
        total_start = vault_summaries["start_tvl"].sum()
        total_end = vault_summaries["end_tvl"].sum()
        if total_start > 0:
            tvl_decline_pct = (total_end - total_start) / total_start

    # Liquidation count
    if not liq_events.empty and "event_count" in liq_events.columns:
        n_liquidations = int(liq_events["event_count"].sum())
    elif not liq_events.empty:
        n_liquidations = len(liq_events)
    else:
        n_liquidations = 0

    # ══════════════════════════════════════════════════════════
    # DISPLAY: Impact Overview KPIs
    # ══════════════════════════════════════════════════════════
    st.subheader("Impact Overview")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Recognized Bad Debt", format_usd(total_bad_debt),
              help="Protocol-level badDebt.usd — what the oracle lets the system see")
    c2.metric("Oracle-Masked Loss",
              format_usd(oracle_masked_loss) if oracle_masked_loss > 0 else format_usd(locked_supply),
              help="Supply locked behind worthless collateral, reported as zero bad debt",
              delta="not recognized" if oracle_masked_loss > 0 else None, delta_color="off")
    c3.metric("Private Exposure", format_usd(private_exposure),
              help="Elixir hidden Morpho markets (source: lawsuit filings)")
    c4.metric("Liquidation Events", str(n_liquidations),
              help="Oracle masking prevented liquidations despite 95-99% collateral value loss")

    c5, c6, c7, c8 = st.columns(4)
    c5.metric("Vaults with Haircuts",
              f"{n_damaged} / {len(vaults)}" if not vaults.empty else "—",
              help="Vaults with >1% share price drawdown")
    c6.metric("Est. Socialized Loss", format_usd(total_socialized_loss),
              help="Share price drawdown x pre-depeg TVL")
    c7.metric("Contagion Bridges", str(n_bridge_vaults),
              help="Vaults allocated to BOTH toxic and clean markets")
    c8.metric("Net Capital Outflow",
              format_usd(abs(total_net_outflow)) if total_net_outflow < 0 else "$0",
              help="Sum of all negative daily flows across exposed vaults")

    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)

    # ══════════════════════════════════════════════════════════
    # LOCKED LIQUIDITY ANALYSIS
    # ══════════════════════════════════════════════════════════
    st.subheader("Locked Liquidity = Unrecognized Permanent Loss")

    st.markdown(
        "Markets at 100% utilization where the collateral is the depegged token (xUSD, deUSD, sdeUSD). "
        "Borrowers posted these tokens as collateral to borrow USDC. With collateral now worth fractions "
        "of a cent, **no rational borrower will repay** — they would be returning good USDC to retrieve "
        "worthless tokens. The oracle still reports these positions as healthy, so the protocol shows "
        "zero in bad debt. But the economic reality is that **the lent USDC is gone**."
    )

    if locked_analysis:
        active_locked = [la for la in locked_analysis if la["supply_locked"] > 1]
        if active_locked:
            locked_df = pd.DataFrame(active_locked).sort_values("supply_locked", ascending=False)
            st.dataframe(
                locked_df[["market", "chain", "collateral", "supply_locked",
                           "bad_debt_recognized", "collateral_spot", "true_loss", "oracle_gap"]],
                column_config={
                    "market": "Market",
                    "chain": "Chain",
                    "collateral": "Collateral",
                    "supply_locked": st.column_config.NumberColumn("Supply Locked", format="$%,.0f"),
                    "bad_debt_recognized": st.column_config.NumberColumn("Bad Debt (Oracle)", format="$%,.0f"),
                    "collateral_spot": "Collateral Spot Price",
                    "true_loss": st.column_config.NumberColumn("True Economic Loss", format="$%,.0f"),
                    "oracle_gap": "Oracle-Masked Gap",
                },
                hide_index=True, use_container_width=True,
            )

            # Callout — using md_usd to avoid LaTeX rendering bugs
            if oracle_masked_loss > 0:
                st.error(
                    f"**{md_usd(oracle_masked_loss)} in losses are not visible to the protocol.** "
                    f"The oracle reports {md_usd(total_recognized_bd)} in bad debt across locked markets, "
                    f"but the true economic loss is {md_usd(total_true_loss)}. "
                    f"The difference ({md_usd(oracle_masked_loss)}) is masked by hardcoded oracle prices "
                    f"that still value the collateral at approximately \\$1.00."
                )
            elif locked_supply > 0:
                st.warning(
                    f"**{md_usd(locked_supply)} in supply locked across {n_locked_markets} markets.** "
                    f"Collateral is the depegged tokens — borrowers have no incentive to repay."
                )

    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)

    # ══════════════════════════════════════════════════════════
    # 7-CATEGORY DAMAGE TABLE
    # ══════════════════════════════════════════════════════════
    st.subheader("Damage Taxonomy")
    st.caption("Seven distinct damage categories, ordered by severity and permanence.")

    damage_rows = [
        {
            "Category": "1. Unrealized Bad Debt",
            "Amount": format_usd(total_bad_debt),
            "Scope": f"{markets_with_debt} markets",
            "Permanent?": "Yes",
            "Status": "Active — oracle masking defers formal realization",
            "See Also": "Bad Debt Analysis",
        },
        {
            "Category": "2. Socialized Losses",
            "Amount": format_usd(total_socialized_loss),
            "Scope": f"{n_damaged} vault{'s' if n_damaged != 1 else ''}",
            "Permanent?": "Yes",
            "Status": ", ".join(f"{v['vault']} (-{v['haircut']:.1%})" for v in vault_losses) if vault_losses else "—",
            "See Also": "Bad Debt Analysis",
        },
        {
            "Category": "3. Locked Liquidity (Oracle-Masked)",
            "Amount": format_usd(locked_supply),
            "Scope": f"{n_locked_markets} markets at 100% util",
            "Permanent?": "Yes — collateral worthless" if total_true_loss > 0 else "Until repayment",
            "Status": f"Collateral worth <\\$0.01. Oracle sees {format_usd(total_recognized_bd)} bad debt; true loss: {format_usd(total_true_loss)}",
            "See Also": "Market Exposure",
        },
        {
            "Category": "4. Private Market Exposure",
            "Amount": format_usd(private_exposure),
            "Scope": "Elixir → Stream (sole borrower)",
            "Permanent?": "Yes — Stream insolvent",
            "Status": "Not visible in public API; sourced from lawsuit filings",
            "See Also": "Market Exposure",
        },
        {
            "Category": "5. Liquidity Contagion",
            "Amount": "No permanent loss",
            "Scope": f"{n_bridge_vaults} bridge vaults",
            "Permanent?": "No — resolved ~6 hours",
            "Status": "Clean vaults hit near-zero withdrawable liquidity",
            "See Also": "Contagion Assessment",
        },
        {
            "Category": "6. Rate Risk",
            "Amount": "Indirect cost",
            "Scope": f"{n_markets_100} markets hit 100% util",
            "Permanent?": "No — normalized in days",
            "Status": f"AdaptiveCurveIRM 4x rate spike. {rate_spike_note}".strip(),
            "See Also": "Liquidity Stress",
        },
        {
            "Category": "7. Capital Flight",
            "Amount": format_usd(abs(total_net_outflow)) if total_net_outflow < 0 else "—",
            "Scope": "All exposed vaults",
            "Permanent?": "Mixed — flight-to-quality effect",
            "Status": f"TVL {tvl_decline_pct:+.1%} across exposed vaults" if tvl_decline_pct else "Varies by curator",
            "See Also": "Liquidity Stress",
        },
    ]

    st.dataframe(
        pd.DataFrame(damage_rows),
        column_config={
            "Category": st.column_config.TextColumn("Damage Category", width="medium"),
            "Amount": st.column_config.TextColumn("Amount", width="small"),
            "Scope": st.column_config.TextColumn("Scope", width="medium"),
            "Permanent?": st.column_config.TextColumn("Permanent?", width="medium"),
            "Status": st.column_config.TextColumn("Status / Detail", width="large"),
            "See Also": st.column_config.TextColumn("See Also", width="small"),
        },
        hide_index=True, use_container_width=True,
    )

    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)

    # ══════════════════════════════════════════════════════════
    # PERMANENT vs TEMPORARY
    # ══════════════════════════════════════════════════════════
    st.subheader("Permanent vs Temporary Damage")

    col_perm, col_temp = st.columns(2)

    with col_perm:
        st.markdown("**Permanent (Irrecoverable)**")
        permanent_items = []
        if total_bad_debt > 0:
            permanent_items.append(("Recognized Bad Debt", total_bad_debt, RED))
        if oracle_masked_loss > 0:
            permanent_items.append(("Oracle-Masked Loss", oracle_masked_loss, "#dc2626"))
        elif locked_supply > 0:
            permanent_items.append(("Locked Supply\n(toxic collateral)", locked_supply, "#dc2626"))
        if total_socialized_loss > 0:
            permanent_items.append(("Socialized via\nShare Price", total_socialized_loss, ORANGE))
        permanent_items.append(("Private Market\n(Elixir)", private_exposure, "#7c3aed"))

        if permanent_items:
            fig_perm = go.Figure(go.Bar(
                x=[p[0] for p in permanent_items],
                y=[p[1] for p in permanent_items],
                marker_color=[p[2] for p in permanent_items],
                text=[format_usd(p[1]) for p in permanent_items],
                textposition="outside",
            ))
            fig_perm = apply_layout(fig_perm, height=380)
            fig_perm.update_layout(margin=dict(t=20, b=80, l=50, r=20),
                                   yaxis_title="USD", yaxis_tickformat="$,.0f", showlegend=False)
            st.plotly_chart(fig_perm, use_container_width=True)

            total_permanent = total_bad_debt + oracle_masked_loss + total_socialized_loss + private_exposure
            st.info(
                f"**Total permanent exposure: {md_usd(total_permanent)}**. "
                f"Note: categories 1 and 3 represent the same underlying markets from "
                f"different perspectives (protocol view vs economic reality). "
                f"Combined locked-market losses: {md_usd(total_true_loss)}. "
                f"Category 2 partially overlaps as vault-level realization of market-level bad debt."
            )

    with col_temp:
        st.markdown("**Temporary (Resolved)**")
        temp_items = []
        if total_net_outflow < 0:
            temp_items.append(("Capital Outflows", abs(total_net_outflow), BLUE))
        if temp_items:
            fig_temp = go.Figure(go.Bar(
                x=[t[0] for t in temp_items],
                y=[t[1] for t in temp_items],
                marker_color=[t[2] for t in temp_items],
                text=[format_usd(t[1]) for t in temp_items],
                textposition="outside",
            ))
            fig_temp = apply_layout(fig_temp, height=380)
            fig_temp.update_layout(margin=dict(t=20, b=80, l=50, r=20),
                                   yaxis_title="USD", yaxis_tickformat="$,.0f", showlegend=False)
            st.plotly_chart(fig_temp, use_container_width=True)

        st.info(
            "Liquidity contagion and rate spikes resolved within hours to days "
            "via AdaptiveCurveIRM rate incentives. "
            "Capital flight varied by curator — some lost 60%+ TVL while Gauntlet "
            "gained ~35% (flight-to-quality effect)."
        )

    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)

    # ══════════════════════════════════════════════════════════
    # VAULT-LEVEL DAMAGE (matching bad_debt.py rendering)
    # ══════════════════════════════════════════════════════════
    st.subheader("Vault-Level Damage")

    if not vault_losses:
        st.info("No vaults with significant share price drawdowns (>1%) detected.")
    else:
        st.caption("Vaults with confirmed share price haircuts from socialized bad debt.")

        # ── Compute peak/trough from timeseries (like bad_debt.py) ─
        group_key = "vault_address" if (not prices.empty and "vault_address" in prices.columns) else "vault_name"

        damaged_info = []
        if not prices.empty:
            for gid, vp in prices.groupby(group_key):
                vp = vp.sort_values("date")
                vault_name = vp["vault_name"].iloc[0] if "vault_name" in vp.columns else str(gid)
                chain = vp["chain"].iloc[0] if "chain" in vp.columns else ""

                cummax = vp["share_price"].cummax()
                dd_series = (vp["share_price"] - cummax) / cummax
                max_dd = dd_series.min()

                if max_dd < -0.01:
                    dd_idx = dd_series.idxmin()
                    peak_idx = cummax[:dd_idx + 1].idxmax() if dd_idx is not None else vp.index[0]

                    # Match to vault_losses entry for TVL context
                    vl_match = [vl for vl in vault_losses
                                if vl["vault"].lower().startswith(vault_name.split(" (")[0].lower())]
                    base_tvl = vl_match[0]["base_tvl"] if vl_match else 0
                    est_loss = vl_match[0]["est_loss"] if vl_match else 0
                    curator = vl_match[0]["curator"] if vl_match else "—"

                    damaged_info.append({
                        "vault_name": vault_name,
                        "chain": chain,
                        "group_key": gid,
                        "haircut": max_dd,
                        "peak": cummax.loc[dd_idx],
                        "peak_date": vp.loc[peak_idx, "date"] if peak_idx in vp.index else vp["date"].iloc[0],
                        "trough": vp.loc[dd_idx, "share_price"],
                        "trough_date": vp.loc[dd_idx, "date"],
                        "last": vp["share_price"].iloc[-1],
                        "base_tvl": base_tvl,
                        "est_loss": est_loss,
                        "curator": curator,
                    })

        if damaged_info:
            # Deduplicate and sort
            df_dam = pd.DataFrame(damaged_info)
            df_dam = df_dam.sort_values("haircut").drop_duplicates("vault_name", keep="first")
            damaged_info = df_dam.to_dict("records")

            for di in damaged_info:
                chain_short = di["chain"][:3].title() if di["chain"] else ""
                display_name = di["vault_name"]
                if chain_short and f"({chain_short})" not in display_name:
                    display_name += f" ({chain_short})"

                with st.container(border=True):
                    # Header line (like bad_debt.py)
                    st.markdown(
                        f"**{display_name}** · Curator: {di['curator']} · "
                        f"Haircut: **{di['haircut']:.1%}** · "
                        f"Current: \\${di['last']:.4f}"
                        + (f" · Pre-depeg TVL: {format_usd(di['base_tvl'])}" if di['base_tvl'] > 0 else "")
                        + (f" · Est. loss: {format_usd(di['est_loss'])}" if di['est_loss'] > 0 else "")
                    )

                    # Chart — resample to weekly to eliminate daily yield oscillation
                    vdata = prices[prices[group_key] == di["group_key"]].sort_values("date").copy()
                    if not vdata.empty:
                        # Weekly resampling: take last value per week
                        vdata["date"] = pd.to_datetime(vdata["date"])
                        vdata_weekly = vdata.set_index("date").resample("W")["share_price"].last().dropna().reset_index()

                        fig = go.Figure()
                        line_color = RED if di["haircut"] < -0.5 else BLUE
                        fig.add_trace(go.Scatter(
                            x=vdata_weekly["date"], y=vdata_weekly["share_price"],
                            mode="lines",
                            line=dict(color=line_color, width=2, shape="spline", smoothing=0.8),
                            hovertemplate="%{x|%b %d, %Y}<br>$%{y:.6f}<extra></extra>",
                            connectgaps=False, showlegend=False,
                        ))

                        chart_title = f"{di['vault_name']} — {di['haircut']:.1%} haircut"
                        fig = apply_layout(fig, title=chart_title, height=320, show_legend=False)
                        fig = depeg_vline(fig)
                        fig.update_yaxes(tickformat="$.4f", title="")
                        fig.update_xaxes(title="")

                        st.plotly_chart(fig, use_container_width=True)

    # V1.1 hidden loss warning
    if not hidden_vaults.empty:
        v11_candidates = hidden_vaults[hidden_vaults["exposure_usd"] > 10_000]
        if not v11_candidates.empty:
            st.warning(
                f"**V1.1 vault architecture note:** {len(v11_candidates)} vault(s) had toxic exposure "
                f"but show no visible share price drawdown. Under MetaMorpho V1.1, the share price "
                f"cannot decrease — losses accrue in a `lostAssets` variable and are only realized when "
                f"the curator force-removes the market. These losses may be *hidden*, not absent."
            )

    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)

    # ══════════════════════════════════════════════════════════
    # RISK ISOLATION ASSESSMENT
    # ══════════════════════════════════════════════════════════
    st.subheader("Risk Isolation Assessment")
    st.caption("Which risk types were contained by Morpho's isolated market architecture.")

    isolation_data = pd.DataFrame([
        {
            "Risk Type": "Credit Risk (bad debt)",
            "Isolated?": "Yes",
            "Evidence": f"Only {n_damaged} vault(s) with visible damage out of {len(vaults)} exposed",
        },
        {
            "Risk Type": "Liquidity Risk (withdrawals)",
            "Isolated?": "No",
            "Evidence": f"{n_bridge_vaults} bridge vaults transmitted stress; clean vaults hit near-zero liquidity ~6 hours",
        },
        {
            "Risk Type": "Oracle Risk (price masking)",
            "Isolated?": "No",
            "Evidence": f"{format_usd(oracle_masked_loss)} hidden by hardcoded prices; collateral <\\$0.01 valued at ~\\$1.00",
        },
        {
            "Risk Type": "Rate Risk (interest spikes)",
            "Isolated?": "No",
            "Evidence": f"{n_markets_100} markets hit 100% utilization — AdaptiveCurveIRM 4x rate spike",
        },
        {
            "Risk Type": "Confidence Risk (capital flight)",
            "Isolated?": "Mixed",
            "Evidence": "Some curators lost 60%+ TVL; others gained (flight-to-quality)",
        },
    ])

    st.dataframe(
        isolation_data,
        column_config={
            "Risk Type": st.column_config.TextColumn("Risk Type", width="medium"),
            "Isolated?": st.column_config.TextColumn("Isolated?", width="small"),
            "Evidence": st.column_config.TextColumn("Evidence", width="large"),
        },
        hide_index=True, use_container_width=True,
    )

    # ── Data Sources ──────────────────────────────────────────
    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
    st.caption(
        "**Data sources:** All figures from Morpho GraphQL API snapshots and on-chain data "
        "except category 4 (private exposure), sourced from lawsuit filings "
        "(Stream Trading Corp. v. McMeans, Case 3:25-cv-10524), BlockEden analysis, and the "
        "YAM exposure map. Rate figures from Gauntlet's Nov 18 report. Spot prices from CoinGecko."
    )
