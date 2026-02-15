"""Section 5: Liquidity Stress: Utilization spikes, TVL outflows, and withdrawal pressure."""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from utils.data_loader import load_utilization, load_net_flows
from utils.charts import apply_layout, depeg_vline, RED, BLUE, ORANGE, GREEN, format_usd


def render():
    st.title("Liquidity Stress Test")

    utilization = load_utilization()
    net_flows = load_net_flows()

    if utilization.empty and net_flows.empty:
        st.error("⚠️ Data not available. Run the pipeline to generate `block3_market_utilization_hourly.csv` and `block3_vault_net_flows.csv`.")
        return

    # ── Compute Key Metrics from Data ────────────────────────
    # Metric 1: Markets reaching 100% utilization
    if not utilization.empty:
        max_util_per_market = utilization.groupby("market")["utilization"].max()
        n_full_util = int((max_util_per_market >= 0.999).sum())
    else:
        n_full_util = 0

    # Clean net_flows early (used in multiple places)
    if not net_flows.empty:
        net_flows = net_flows[~net_flows["vault_name"].str.contains("Duplicated Key", case=False, na=False)]

    # Metric 2: Peak single-day outflow
    if not net_flows.empty and "daily_flow_usd" in net_flows.columns:
        peak_outflow = net_flows["daily_flow_usd"].min()
    else:
        peak_outflow = 0

    # Metrics 3 & 4: Average and worst TVL change over stress period
    # Sort by date before computing first/last to avoid ordering bugs
    if not net_flows.empty:
        nf_sorted = net_flows.sort_values("date")
        stress_summary = nf_sorted.groupby("vault_name").agg(
            start_tvl=("tvl_usd", "first"),
            end_tvl=("tvl_usd", "last"),
        ).reset_index()
        stress_summary["tvl_change_pct"] = (
            (stress_summary["end_tvl"] - stress_summary["start_tvl"])
            / stress_summary["start_tvl"].clip(lower=1)
        )
        # Weighted average by start TVL (prevents tiny vaults skewing the metric)
        total_start = stress_summary["start_tvl"].sum()
        if total_start > 0:
            avg_tvl_loss = (stress_summary["tvl_change_pct"] * stress_summary["start_tvl"]).sum() / total_start
        else:
            avg_tvl_loss = stress_summary["tvl_change_pct"].median()
        worst_tvl_loss = stress_summary["tvl_change_pct"].min()
        worst_vault = stress_summary.loc[stress_summary["tvl_change_pct"].idxmin(), "vault_name"] if len(stress_summary) > 0 else ""
    else:
        avg_tvl_loss = 0
        worst_tvl_loss = 0
        worst_vault = ""

    # Compute date range for display
    if not net_flows.empty:
        date_range_days = (pd.to_datetime(net_flows["date"]).max() - pd.to_datetime(net_flows["date"]).min()).days
    elif not utilization.empty:
        date_range_days = (pd.to_datetime(utilization["timestamp"]).max() - pd.to_datetime(utilization["timestamp"]).min()).days
    else:
        date_range_days = 15

    st.caption(
        f"Stress period analysis: utilization reached 100% across {n_full_util} markets, "
        f"while vault TVLs declined as depositors reduced exposure."
    )

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Markets at 100% Util", str(n_full_util), help="Markets reaching zero available liquidity")
    c2.metric("Peak Outflow Day", format_usd(peak_outflow), help="Largest single-day net outflow across all vaults")
    c3.metric(f"Wtd Avg TVL Change ({date_range_days}d)", f"{avg_tvl_loss:+.0%}",
              help="TVL-weighted average change across exposed vaults (weighted by starting TVL to avoid small-vault skew)")
    c4.metric("Worst TVL Change", f"{worst_tvl_loss:+.1%}",
              help=f"{worst_vault}: largest proportional decline" if worst_vault else "Largest proportional decline")

    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)

    # ── Utilization Chart ───────────────────────────────────
    st.subheader("Market Utilization: Depeg Period")

    if not utilization.empty:
        market_filter = st.multiselect(
            "Filter markets:",
            utilization["market"].unique().tolist(),
            default=utilization["market"].unique().tolist()[:4],
            key="util_filter",
        )

        mask = utilization["market"].isin(market_filter)
        fig = px.line(utilization[mask], x="timestamp", y="utilization", color="market")
        fig.update_traces(connectgaps=False)
        fig = apply_layout(fig, height=400)
        fig = depeg_vline(fig)
        fig.update_yaxes(title="Utilization", tickformat=".0%", range=[0, 1.1])
        fig.update_xaxes(title="")

        # Add 100% reference line
        fig.add_hline(y=1.0, line_dash="dot", line_color="rgba(0,0,0,0.15)")
        fig.add_annotation(x=1, xref="paper", y=1.0, text="100%",
                           showarrow=False, font=dict(size=10, color="rgba(0,0,0,0.35)"), xshift=10)
        st.plotly_chart(fig, use_container_width=True)

    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)

    # ── TVL Net Flows ───────────────────────────────────────
    st.subheader("Vault TVL During Stress Period")

    if not net_flows.empty:
        tab1, tab2 = st.tabs(["TVL Trajectories", "Daily Net Flows"])

        with tab1:
            all_vaults = net_flows["vault_name"].unique().tolist()
            vault_filter = st.multiselect(
                "Select vaults:",
                all_vaults,
                default=all_vaults[:5],
                key="tvl_filter",
            )
            mask = net_flows["vault_name"].isin(vault_filter)
            fig = px.line(net_flows[mask], x="date", y="tvl_usd", color="vault_name")
            fig.update_traces(connectgaps=False)
            fig = apply_layout(fig, height=420)
            fig = depeg_vline(fig)
            fig.update_yaxes(title="TVL (USD)", tickformat="$,.0f")
            st.plotly_chart(fig, use_container_width=True)

        with tab2:
            vault_select = st.selectbox(
                "Select vault:",
                net_flows["vault_name"].unique().tolist(),
                index=0,
                key="flow_select",
            )
            vault_data = net_flows[net_flows["vault_name"] == vault_select]

            colors = [RED if v < 0 else GREEN for v in vault_data["daily_flow_usd"]]
            fig = go.Figure(go.Bar(
                x=vault_data["date"],
                y=vault_data["daily_flow_usd"],
                marker_color=colors,
                text=vault_data["daily_flow_usd"].apply(format_usd),
                textposition="outside",
                textfont_size=9,
            ))
            fig = apply_layout(fig, title=f"Daily Net Flows: {vault_select}", height=380)
            fig = depeg_vline(fig)
            fig.update_yaxes(title="Net Flow (USD)", tickformat="$,.0f")
            st.plotly_chart(fig, use_container_width=True)

    # ── Stress Rankings ─────────────────────────────────────
    st.subheader("Vault Stress Rankings")

    if not net_flows.empty:
        # Reuse pre-sorted data for correct first/last computation
        nf_sorted = net_flows.sort_values("date")
        stress = nf_sorted.groupby("vault_name").agg(
            start_tvl=("tvl_usd", "first"),
            end_tvl=("tvl_usd", "last"),
            min_flow=("daily_flow_usd", "min"),
            withdrawal_days=("daily_flow_usd", lambda x: (x < 0).sum()),
            total_days=("daily_flow_usd", "count"),
        ).reset_index()
        stress["net_change_pct"] = ((stress["end_tvl"] - stress["start_tvl"]) / stress["start_tvl"].clip(lower=1)) * 100
        stress = stress.sort_values("net_change_pct")

        st.dataframe(
            stress,
            column_config={
                "vault_name": "Vault",
                "start_tvl": st.column_config.NumberColumn("Start TVL", format="$%,.0f"),
                "end_tvl": st.column_config.NumberColumn("End TVL", format="$%,.0f"),
                "net_change_pct": st.column_config.NumberColumn("Net Change", format="%.1f%%"),
                "min_flow": st.column_config.NumberColumn("Max Daily Outflow", format="$%,.0f"),
                "withdrawal_days": st.column_config.NumberColumn("Withdrawal Days", format="%d"),
            },
            hide_index=True,
            use_container_width=True,
        )

    st.info(
        "The stress period split outcomes cleanly. Large institutional vaults "
        "(Gauntlet, Smokehouse) experienced moderate outflows but maintained operations, while smaller "
        "vaults with concentrated toxic exposure experienced near-total withdrawal pressure within days."
    )
