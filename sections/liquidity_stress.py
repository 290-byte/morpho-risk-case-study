"""Section 5: Liquidity Stress — Utilization spikes, TVL outflows, and withdrawal pressure."""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from utils.data_loader import load_utilization, load_net_flows
from utils.charts import apply_layout, depeg_vline, RED, BLUE, ORANGE, GREEN, format_usd


def render():
    st.title("Liquidity Stress Test")
    st.caption(
        "Nov 1–15 stress period: utilization spiked to 100% across 6 markets, "
        "while vault TVLs collapsed as depositors rushed to withdraw."
    )

    utilization = load_utilization()
    net_flows = load_net_flows()

    # ── Key Metrics ─────────────────────────────────────────
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Markets at 100% Util", "6", help="Markets with zero available liquidity")
    c2.metric("Peak Outflow Day", "-$266M", help="MEV Capital USDC single-day outflow")
    c3.metric("Avg TVL Loss (14d)", "-62%", help="Average across all exposed vaults")
    c4.metric("Worst TVL Loss", "-99.8%", help="Relend USDC — near-total withdrawal")

    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)

    # ── Utilization Chart ───────────────────────────────────
    st.subheader("Market Utilization — Depeg Period")

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
            fig = apply_layout(fig, title=f"Daily Net Flows — {vault_select}", height=380)
            fig = depeg_vline(fig)
            fig.update_yaxes(title="Net Flow (USD)", tickformat="$,.0f")
            st.plotly_chart(fig, use_container_width=True)

    # ── Stress Rankings ─────────────────────────────────────
    st.subheader("Vault Stress Rankings")

    if not net_flows.empty:
        # Calculate summary per vault
        stress = net_flows.groupby("vault_name").agg(
            start_tvl=("tvl_usd", "first"),
            end_tvl=("tvl_usd", "last"),
            min_flow=("daily_flow_usd", "min"),
            withdrawal_days=("daily_flow_usd", lambda x: (x < 0).sum()),
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
                "withdrawal_days": st.column_config.NumberColumn("Withdrawal Days", format="%d / 15"),
            },
            hide_index=True,
            use_container_width=True,
        )

    st.info(
        "**Key finding:** The stress test revealed a bifurcation — large institutional vaults "
        "(Gauntlet, Smokehouse) saw moderate outflows but maintained operations, while smaller "
        "vaults with toxic exposure experienced near-total bank runs within days."
    )
