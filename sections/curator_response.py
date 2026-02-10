"""Section 4: Curator Response — How curators reacted to toxic exposure."""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from utils.data_loader import load_vaults
from utils.charts import apply_layout, RESPONSE_COLORS, RED, GREEN, BLUE, YELLOW, format_usd


def render():
    st.title("Curator Response Analysis")
    st.caption(
        "15 vaults exited proactively (weeks before depeg), 3 reacted early, "
        "1 was slow, and 7 never properly exited — response speed was the #1 predictor of outcome."
    )

    vaults = load_vaults()
    if vaults.empty:
        st.error("⚠️ Vault data not available — run the pipeline to generate `block1_vaults_graphql.csv`.")
        return

    # ── Key Metrics ─────────────────────────────────────────
    response_counts = vaults["response_class"].value_counts()
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Proactive", response_counts.get("PROACTIVE", 0), help="Exited >7 days before depeg")
    c2.metric("Early Reactor", response_counts.get("EARLY_REACTOR", 0), help="Exited 0-7 days before depeg")
    c3.metric("Slow Reactor", response_counts.get("SLOW_REACTOR", 0), help="Exited within 7 days after depeg")
    c4.metric("Very Late / No Exit", response_counts.get("VERY_LATE", 0), help="Still exposed weeks/months later")

    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)

    # ── Response Timeline ───────────────────────────────────
    st.subheader("Response Timeline")

    vaults["response_date"] = pd.to_datetime(vaults["response_date"])
    depeg_date = pd.Timestamp("2025-11-04")

    fig = go.Figure()

    for _, v in vaults.iterrows():
        color = RESPONSE_COLORS.get(v["response_class"], "#666")
        fig.add_trace(go.Scatter(
            x=[v["response_date"]],
            y=[v["vault_name"]],
            mode="markers",
            marker=dict(size=10, color=color),
            showlegend=False,
            hovertext=f"{v['vault_name']}<br>Curator: {v['curator']}<br>Response: {v['response_class']}<br>Days vs depeg: {v['days_before_depeg']:+.0f}d",
            hoverinfo="text",
        ))

    # Depeg line
    import pandas as pd_dt
    fig.add_vline(x=pd_dt.Timestamp("2025-11-04"), line_dash="dash", line_color=RED, opacity=0.7)
    fig.add_annotation(x=pd_dt.Timestamp("2025-11-04"), y=1, yref="paper", text="DEPEG",
                       showarrow=False, font=dict(color=RED, size=11), yshift=10)

    fig = apply_layout(fig, height=550)
    fig.update_xaxes(title="Response Date", range=["2025-08-15", "2026-02-15"])
    fig.update_yaxes(title="", autorange="reversed")
    st.plotly_chart(fig, use_container_width=True)

    # ── Response Matrix ─────────────────────────────────────
    st.subheader("Response Classification")

    for response_class in ["PROACTIVE", "EARLY_REACTOR", "SLOW_REACTOR", "VERY_LATE"]:
        subset = vaults[vaults["response_class"] == response_class].sort_values("tvl_usd", ascending=False)
        if subset.empty:
            continue

        color = RESPONSE_COLORS.get(response_class, "#666")
        total_tvl = subset["tvl_usd"].sum()

        with st.expander(
            f"**{response_class}** — {len(subset)} vaults, {format_usd(total_tvl)} TVL",
            expanded=(response_class == "PROACTIVE"),
        ):
            st.dataframe(
                subset[["vault_name", "chain", "curator", "tvl_usd", "days_before_depeg",
                         "timelock_days", "share_price_drawdown"]],
                column_config={
                    "vault_name": "Vault",
                    "chain": "Chain",
                    "curator": "Curator",
                    "tvl_usd": st.column_config.NumberColumn("TVL", format="$%,.0f"),
                    "days_before_depeg": st.column_config.NumberColumn("Days Before Depeg", format="%+.1f"),
                    "timelock_days": st.column_config.NumberColumn("Timelock (days)", format="%.0f"),
                    "share_price_drawdown": st.column_config.NumberColumn("SP Drawdown", format="%.2%"),
                },
                hide_index=True,
                use_container_width=True,
            )

    # ── Timelock Analysis ───────────────────────────────────
    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
    st.subheader("Timelock Distribution")
    st.caption("Vaults with 0-day timelocks could change allocations instantly — a governance risk.")

    col1, col2 = st.columns(2)

    with col1:
        instant = len(vaults[vaults["timelock_days"] == 0])
        timelocked = len(vaults[vaults["timelock_days"] > 0])
        fig = go.Figure(go.Pie(
            values=[instant, timelocked],
            labels=["Instant (0 days)", "Timelocked (>0 days)"],
            hole=0.5,
            marker_colors=[RED, GREEN],
            textinfo="label+value",
        ))
        fig = apply_layout(fig, height=300, show_legend=False)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        timelock_dist = vaults.groupby("timelock_days").size().reset_index(name="count")
        fig = px.bar(timelock_dist, x="timelock_days", y="count",
                     text="count", color_discrete_sequence=[BLUE])
        fig = apply_layout(fig, height=300)
        fig.update_xaxes(title="Timelock (days)")
        fig.update_yaxes(title="Vault Count")
        fig.update_traces(textposition="outside")
        st.plotly_chart(fig, use_container_width=True)

    # ── Key Insight ─────────────────────────────────────────
    st.info(
        "**Key finding:** Response speed, not vault size, determined outcomes. "
        "Gauntlet's \\$192M frontier vault exited 63 days before depeg with zero loss, "
        "while Relend (\\$63K TVL) suffered 98.4% drawdown because it couldn't exit its position in time."
    )
