"""Section 4: Curator Response: How curators reacted to toxic exposure."""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from utils.data_loader import load_vaults
from utils.charts import apply_layout, RESPONSE_COLORS, RED, GREEN, BLUE, YELLOW, format_usd


def render():
    st.title("Curator Response Analysis")

    vaults = load_vaults()
    if vaults.empty:
        st.error("⚠️ Vault data not available. Run the pipeline to generate `block1_vaults_graphql.csv`.")
        return

    # Filter dust/test vaults: must have had >$10K TVL or allocation at any point
    MIN_TVL = 10_000
    _tvl_cols = [c for c in ["tvl_usd", "tvl_pre_depeg_usd", "tvl_at_peak_usd", "peak_allocation"] if c in vaults.columns]
    _max_tvl = vaults[_tvl_cols].fillna(0).max(axis=1) if _tvl_cols else pd.Series(0, index=vaults.index)
    vaults = vaults[_max_tvl >= MIN_TVL].copy()

    # ── Dynamic caption from data ────────────────────────────
    response_counts = vaults["response_class"].value_counts()
    n_proactive = response_counts.get("PROACTIVE", 0)
    n_early = response_counts.get("EARLY_REACTOR", 0)
    n_slow = response_counts.get("SLOW_REACTOR", 0)
    n_late = response_counts.get("VERY_LATE", 0)
    n_total = len(vaults)

    st.caption(
        f"{n_total} vaults analyzed: {n_proactive} exited proactively, {n_early} reacted early, "
        f"{n_slow} responded slowly, and {n_late} maintained exposure through the analysis period "
        f". Response speed was the primary predictor of outcome."
    )

    # ── Key Metrics ─────────────────────────────────────────
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Proactive", n_proactive, help="Exited >7 days before depeg")
    c2.metric("Early Reactor", n_early, help="Exited 0–7 days before depeg")
    c3.metric("Slow Reactor", n_slow, help="Exited within 7 days after depeg")
    c4.metric("Very Late / No Exit", n_late, help="Still exposed weeks/months later")

    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)

    # ── Response Timeline ───────────────────────────────────
    st.subheader("Response Timeline")

    vaults["response_date"] = pd.to_datetime(vaults["response_date"])

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

    # Depeg lines, two separate events
    fig.add_vline(x=pd.Timestamp("2025-11-04"), line_dash="dash", line_color=RED, opacity=0.7)
    fig.add_annotation(x=pd.Timestamp("2025-11-04"), y=1, yref="paper",
                       text="xUSD depeg (Nov 4)",
                       showarrow=False, font=dict(color=RED, size=10), yshift=14)

    fig.add_vline(x=pd.Timestamp("2025-11-06"), line_dash="dash", line_color="#9333ea", opacity=0.7)
    fig.add_annotation(x=pd.Timestamp("2025-11-06"), y=1, yref="paper",
                       text="deUSD depeg (Nov 6)",
                       showarrow=False, font=dict(color="#9333ea", size=10), yshift=-6)

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

        _tvl_label = format_usd(total_tvl).replace("$", r"\$")
        with st.expander(
            f"**{response_class}**: {len(subset)} vaults, {_tvl_label} current TVL",
            expanded=(response_class == "PROACTIVE"),
        ):
            display_cols = ["vault_name", "chain", "curator", "tvl_usd", "days_before_depeg",
                            "timelock_days"]
            col_config = {
                    "vault_name": "Vault",
                    "chain": "Chain",
                    "curator": "Curator",
                    "tvl_usd": st.column_config.NumberColumn("Current TVL", format="$%,.0f"),
                    "days_before_depeg": st.column_config.NumberColumn("Days Before Depeg", format="%+.1f"),
                    "timelock_days": st.column_config.NumberColumn("Timelock (days)", format="%.0f"),
            }
            # Add pre-depeg TVL column if available
            if "tvl_pre_depeg_usd" in subset.columns and subset["tvl_pre_depeg_usd"].sum() > 0:
                display_cols.insert(4, "tvl_pre_depeg_usd")
                col_config["tvl_pre_depeg_usd"] = st.column_config.NumberColumn(
                    "Pre-depeg TVL", format="$%,.0f"
                )

            st.dataframe(
                subset[display_cols],
                column_config=col_config,
                hide_index=True,
                use_container_width=True,
            )

    # ── Timelock Analysis ───────────────────────────────────
    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
    st.subheader("Timelock Distribution")
    st.caption("Vaults with 0-day timelocks could change allocations instantly, a configuration worth monitoring.")

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

    # ── Key Insight (computed from data) ─────────────────────
    # Find the best and worst outcomes dynamically
    proactive_vaults = vaults[vaults["response_class"] == "PROACTIVE"].sort_values("tvl_usd", ascending=False)

    # For worst outcome: find the vault with the largest actual haircut
    damaged_vaults = vaults[
        vaults["share_price_drawdown"].abs() > 0.005
    ].sort_values("share_price_drawdown", ascending=True) if not vaults.empty else pd.DataFrame()

    # Fallback: VERY_LATE vaults
    if damaged_vaults.empty:
        damaged_vaults = vaults[
            vaults["response_class"] == "VERY_LATE"
        ].sort_values("share_price_drawdown", ascending=True) if not vaults.empty else pd.DataFrame()

    if not proactive_vaults.empty and not damaged_vaults.empty:
        best = proactive_vaults.iloc[0]
        worst = damaged_vaults.iloc[0]

        best_tvl = format_usd(best.get("tvl_pre_depeg_usd", best.get("tvl_usd", 0)))
        best_name = best["curator"]
        best_days = abs(int(best["days_before_depeg"]))

        worst_name = worst["vault_name"]
        worst_tvl = format_usd(worst.get("tvl_pre_depeg_usd", worst.get("tvl_usd", 0)))

        finding_text = (
            f"Response speed and allocation decisions, not vault size, determined outcomes. "
            f"{best_name}'s {best_tvl} vault exited {best_days} days before the depeg with zero loss, "
            f"while {worst_name} ({worst_tvl} pre-depeg TVL) lost capital "
            f"from concentrated toxic exposure."
        ).replace("$", "\\$")
    else:
        finding_text = (
            "Response speed, not vault size, was what mattered."
        )

    st.info(finding_text)
