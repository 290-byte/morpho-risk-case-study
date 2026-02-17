"""Section 4: Curator Response: How curators reacted to toxic exposure."""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from utils.data_loader import load_vaults
from utils.charts import fmt_usd_cols,  apply_layout, RESPONSE_COLORS, RED, GREEN, BLUE, YELLOW, format_usd


def render():
    st.title("Curator Response Analysis")

    vaults = load_vaults()
    if vaults.empty:
        st.error("Vault data not available. Run the pipeline to generate `block1_vaults_graphql.csv`.")
        return

    # Filter dust/test vaults: must have had >$10K TVL or allocation at any point
    MIN_TVL = 10_000
    _tvl_cols = [c for c in ["tvl_usd", "tvl_pre_depeg_usd", "tvl_at_peak_usd", "peak_allocation"] if c in vaults.columns]
    _max_tvl = vaults[_tvl_cols].fillna(0).max(axis=1) if _tvl_cols else pd.Series(0, index=vaults.index)
    vaults = vaults[_max_tvl >= MIN_TVL].copy()

    # ── What we measured ──────────────────────────────────────
    st.markdown(
        "**What this page measures.** "
        "Each vault that allocated to a toxic collateral market (xUSD, deUSD, sdeUSD) "
        "is classified by *when* it reduced that allocation below 50% of its peak. "
        "This matters because once a market hits 100% utilization after the depeg, "
        "no further withdrawals are possible. Capital is trapped."
    )
    st.markdown(
        "**Pre-depeg actions** (Proactive, Early Reactor) represent actual capital recovered: "
        "the curator moved USDC out of the toxic market while liquidity was still available. "
        "Depositors in these vaults kept their money. "
        "**Post-depeg actions** (Slow Reactor, administrative cap-zeroing, force-removal) "
        "did not recover capital. They are accounting steps: setting supply caps to zero "
        "prevents new inflows, and force-removal crystallizes the loss in the vault's share price "
        "(V1.0) or defers it into a hidden variable (V1.1). The trapped USDC remains in the market either way."
    )

    # ── Dynamic caption from data ────────────────────────────
    response_counts = vaults["response_class"].value_counts()
    n_proactive = response_counts.get("PROACTIVE", 0)
    n_early = response_counts.get("EARLY_REACTOR", 0)
    n_slow = response_counts.get("SLOW_REACTOR", 0) + response_counts.get("DURING_DEPEG", 0)
    n_late = response_counts.get("VERY_LATE", 0) + response_counts.get("STAYED_EXPOSED", 0)
    n_exited_unknown = response_counts.get("EXITED_TIMING_UNKNOWN", 0)
    n_total = len(vaults)

    caption_parts = []
    if n_proactive:
        caption_parts.append(f"{n_proactive} reduced exposure >7 days before depeg (capital saved)")
    if n_early:
        caption_parts.append(f"{n_early} reduced exposure 1-7 days before (capital saved)")
    if n_slow:
        caption_parts.append(f"{n_slow} acted during or after depeg (administrative only)")
    if n_late:
        caption_parts.append(f"{n_late} maintained exposure")
    if n_exited_unknown:
        caption_parts.append(f"{n_exited_unknown} exited at unknown timing")

    st.caption(
        f"{n_total} vaults analyzed: {', '.join(caption_parts)}."
    )

    # ── Key Metrics ─────────────────────────────────────────
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Proactive", n_proactive, help="Allocation dropped >50% of peak more than 7 days before depeg. Capital was actually withdrawn.")
    c2.metric("Early Reactor", n_early, help="Allocation dropped >50% of peak 1-7 days before depeg. Capital was actually withdrawn.")
    c3.metric("Slow / During Depeg", n_slow, help="Acted during or after depeg. By this point, markets were at 100% utilization. Actions were administrative (cap to zero, force-remove), not capital recovery.")
    c4.metric("Late / Stayed Exposed", n_late, help="Still exposed weeks later or never removed allocation.")

    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)

    # ── Response Timeline ───────────────────────────────────
    st.subheader("Response Timeline")

    vaults["response_date"] = pd.to_datetime(vaults["response_date"])

    # Only plot vaults with a known response date
    vaults_with_date = vaults[vaults["response_date"].notna()].copy()

    fig = go.Figure()

    for _, v in vaults_with_date.iterrows():
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

    n_no_date = len(vaults) - len(vaults_with_date)
    if n_no_date > 0:
        st.caption(
            f"{n_no_date} vault(s) not shown: exited but exact timing could not be "
            f"determined from allocation or admin event data."
        )

    # ── Response Matrix ─────────────────────────────────────
    st.subheader("Response Classification")
    st.caption(
        "Classification is based on the earliest date a vault's toxic allocation dropped "
        "below 50% of its peak, or was fully zeroed, or had its supply cap set to 0. "
        "\"Days Before Depeg\" is relative to November 4, 2025. Positive = before depeg (capital saved), "
        "negative or zero = during/after (administrative action only)."
    )

    for response_class in ["PROACTIVE", "EARLY_REACTOR", "DURING_DEPEG",
                           "SLOW_REACTOR", "VERY_LATE", "STAYED_EXPOSED",
                           "EXITED_TIMING_UNKNOWN"]:
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
                    "tvl_usd": "Current TVL (USD)",
                    "days_before_depeg": st.column_config.NumberColumn("Days Before Depeg", format="%+.1f"),
                    "timelock_days": st.column_config.NumberColumn("Timelock (days)", format="%.0f"),
            }
            # Add pre-depeg TVL column if available
            if "tvl_pre_depeg_usd" in subset.columns and subset["tvl_pre_depeg_usd"].sum() > 0:
                display_cols.insert(4, "tvl_pre_depeg_usd")
                col_config["tvl_pre_depeg_usd"] = "Pre-depeg TVL (USD)"

            st.dataframe(
                fmt_usd_cols(subset[display_cols]),
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
    proactive_vaults = vaults[vaults["response_class"].isin(["PROACTIVE", "EARLY_REACTOR"])].sort_values("tvl_usd", ascending=False)

    # For worst outcome: find the vault with the largest actual haircut
    damaged_vaults = vaults[
        vaults["share_price_drawdown"].abs() > 0.005
    ].sort_values("share_price_drawdown", ascending=True) if not vaults.empty else pd.DataFrame()

    # Fallback: VERY_LATE vaults
    if damaged_vaults.empty:
        damaged_vaults = vaults[
            vaults["response_class"].isin(["VERY_LATE", "STAYED_EXPOSED"])
        ].sort_values("share_price_drawdown", ascending=True) if not vaults.empty else pd.DataFrame()

    n_pre_depeg = len(proactive_vaults)
    n_damaged = len(damaged_vaults)

    if not proactive_vaults.empty and not damaged_vaults.empty:
        worst = damaged_vaults.iloc[0]
        worst_name = worst["vault_name"]

        finding_text = (
            f"{n_pre_depeg} of {n_total} vaults reduced their toxic allocation before the depeg, "
            f"while liquidity was still available. These vaults preserved depositor capital. "
            f"The {n_damaged} vault(s) that did not exit in time (including {worst_name}) "
            f"had their capital trapped at 100% utilization. Post-depeg actions were administrative only."
        ).replace("$", "\\$")
    else:
        finding_text = (
            f"{n_pre_depeg} of {n_total} vaults reduced their toxic allocation before the depeg."
        )

    st.info(finding_text)
