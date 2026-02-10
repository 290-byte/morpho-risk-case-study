"""Section 7: Contagion Assessment — Cross-market exposure and contagion bridges."""

import math
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from utils.data_loader import load_bridges, load_exposure_summary, load_vaults, load_csv
from utils.charts import apply_layout, donut_chart, RED, BLUE, ORANGE, GREEN, YELLOW, format_usd


def render():
    st.title("Contagion Assessment")

    bridges = load_bridges()
    exposure = load_exposure_summary()   # categorised: Single / Multi / High / Bridge
    vaults = load_vaults()
    exposure_raw = load_csv("block6_vault_market_exposure.csv")  # one row per vault-market pair
    markets_gql = load_csv("block1_markets_graphql.csv")

    if bridges.empty and exposure.empty and exposure_raw.empty:
        st.error("⚠️ Data not available — run the pipeline to generate block6 CSVs.")
        return

    # ── Compute metrics from data ──────────────────────────
    total_exposures = len(exposure_raw) if not exposure_raw.empty else 0
    n_toxic_markets = len(markets_gql) if not markets_gql.empty else 0

    # From exposure summary (vault-level categories)
    cat_counts = {}
    if not exposure.empty:
        cat_counts = dict(zip(exposure["category"], exposure["count"]))

    n_single = cat_counts.get("Single Market (1)", 0)
    n_multi = cat_counts.get("Multi-Market (2)", 0)
    n_high = cat_counts.get("High Risk (3+)", 0)
    n_bridge = cat_counts.get("Contagion Bridge", 0)
    total_vaults = n_single + n_multi + n_high + n_bridge

    # Count actual BRIDGE type from bridges CSV
    if not bridges.empty:
        bp_col = "bridge_type" if "bridge_type" in bridges.columns else "contagion_path"
        if bp_col in bridges.columns:
            actual_bridges = bridges[bridges[bp_col] == "BRIDGE"]
        else:
            actual_bridges = bridges
        n_bridge_actual = len(actual_bridges)
    else:
        n_bridge_actual = n_bridge
        actual_bridges = pd.DataFrame()

    n_multi_market = n_multi + n_high + n_bridge  # vaults with >=2 toxic markets or bridges

    st.caption(
        f"{total_exposures} vault-market exposure pairs, {total_vaults} vaults analysed, "
        f"and {n_bridge_actual} contagion bridges where toxic and clean markets share the same vault."
    )

    # ── Key Metrics ─────────────────────────────────────────
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Exposures", f"{total_exposures:,}", help="Vault-market pairs touching toxic collateral")
    c2.metric("Multi-Market Vaults", f"{n_multi_market}", help="Vaults exposed to ≥2 toxic markets")
    c3.metric("High-Risk (≥3 mkts)", f"{n_high}", help="Vaults with concentrated multi-market risk")
    c4.metric("Contagion Bridges", f"{n_bridge_actual}", help="Vaults bridging toxic ↔ clean markets")

    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)

    # ── Exposure Distribution ───────────────────────────────
    st.subheader("Exposure Distribution")

    col1, col2 = st.columns(2)

    with col1:
        if not exposure.empty and len(exposure) > 1:
            fig = donut_chart(
                exposure["count"].tolist(),
                exposure["category"].tolist(),
                colors=[BLUE, ORANGE, RED, "#991b1b"][:len(exposure)],
                height=350,
            )
            st.plotly_chart(fig, use_container_width=True)
        elif not exposure.empty:
            # Only one category — bar chart is more useful than a 100% donut
            fig = go.Figure(go.Bar(
                x=exposure["category"],
                y=exposure["count"],
                marker_color=[BLUE, ORANGE, RED, "#991b1b"][:len(exposure)],
                text=exposure["count"],
                textposition="outside",
            ))
            fig = apply_layout(fig, height=350)
            fig.update_layout(yaxis_title="Vaults", xaxis_title="")
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        # Dynamic narrative
        parts = ["**The contagion funnel:**\n"]

        if n_single > 0:
            parts.append(
                f"Most vaults ({n_single}) only touched a single toxic market — "
                f"limited blast radius."
            )
        if n_multi > 0 or n_high > 0:
            multi_total = n_multi + n_high
            parts.append(
                f"But {multi_total} vault{'s' if multi_total != 1 else ''} had exposure to "
                f"multiple toxic markets simultaneously"
                + (f", and {n_high} {'were' if n_high != 1 else 'was'} exposed to 3 or more "
                   f"markets, creating concentrated risk." if n_high > 0 else ".")
            )
        if n_bridge_actual > 0:
            parts.append(
                f"\nMost critically, **{n_bridge_actual} vault{'s' if n_bridge_actual != 1 else ''} "
                f"act{'s' if n_bridge_actual == 1 else ''} as contagion bridges** — they hold "
                f"both toxic and clean market positions, meaning depositors in \"safe\" markets "
                f"unknowingly share exposure to toxic collateral through the vault's pooled accounting."
            )

        if not parts[1:]:
            parts.append("No multi-market or bridge exposure detected in the current dataset.")

        st.markdown(" ".join(parts))

    # ── Contagion Bridges ───────────────────────────────────
    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
    st.subheader("Contagion Bridges — Toxic ↔ Clean")

    display_bridges = actual_bridges if not actual_bridges.empty else bridges

    if not display_bridges.empty:
        for _, b in display_bridges.iterrows():
            toxic_mkts = b.get("toxic_markets", b.get("n_toxic_markets", "?"))
            toxic_exp = b.get("toxic_exposure_usd", b.get("toxic_supply_usd", 0))
            clean_mkts = b.get("clean_markets", b.get("n_clean_markets", "?"))
            clean_exp = b.get("clean_exposure_usd", b.get("clean_supply_usd", 0))

            with st.container(border=True):
                cols = st.columns([2, 1, 1, 1, 1])
                cols[0].metric("Vault", b.get("vault_name", "Unknown"))
                cols[1].metric("Toxic Mkts", str(toxic_mkts))
                cols[2].metric("Toxic $", format_usd(toxic_exp))
                cols[3].metric("Clean Mkts", str(clean_mkts))
                cols[4].metric("Clean $", format_usd(clean_exp))

        st.warning(
            "**Risk:** Depositors who supplied to these vaults thinking they were only exposed to "
            "clean, safe markets actually shared losses from the toxic market positions. "
            "The vault's share price socializes gains AND losses across all depositors."
        )
    else:
        st.info("No contagion bridges detected — all vaults had purely toxic exposure.")

    # ── Bridge Network Visualization ────────────────────────
    if not display_bridges.empty and len(display_bridges) > 0:
        st.subheader("Bridge Network")
        _render_bridge_network(display_bridges, n_toxic_markets)

    # ── Vault Exposure Table ────────────────────────────────
    if not exposure_raw.empty:
        st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
        st.subheader("Vault-Market Exposure Detail")

        display_cols = []
        preferred = ["vault_name", "primary_chain", "n_toxic_markets", "toxic_markets",
                      "collateral_types", "total_supply_usd", "risk_class"]
        for c in preferred:
            if c in exposure_raw.columns:
                display_cols.append(c)

        if display_cols:
            df_show = exposure_raw[display_cols].copy()
            if "total_supply_usd" in df_show.columns:
                df_show = df_show.sort_values("total_supply_usd", ascending=False)
            st.dataframe(df_show, use_container_width=True, hide_index=True)

    # ── Q2 Framework ────────────────────────────────────────
    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
    st.subheader("Q2: How Could Morpho Be More Resilient?")

    recs = [
        ("Oracle Circuit Breakers",
         "Implement deviation thresholds that flag or pause markets when oracle price "
         "diverges >5% from on-chain TWAP/DEX prices."),
        ("Automated Exposure Caps",
         "Protocol-level limits on single-collateral concentration across vaults, "
         "triggering automatic cap reductions."),
        ("Mandatory Timelocks",
         "Require minimum timelock periods (e.g., 24h) for all vaults to prevent instant, "
         "unchecked allocation changes."),
        ("Contagion Disclosure",
         "Surface cross-market exposure data in the vault UI so depositors understand "
         "their actual risk profile."),
        ("Stress Testing Framework",
         "Regular simulated depeg scenarios to validate that liquidation mechanisms "
         "would actually trigger."),
    ]

    for title, desc in recs:
        with st.container(border=True):
            st.markdown(f"**{title}**")
            st.caption(desc)


def _render_bridge_network(bridges: pd.DataFrame, n_toxic_markets: int):
    """Improved bridge network: horizontal flow layout with clear labels."""
    fig = go.Figure()

    n = len(bridges)
    y_positions = list(range(n))
    y_center = (n - 1) / 2

    # Central toxic node (left)
    fig.add_trace(go.Scatter(
        x=[0], y=[y_center], mode="markers+text",
        marker=dict(size=50, color=RED, line=dict(width=2, color="#7f1d1d")),
        text=[f"<b>Toxic Markets</b><br>({n_toxic_markets})"],
        textposition="middle center",
        textfont=dict(size=10, color="white"),
        showlegend=False, hoverinfo="skip",
    ))

    for i, (_, b) in enumerate(bridges.iterrows()):
        vault_name = b.get("vault_name", "?")
        toxic_exp = b.get("toxic_exposure_usd", b.get("toxic_supply_usd", 0))
        clean_mkts = b.get("clean_markets", b.get("n_clean_markets", 0))
        clean_exp = b.get("clean_exposure_usd", b.get("clean_supply_usd", 0))

        y = y_positions[i]
        vx = 3    # vault x
        cx = 5.5  # clean x

        # Edge: toxic → vault (red dashed)
        fig.add_trace(go.Scatter(
            x=[0.5, vx - 0.3], y=[y_center + (y - y_center) * 0.3, y],
            mode="lines",
            line=dict(color=RED, width=2, dash="dot"),
            showlegend=False, hoverinfo="skip",
        ))

        # Vault node (orange, sized by exposure)
        size_factor = max(22, min(45, 22 + (toxic_exp / 500_000)))
        fig.add_trace(go.Scatter(
            x=[vx], y=[y], mode="markers+text",
            marker=dict(size=size_factor, color=ORANGE,
                        line=dict(width=1.5, color="#92400e")),
            text=[f"<b>{vault_name}</b><br>{format_usd(toxic_exp)}"],
            textposition="top center",
            textfont=dict(size=9),
            showlegend=False,
            hovertemplate=(
                f"<b>{vault_name}</b><br>"
                f"Toxic exposure: {format_usd(toxic_exp)}<br>"
                f"Clean exposure: {format_usd(clean_exp)}<extra></extra>"
            ),
        ))

        # Edge: vault → clean (green)
        fig.add_trace(go.Scatter(
            x=[vx + 0.3, cx - 0.2], y=[y, y],
            mode="lines",
            line=dict(color=GREEN, width=1.5),
            showlegend=False, hoverinfo="skip",
        ))

        # Clean node (green)
        fig.add_trace(go.Scatter(
            x=[cx], y=[y], mode="markers+text",
            marker=dict(size=24, color=GREEN,
                        line=dict(width=1, color="#166534")),
            text=[f"Clean<br>({clean_mkts})"],
            textposition="middle center",
            textfont=dict(size=8, color="white"),
            showlegend=False,
            hovertemplate=f"{clean_mkts} clean markets<br>{format_usd(clean_exp)} clean exposure<extra></extra>",
        ))

    fig = apply_layout(fig, height=max(350, 150 * n))
    fig.update_xaxes(visible=False, range=[-1.5, 7])
    fig.update_yaxes(visible=False, range=[-1, n])
    fig.update_layout(
        annotations=[
            dict(x=0, y=-0.7, text="<b>Toxic Collateral</b>",
                 showarrow=False, font=dict(size=11, color=RED)),
            dict(x=3, y=-0.7, text="<b>Bridge Vaults</b>",
                 showarrow=False, font=dict(size=11, color=ORANGE)),
            dict(x=5.5, y=-0.7, text="<b>Clean Markets</b>",
                 showarrow=False, font=dict(size=11, color=GREEN)),
        ]
    )

    st.plotly_chart(fig, use_container_width=True)
