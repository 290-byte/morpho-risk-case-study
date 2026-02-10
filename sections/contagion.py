"""Section 7: Contagion Assessment — Cross-market exposure and contagion bridges."""

import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
from utils.data_loader import load_bridges, load_exposure_summary, load_vaults
from utils.charts import apply_layout, donut_chart, RED, BLUE, ORANGE, GREEN, YELLOW, format_usd


def render():
    st.title("Contagion Assessment")
    st.caption(
        "200 vault-market exposure pairs, 28 vaults with multi-market exposure, "
        "and 4 contagion bridges where toxic and clean markets share the same vault."
    )

    bridges = load_bridges()
    exposure = load_exposure_summary()
    vaults = load_vaults()

    # ── Key Metrics ─────────────────────────────────────────
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Exposures", "200", help="Vault-market pairs touching toxic collateral")
    c2.metric("Multi-Market Vaults", "28", help="Vaults exposed to ≥2 toxic markets")
    c3.metric("High-Risk (≥3 mkts)", "8", help="Vaults with concentrated multi-market risk")
    c4.metric("Contagion Bridges", "4", help="Vaults bridging toxic ↔ clean markets")

    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)

    # ── Exposure Distribution ───────────────────────────────
    st.subheader("Exposure Distribution")

    col1, col2 = st.columns(2)

    with col1:
        if not exposure.empty:
            fig = donut_chart(
                exposure["count"].tolist(),
                exposure["category"].tolist(),
                colors=[BLUE, ORANGE, RED, "#991b1b"],
                height=350,
            )
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.markdown("""
        **The contagion funnel:**

        Most vaults (172) only touched a single toxic market — limited blast radius.
        But 28 vaults had exposure to multiple toxic markets simultaneously, 
        and 8 vaults were exposed to 3 or more markets, creating concentrated risk.

        Most critically, **4 vaults act as contagion bridges** — they hold both toxic 
        and clean market positions, meaning depositors in "safe" markets unknowingly 
        share exposure to toxic collateral through the vault's pooled accounting.
        """)

    # ── Contagion Bridges ───────────────────────────────────
    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
    st.subheader("Contagion Bridges — Toxic ↔ Clean")

    if not bridges.empty:
        for _, b in bridges.iterrows():
            with st.container(border=True):
                col1, col2, col3, col4 = st.columns(4)
                col1.metric("Vault", b["vault_name"])
                col2.metric("Toxic Markets", b["toxic_markets"])
                col3.metric("Toxic Exposure", format_usd(b["toxic_exposure_usd"]))
                col4.metric("Clean Markets", b["clean_markets"])

        st.warning(
            "**Risk:** Depositors who supplied to these vaults thinking they were only exposed to "
            "clean, safe markets actually shared losses from the toxic market positions. "
            "The vault's share price socializes gains AND losses across all depositors."
        )

    # ── Bridge Network Visualization ────────────────────────
    st.subheader("Bridge Network")

    if not bridges.empty:
        fig = go.Figure()
        
        # Central toxic node
        fig.add_trace(go.Scatter(
            x=[0], y=[0], mode="markers+text",
            marker=dict(size=40, color=RED),
            text=["Toxic Markets<br>(18)"],
            textposition="middle center",
            textfont=dict(size=9, color="white"),
            showlegend=False,
        ))

        # Bridge vault nodes arranged in circle
        import math
        n = len(bridges)
        for i, (_, b) in enumerate(bridges.iterrows()):
            angle = 2 * math.pi * i / n
            x = 2 * math.cos(angle)
            y = 2 * math.sin(angle)

            # Vault node
            fig.add_trace(go.Scatter(
                x=[x], y=[y], mode="markers+text",
                marker=dict(size=25, color=ORANGE),
                text=[f"{b['vault_name']}<br>{format_usd(b['toxic_exposure_usd'])}"],
                textposition="top center",
                textfont=dict(size=8),
                showlegend=False,
            ))

            # Edge to toxic
            fig.add_trace(go.Scatter(
                x=[0, x], y=[0, y], mode="lines",
                line=dict(color=RED, width=2, dash="dot"),
                showlegend=False,
            ))

            # Clean market nodes
            cx = x + 1.2 * math.cos(angle)
            cy = y + 1.2 * math.sin(angle)
            fig.add_trace(go.Scatter(
                x=[cx], y=[cy], mode="markers+text",
                marker=dict(size=15, color=GREEN),
                text=[f"Clean ({b['clean_markets']})"],
                textposition="bottom center",
                textfont=dict(size=8, color=GREEN),
                showlegend=False,
            ))
            fig.add_trace(go.Scatter(
                x=[x, cx], y=[y, cy], mode="lines",
                line=dict(color=GREEN, width=1),
                showlegend=False,
            ))

        fig = apply_layout(fig, height=500)
        fig.update_xaxes(visible=False, range=[-4, 4])
        fig.update_yaxes(visible=False, range=[-4, 4], scaleanchor="x")
        st.plotly_chart(fig, use_container_width=True)

    # ── Q2 Framework ────────────────────────────────────────
    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
    st.subheader("Q2: How Could Morpho Be More Resilient?")

    recs = [
        ("Oracle Circuit Breakers", "Implement deviation thresholds that flag or pause markets when oracle price diverges >5% from on-chain TWAP/DEX prices."),
        ("Automated Exposure Caps", "Protocol-level limits on single-collateral concentration across vaults, triggering automatic cap reductions."),
        ("Mandatory Timelocks", "Require minimum timelock periods (e.g., 24h) for all vaults to prevent instant, unchecked allocation changes."),
        ("Contagion Disclosure", "Surface cross-market exposure data in the vault UI so depositors understand their actual risk profile."),
        ("Stress Testing Framework", "Regular simulated depeg scenarios to validate that liquidation mechanisms would actually trigger."),
    ]

    for title, desc in recs:
        with st.container(border=True):
            st.markdown(f"**{title}**")
            st.caption(desc)
