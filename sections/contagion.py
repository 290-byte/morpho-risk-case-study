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

    # Pre-compute bridge display data (used in both Exposure Distribution and Bridge cards)
    display_bridges_early = actual_bridges if not actual_bridges.empty else bridges

    # ── Exposure Distribution ───────────────────────────────
    st.subheader("Exposure Distribution")

    col1, col2 = st.columns(2)

    with col1:
        # Show toxic vs clean exposure per bridge vault as stacked horizontal bars
        if not display_bridges_early.empty:
            vault_names = []
            toxic_vals = []
            clean_vals = []
            for _, b in display_bridges_early.iterrows():
                vault_names.append(b.get("vault_name", "Unknown"))
                toxic_vals.append(float(b.get("toxic_exposure_usd", b.get("toxic_supply_usd", 0)) or 0))
                clean_vals.append(float(b.get("clean_exposure_usd", b.get("clean_supply_usd", 0)) or 0))

            fig = go.Figure()
            fig.add_trace(go.Bar(
                y=vault_names, x=toxic_vals, name="Toxic Exposure",
                orientation="h", marker_color=RED,
                text=[format_usd(v) for v in toxic_vals], textposition="inside",
            ))
            fig.add_trace(go.Bar(
                y=vault_names, x=clean_vals, name="Clean Exposure",
                orientation="h", marker_color=GREEN,
                text=[format_usd(v) for v in clean_vals], textposition="inside",
            ))
            fig = apply_layout(fig, height=max(250, 80 * len(vault_names)))
            fig.update_layout(barmode="stack", xaxis_title="Exposure (USD)", yaxis_title="",
                              xaxis_tickformat="$,.0f",
                              legend=dict(orientation="h", yanchor="bottom", y=1.02))
            st.plotly_chart(fig, use_container_width=True)
        elif not exposure.empty:
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

    display_bridges = display_bridges_early

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
        st.caption(
            "**Note:** Appearing here means the vault had toxic *exposure* (the risk existed), not necessarily "
            "realized loss. Some bridge vaults exited toxic positions before bad debt accrued. "
            "See the Bad Debt Analysis page for vaults where losses actually materialized in share prices."
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

    # ── Key Finding: Credit vs. Liquidity Contagion ──────────
    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
    st.subheader("Key Finding: Credit Risk Isolated, Liquidity Risk Propagated")

    col_a, col_b = st.columns(2)

    with col_a:
        with st.container(border=True):
            st.markdown("**✅ Credit Risk — Contained**")
            st.markdown(
                "Bad debt stayed within specific vaults. The vast majority of public vaults had "
                "zero permanent capital loss. Morpho's isolated market architecture "
                "prevented bad debt from spreading to unrelated depositors."
            )
            st.caption("Only 2 vaults showed permanent share price haircuts in on-chain data: Relend USDC (Eth, -98.4%), "
                       "MEV Capital USDC (Eth, -3.5%). A third vault (MEV Capital USDC, Arb) used V1.1 mechanics "
                       "that mask bad debt — see Bad Debt Analysis for the anomaly detection.")

    with col_b:
        with st.container(border=True):
            st.markdown("**⚠️ Liquidity Risk — Propagated**")
            st.markdown(
                "Gauntlet's Balanced and Frontier vaults had *zero* toxic exposure but "
                "experienced near-zero withdrawable liquidity for ~6 hours on Nov 4. "
                "When toxic vaults pulled liquidity to service panic withdrawals, they "
                "drained shared underlying markets that clean vaults also relied on."
            )
            st.caption("Source: Gauntlet November 2025 market risk report. "
                       "Stani Kulechov (Aave): \"One curator's stress becomes everyone's problem.\"")

    st.info(
        "**The nuanced answer to Q2:** Markets ARE isolated at the protocol level for credit risk. "
        "But curators create shared liquidity risk through overlapping market allocations. "
        "Multiple vaults supplying to the same underlying Morpho market means one vault's panic "
        "withdrawal reduces available liquidity for all other vaults in that market. "
        "An arxiv paper (Dec 2025) formalized this: *\"Isolation applies primarily to credit "
        "rather than liquidity risk.\"*"
    )

    # ── Q2 Framework ────────────────────────────────────────
    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
    st.subheader("Q2: How Could Morpho Be More Resilient?")

    recs = [
        ("Oracle Circuit Breakers",
         "Implement deviation thresholds that flag or pause markets when oracle price "
         "diverges >5% from on-chain TWAP/DEX prices. The Steakhouse MetaOracle governance "
         "proposal already moves in this direction.",
         "HIGH"),
        ("Timelocks — Double-Edged Sword",
         "Timelocks (e.g., MEV Capital's 3-day timelock) were designed to prevent "
         "unchecked allocation changes, but during the crisis they DELAYED removal of "
         "toxic markets, trapping depositor funds for days. 0-day timelock vaults (Relend) "
         "suffered instant, catastrophic loss. The optimal answer isn't simply 'longer' or "
         "'shorter' — it's timelocks paired with emergency circuit breakers that can bypass "
         "the delay when oracle deviations exceed thresholds.",
         "NUANCED"),
        ("Liquidity Isolation Mechanisms",
         "Address the liquidity contagion vector: rate-limit withdrawals from shared markets "
         "during stress, or implement per-vault liquidity reserves to prevent one vault's "
         "panic from draining shared pools. Gauntlet's 6-hour illiquidity event shows this "
         "is a real, not theoretical, risk.",
         "HIGH"),
        ("Contagion Disclosure",
         "Surface cross-market exposure data in the vault UI so depositors can see which "
         "other vaults share their underlying markets. Currently invisible to most users.",
         "MEDIUM"),
        ("V1.1 Bad Debt Transparency",
         "V1.1 vaults mask bad debt by not auto-realizing it in share prices. As shown in the MEV Capital USDC (Arb) "
         "example, the share price can continue rising while the vault carries unrealized losses — "
         "only the massive TVL drop reveals the problem. Require explicit disclosure when vaults "
         "carry unrealized bad debt from toxic markets.",
         "HIGH"),
    ]

    for title, desc, priority in recs:
        with st.container(border=True):
            priority_colors = {"HIGH": "🔴", "MEDIUM": "🟡", "NUANCED": "🟠"}
            st.markdown(f"**{priority_colors.get(priority, '')} {title}**")
            st.caption(desc)


def _render_bridge_network(bridges: pd.DataFrame, n_toxic_markets: int):
    """Network cluster diagram: Toxic Markets ← Bridge Vaults → Clean Markets."""

    n = len(bridges)
    if n == 0:
        st.info("No bridge vaults detected.")
        return

    fig = go.Figure()

    # Layout: 3 columns — toxic nodes left (x=0), vaults center (x=1), clean nodes right (x=2)
    # Spread vaults vertically
    vault_ys = [(i / max(n - 1, 1)) for i in range(n)] if n > 1 else [0.5]

    # -- Toxic market node (single cluster, left) --
    fig.add_trace(go.Scatter(
        x=[0], y=[0.5], mode="markers+text",
        marker=dict(size=40, color=RED, line=dict(color="#1e293b", width=2)),
        text=[f"Toxic Markets<br>({n_toxic_markets})"],
        textposition="middle left", textfont=dict(size=12, color=RED, family="Inter"),
        hoverinfo="text", hovertext=f"{n_toxic_markets} toxic collateral markets (xUSD, deUSD, sdeUSD)",
        showlegend=False,
    ))

    for i, (_, b) in enumerate(bridges.iterrows()):
        vault_name = b.get("vault_name", f"Vault {i+1}")
        toxic_exp = float(b.get("toxic_exposure_usd", b.get("toxic_supply_usd", 0)) or 0)
        clean_mkts = int(b.get("clean_markets", b.get("n_clean_markets", 0)) or 0)
        clean_exp = float(b.get("clean_exposure_usd", b.get("clean_supply_usd", 0)) or 0)
        vy = vault_ys[i]

        # -- Red line: Toxic → Vault --
        line_width = max(1.5, min(8, toxic_exp / 500_000))  # scale by exposure
        fig.add_trace(go.Scatter(
            x=[0.08, 0.92], y=[0.5, vy], mode="lines",
            line=dict(color="rgba(239,68,68,0.45)", width=line_width),
            hoverinfo="skip", showlegend=False,
        ))

        # -- Vault node (center) --
        vault_size = max(20, min(45, 20 + toxic_exp / 200_000))
        fig.add_trace(go.Scatter(
            x=[1], y=[vy], mode="markers+text",
            marker=dict(size=vault_size, color=ORANGE, line=dict(color="#1e293b", width=1.5)),
            text=[vault_name],
            textposition="top center", textfont=dict(size=11, color="#1e293b", family="Inter"),
            hoverinfo="text",
            hovertext=f"<b>{vault_name}</b><br>Toxic exposure: ${toxic_exp:,.0f}<br>Clean markets: {clean_mkts}<br>Clean exposure: ${clean_exp:,.0f}",
            showlegend=False,
        ))

        # -- Green line: Vault → Clean cluster --
        green_width = max(1, min(6, clean_exp / 500_000)) if clean_exp > 0 else 1
        fig.add_trace(go.Scatter(
            x=[1.08, 1.92], y=[vy, vy], mode="lines",
            line=dict(color="rgba(34,197,94,0.45)", width=green_width),
            hoverinfo="skip", showlegend=False,
        ))

        # -- Clean market node (right) --
        clean_size = max(16, min(35, 16 + clean_mkts * 2))
        fig.add_trace(go.Scatter(
            x=[2], y=[vy], mode="markers+text",
            marker=dict(size=clean_size, color=GREEN, line=dict(color="#1e293b", width=1)),
            text=[f"{clean_mkts} clean"],
            textposition="middle right", textfont=dict(size=10, color=GREEN, family="Inter"),
            hoverinfo="text",
            hovertext=f"{clean_mkts} clean markets · ${clean_exp:,.0f} exposure",
            showlegend=False,
        ))

    fig = apply_layout(fig, height=max(350, 130 * n))
    fig.update_layout(
        xaxis=dict(visible=False, range=[-0.4, 2.7]),
        yaxis=dict(visible=False, range=[-0.15, 1.15]),
        margin=dict(l=10, r=10, t=10, b=10),
        plot_bgcolor="rgba(0,0,0,0)",
    )

    st.plotly_chart(fig, use_container_width=True)
    st.caption(
        "**Reading the network:** Red lines connect toxic collateral markets (left) to bridge vaults (center). "
        "Green lines show those same vaults also serving clean markets (right). Node size reflects exposure amount. "
        "Depositors in the clean markets unknowingly share the vault's pooled accounting with toxic positions."
    )
