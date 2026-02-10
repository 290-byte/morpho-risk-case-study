"""Section 3: Bad Debt Analysis — $3.64M quantification and share price impacts."""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from utils.data_loader import load_markets, load_vaults, load_share_prices
from utils.charts import apply_layout, depeg_vline, RED, GREEN, BLUE, YELLOW, ORANGE, format_usd


def render():
    st.title("Bad Debt Analysis")
    st.caption("\\$3.64M in unrealized bad debt across 4 markets — plus share price damage to 2 vaults.")

    markets = load_markets()
    vaults = load_vaults()
    prices = load_share_prices()

    if markets.empty:
        st.error("⚠️ Market data not available — run the pipeline to generate `block1_markets_graphql.csv`.")
        return

    # ── Key Metrics ─────────────────────────────────────────
    total_bad_debt = markets["bad_debt_usd"].sum()
    markets_with_debt = len(markets[markets["bad_debt_usd"] > 0])
    
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Bad Debt", f"${total_bad_debt:,.0f}", delta=f"-${total_bad_debt:,.0f}", delta_color="inverse")
    c2.metric("Markets with Bad Debt", f"{markets_with_debt} / {len(markets)}")
    c3.metric("Largest Single Market", "$3.64M", help="xUSD/USDC on Arbitrum")
    c4.metric("Realized Bad Debt", "$8,947", help="Only a tiny fraction has been formally realized")

    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)

    # ── Bad Debt Waterfall ──────────────────────────────────
    st.subheader("Bad Debt by Market")

    bad_markets = markets[markets["bad_debt_usd"] > 0].sort_values("bad_debt_usd", ascending=False)
    if not bad_markets.empty:
        fig = go.Figure(go.Waterfall(
            name="Bad Debt",
            orientation="v",
            x=bad_markets["market_label"].tolist() + ["Total"],
            y=bad_markets["bad_debt_usd"].tolist() + [0],
            measure=["relative"] * len(bad_markets) + ["total"],
            text=[format_usd(v) for v in bad_markets["bad_debt_usd"]] + [format_usd(total_bad_debt)],
            textposition="outside",
            connector=dict(line=dict(color="rgba(99,102,241,0.3)")),
            increasing=dict(marker=dict(color=RED)),
            totals=dict(marker=dict(color=ORANGE)),
        ))
        fig = apply_layout(fig, height=380)
        fig.update_yaxes(tickformat="$,.0f")
        st.plotly_chart(fig, use_container_width=True)

    # ── Share Price Impact ──────────────────────────────────
    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
    st.subheader("Share Price Impact — Bad Debt Socialization")

    tab1, tab2 = st.tabs(["Damaged Vaults", "Stable Vaults"])

    with tab1:
        # Compute haircut from daily prices, grouped by vault_address to avoid
        # mixing cross-chain data (block2 daily has both ETH + ARB rows for same name)
        damaged_info = []
        if prices.empty:
            st.error("⚠️ Share price data not available — run the pipeline to generate `block2_share_prices_daily.csv`.")
        else:
            # Determine grouping key: vault_address (unique per chain) > vault_name
            group_key = "vault_address" if "vault_address" in prices.columns else "vault_name"

            for gid, vp in prices.groupby(group_key):
                vp = vp.sort_values("date")
                vault_name = vp["vault_name"].iloc[0] if "vault_name" in vp.columns else str(gid)
                chain = vp["chain"].iloc[0] if "chain" in vp.columns else ""

                # Running-max drawdown (cummax method — same as block3b)
                cummax = vp["share_price"].cummax()
                dd_series = (vp["share_price"] - cummax) / cummax
                max_dd = dd_series.min()

                if max_dd < -0.01:  # > 1% drawdown
                    dd_idx = dd_series.idxmin()
                    peak_idx = cummax[:dd_idx + 1].idxmax() if dd_idx is not None else vp.index[0]
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
                    })

        # Deduplicate: if same vault_name appears on multiple chains, keep the one
        # with the largest drawdown (that's the one that actually got damaged)
        if damaged_info:
            df_dam = pd.DataFrame(damaged_info)
            df_dam = df_dam.sort_values("haircut").drop_duplicates("vault_name", keep="first")
            damaged_info = df_dam.to_dict("records")

        if damaged_info:
            st.markdown(
                f"**{len(damaged_info)} vault{'s' if len(damaged_info) > 1 else ''}** suffered share price drops "
                "after the depeg, meaning depositors bore losses from bad debt socialization."
            )

            for di in damaged_info:
                # Find matching vault for curator/TVL
                base_name = di["vault_name"].split(" (")[0]
                vmatch = vaults[vaults["vault_name"] == base_name] if not vaults.empty else pd.DataFrame()
                if vmatch.empty:
                    vmatch = vaults[vaults["vault_name"].str.startswith(base_name)] if not vaults.empty else pd.DataFrame()
                if not vmatch.empty:
                    row = vmatch.iloc[0]
                    curator = row.get("curator", "—")
                    tvl = row.get("tvl_usd", 0)
                else:
                    curator = "—"
                    tvl = 0

                with st.container(border=True):
                    col1, col2, col3, col4, col5 = st.columns(5)
                    display_name = di["vault_name"]
                    if di["chain"] and f"({di['chain'][:3]})" not in display_name:
                        display_name += f" ({di['chain'][:3].title()})"
                    col1.metric("Vault", display_name)
                    col2.metric("Curator", curator)
                    col3.metric("Haircut", f"{di['haircut']:.1%}",
                                delta=f"{di['haircut']:.1%}", delta_color="inverse")
                    col4.metric("Current Price", f"${di['last']:.4f}")
                    col5.metric("TVL", f"${tvl:,.0f}" if tvl > 0 else "—")

            # Chart per damaged vault from daily price data
            group_key = "vault_address" if "vault_address" in prices.columns else "vault_name"
            for di in damaged_info:
                vdata = prices[prices[group_key] == di["group_key"]].sort_values("date").copy()
                if vdata.empty:
                    continue

                fig = go.Figure()
                line_color = RED if di["haircut"] < -0.5 else BLUE
                fig.add_trace(go.Scatter(
                    x=vdata["date"], y=vdata["share_price"],
                    mode="lines",
                    line=dict(color=line_color, width=2, shape="spline", smoothing=0.8),
                    hovertemplate="%{x}<br>$%{y:.4f}<extra></extra>",
                    connectgaps=False, showlegend=False,
                ))

                chart_title = f"{di['vault_name']} — {di['haircut']:.1%} haircut"
                fig = apply_layout(fig, title=chart_title, height=320, show_legend=False)
                fig = depeg_vline(fig)
                fig.update_yaxes(tickformat="$.4f", title="")
                fig.update_xaxes(title="")

                # Annotate peak and trough
                fig.add_annotation(
                    x=di["peak_date"], y=di["peak"],
                    text=f"Peak ${di['peak']:.4f}", showarrow=True,
                    arrowhead=2, ax=-50, ay=-25,
                    font=dict(size=10, color="#6B7280"),
                )
                fig.add_annotation(
                    x=di["trough_date"], y=di["trough"],
                    text=f"Trough ${di['trough']:.4f} ({di['haircut']:.1%})",
                    showarrow=True, arrowhead=2, ax=60, ay=-25,
                    font=dict(size=10, color=RED),
                )

                st.plotly_chart(fig, use_container_width=True)
        else:
            if not prices.empty:
                st.info("No vaults with significant share price drawdowns (>1%) detected in the data.")
            # If prices is empty, the error message was already shown above

    with tab2:
        st.markdown("These vaults maintained stable share prices — proactive curator exits protected depositors.")

        damaged_names = {d["vault_name"].split(" (")[0] for d in damaged_info} if damaged_info else set()
        stable = vaults[~vaults["vault_name"].isin(damaged_names)].sort_values("tvl_usd", ascending=False) if not vaults.empty else pd.DataFrame()
        if not stable.empty:
            st.dataframe(
                stable[["vault_name", "curator", "tvl_usd", "share_price", "response_class"]],
                column_config={
                    "vault_name": "Vault",
                    "curator": "Curator",
                    "tvl_usd": st.column_config.NumberColumn("TVL", format="$%,.0f"),
                    "share_price": st.column_config.NumberColumn("Share Price", format="%.6f"),
                    "response_class": "Response",
                },
                hide_index=True,
                use_container_width=True,
            )

        # Chart: stable vaults share price
        if not prices.empty:
            stable_names = stable["vault_name"].head(4).tolist() if not stable.empty else []
            mask = prices["vault_name"].isin(stable_names)
            if mask.any():
                fig = px.line(prices[mask], x="date", y="share_price", color="vault_name")
                fig = apply_layout(fig, title="Share Price — Protected Vaults", height=350)
                fig = depeg_vline(fig)
                fig.update_yaxes(tickformat="$.4f", title="")
                fig.update_xaxes(title="")
                fig.update_traces(connectgaps=False)
                st.plotly_chart(fig, use_container_width=True)

    # ── Three-Layer Analysis ────────────────────────────────
    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
    st.subheader("Three-Layer Bad Debt Analysis")

    col1, col2, col3 = st.columns(3)

    with col1:
        with st.container(border=True):
            st.markdown("**Layer 1: Supply-Borrow Gap**")
            st.metric("Markets with gap < 0", "0")
            st.caption("Oracle-independent check. No markets have borrowing exceeding supply at face value.")

    with col2:
        with st.container(border=True):
            st.markdown("**Layer 2: Protocol Bad Debt**")
            st.metric("Total Unrealized", f"${total_bad_debt:,.0f}")
            st.caption("Morpho's internal accounting shows bad debt that hasn't been formally realized.")

    with col3:
        with st.container(border=True):
            st.markdown("**Layer 3: Oracle vs Spot**")
            st.metric("Markets Mispriced (>5%)", "0 detected")
            st.caption("Paradoxically zero — because oracles are hardcoded at ≈\\$1.00, masking the real gap.")
