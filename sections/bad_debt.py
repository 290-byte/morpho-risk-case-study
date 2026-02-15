"""Section 3: Bad Debt Analysis: quantification and share price impacts."""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from utils.data_loader import load_markets, load_vaults, load_share_prices, load_bad_debt_detail
from utils.charts import apply_layout, depeg_vline, RED, GREEN, BLUE, YELLOW, ORANGE, format_usd


def md_usd(value):
    """format_usd but with escaped $ for safe use in st.markdown / st.caption."""
    return format_usd(value).replace("$", r"\$")


def render():
    st.title("Bad Debt Analysis")

    markets = load_markets()
    vaults = load_vaults()
    prices = load_share_prices()

    if markets.empty:
        st.error("⚠️ Market data not available. Run the pipeline to generate `block1_markets_graphql.csv`.")
        return

    # ── Key Metrics ─────────────────────────────────────────
    # Separate public and private
    public_markets = markets[~markets.get("is_private_market", False)] if "is_private_market" in markets.columns else markets
    private_markets = markets[markets.get("is_private_market", False)] if "is_private_market" in markets.columns else pd.DataFrame()

    total_bad_debt = public_markets["bad_debt_usd"].sum()
    markets_with_debt = len(public_markets[public_markets["bad_debt_usd"] > 0])
    private_capital_lost = private_markets["original_capital_lost"].sum() if not private_markets.empty and "original_capital_lost" in private_markets.columns else 0

    st.caption(
        f"{format_usd(total_bad_debt)} in public-market bad debt across {markets_with_debt} market{'s' if markets_with_debt != 1 else ''}, "
        f"plus ~{format_usd(private_capital_lost)} lost in a private Elixir-Stream market on Plume.".replace("$", "\\$")
    )
    largest_market_debt = public_markets["bad_debt_usd"].max() if not public_markets.empty else 0
    largest_market_label = public_markets.loc[public_markets["bad_debt_usd"].idxmax(), "market_label"] if largest_market_debt > 0 else "-"

    # Realized bad debt from detailed data if available
    bd_detail_for_metrics = load_bad_debt_detail()
    if not bd_detail_for_metrics.empty and "L2_realized_bad_debt_usd" in bd_detail_for_metrics.columns:
        realized_bad_debt = bd_detail_for_metrics["L2_realized_bad_debt_usd"].sum()
    elif "realized_bad_debt_usd" in markets.columns:
        realized_bad_debt = markets["realized_bad_debt_usd"].sum()
    else:
        realized_bad_debt = 0

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Public Bad Debt", f"${total_bad_debt:,.0f}", delta=f"-${total_bad_debt:,.0f}", delta_color="inverse")
    c2.metric("Markets with Bad Debt", f"{markets_with_debt} / {len(public_markets)}")
    c3.metric("Largest Public Market", format_usd(largest_market_debt), help=largest_market_label)
    c4.metric("Private Market Loss", format_usd(private_capital_lost) if private_capital_lost > 0 else "-",
              help="~$68M in xUSD collateral lost at depeg (nominal position now inflated by interest)")

    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)

    # ── Bad Debt Waterfall ──────────────────────────────────
    st.subheader("Bad Debt by Market")

    bad_markets = public_markets[public_markets["bad_debt_usd"] > 0].sort_values("bad_debt_usd", ascending=False)
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

    # Private market context
    if not private_markets.empty and private_capital_lost > 0:
        _pm_supply = private_markets["supply_usd"].sum()
        _pm_bd = private_markets["bad_debt_usd"].sum()
        st.warning(
            f"**Private market (Plume, not shown above).** "
            f"An unlisted xUSD/USDC market on Plume lost roughly "
            f"**~{format_usd(private_capital_lost)}** in xUSD collateral at the time of the depeg. "
            f"Interest has since inflated the nominal position to {format_usd(_pm_supply)} supply "
            f"and {format_usd(_pm_bd)} in recorded bad debt, but this is purely accrual on a "
            f"frozen, unrecoverable position. The figure that matters is the original "
            f"~68M USD lost when the collateral became worthless."
        )

    # ── Share Price Impact ──────────────────────────────────
    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
    st.subheader("Share Price Impact: Bad Debt Socialization")

    tab1, tab2 = st.tabs(["Damaged Vaults", "Stable Vaults"])

    with tab1:
        damaged_info = []
        if prices.empty:
            st.error("⚠️ Share price data not available. Run the pipeline to generate `block2_share_prices_daily.csv`.")
        else:
            group_key = "vault_address" if "vault_address" in prices.columns else "vault_name"

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

        if damaged_info:
            df_dam = pd.DataFrame(damaged_info)
            df_dam = df_dam.sort_values("haircut").drop_duplicates("vault_name", keep="first")
            damaged_info = df_dam.to_dict("records")

        # ── V1.1 Example Vault (MEV Capital USDC, Arb) ────────
        # We look up MEV Capital USDC on Arbitrum as the known V1.1 example
        if not vaults.empty:
            mev_arb_mask = (
                vaults["vault_name"].str.contains("MEV Capital", case=False, na=False) &
                vaults["vault_name"].str.contains("USDC", case=False, na=False) &
                vaults.get("chain", pd.Series(dtype=str)).str.contains("arb", case=False, na=False)
            )
            if mev_arb_mask.any():
                v = vaults[mev_arb_mask].iloc[0]
                v11_example = {
                    "vault_name": v.get("vault_name", "MEV Capital USDC"),
                    "chain": v.get("chain", "arbitrum"),
                    "group_key": str(v.get("vault_address", "")).lower(),
                    "share_price": float(v.get("share_price", 1.0)),
                    "drawdown": abs(float(v.get("share_price_drawdown", 0))),
                    "tvl_pre_depeg": float(v.get("tvl_pre_depeg_usd", v.get("tvl_at_peak_usd", 0))),
                    "tvl_now": float(v.get("tvl_usd", 0)),
                    "collateral": str(v.get("collateral", "xUSD")),
                    "curator": v.get("curator", "MEV Capital"),
                }
                tvl_pd = v11_example["tvl_pre_depeg"]
                tvl_now = v11_example["tvl_now"]
                v11_example["tvl_drop_pct"] = (tvl_pd - tvl_now) / tvl_pd if tvl_pd > 0 else 0
            else:
                v11_example = None
        else:
            v11_example = None

        # ── Render Damaged Vaults (share price drop > 1%) ────
        if damaged_info:
            st.markdown(
                f"**{len(damaged_info)} vault{'s' if len(damaged_info) > 1 else ''}** suffered permanent share price losses "
                "where depositors bore losses from bad debt socialization."
            )

            for di in damaged_info:
                base_name = di["vault_name"].split(" (")[0]
                # Match by name AND chain to avoid cross-chain confusion
                # (e.g. "MEV Capital USDC" exists on both Ethereum and Arbitrum)
                chain_val = di.get("chain", "")
                if not vaults.empty and chain_val:
                    vmatch = vaults[
                        vaults["vault_name"].str.startswith(base_name) &
                        vaults.get("chain", pd.Series(dtype=str)).str.contains(
                            chain_val[:3], case=False, na=False
                        )
                    ]
                    if vmatch.empty:
                        # Fallback: name-only match
                        vmatch = vaults[vaults["vault_name"].str.startswith(base_name)]
                elif not vaults.empty:
                    vmatch = vaults[vaults["vault_name"].str.startswith(base_name)]
                else:
                    vmatch = pd.DataFrame()
                if not vmatch.empty:
                    row = vmatch.iloc[0]
                    curator = row.get("curator", "-")
                    tvl = row.get("tvl_usd", 0)
                else:
                    curator = "-"
                    tvl = 0

                chain_short = di["chain"][:3].title() if di["chain"] else ""
                display_name = di["vault_name"]
                if chain_short and f"({chain_short})" not in display_name:
                    display_name += f" ({chain_short})"

                with st.container(border=True):
                    # Determine pre-depeg TVL for context
                    tvl_pre = row.get("tvl_pre_depeg_usd", 0) if not vmatch.empty else 0
                    tvl_label = f"TVL (current): {md_usd(tvl)}" if tvl > 0 else ""
                    tvl_pre_label = f" · Pre-depeg TVL: {md_usd(tvl_pre)}" if tvl_pre > 0 and tvl_pre != tvl else ""

                    st.markdown(
                        f"**{display_name}** · Curator: {curator} · "
                        f"Current price: \\${di['last']:.4f}"
                        + (f" · {tvl_label}{tvl_pre_label}" if tvl_label else "")
                    )

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

                chart_title = f"{di['vault_name']} : Share Price"
                fig = apply_layout(fig, title=chart_title, height=320, show_legend=False)
                fig = depeg_vline(fig)
                fig.update_yaxes(tickformat="$.4f", title="")
                fig.update_xaxes(title="")

                fig.add_annotation(
                    x=di["peak_date"], y=di["peak"],
                    text=f"Peak ${di['peak']:.4f}", showarrow=True,
                    arrowhead=2, ax=-50, ay=-25,
                    font=dict(size=10, color="#6B7280"),
                )
                fig.add_annotation(
                    x=di["trough_date"], y=di["trough"],
                    text=f"Trough ${di['trough']:.4f}",
                    showarrow=True, arrowhead=2, ax=60, ay=-25,
                    font=dict(size=10, color=RED),
                )

                st.plotly_chart(fig, use_container_width=True)
        else:
            if not prices.empty:
                st.info("No vaults with significant share price drawdowns (>1%) detected in the data.")

        # ── V1.1 Mechanics Explanation ──────────────────────
        st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
        st.markdown("### V1.1 Vault Mechanics: Hidden Bad Debt")
        st.markdown(
            "MetaMorpho V1.1 vaults do **not** auto-realize bad debt in the share price. "
            "V1.0 vaults (e.g. Relend USDC) socialize losses directly into the share price, "
            "but V1.1 vaults continue compounding yield from healthy markets even when one of their "
            "underlying positions carries unrealized losses. The result: "
            "the share price keeps rising while the vault carries hidden bad debt."
        )
        st.markdown(
            "Depositors who notice the problem typically withdraw, causing a massive TVL drop, "
            "but the share price chart alone does not reveal the loss."
        )

        if v11_example is not None:
            chain_short = v11_example["chain"][:3].title() if v11_example["chain"] else ""
            display_name = v11_example["vault_name"]
            if chain_short and f"({chain_short})" not in display_name:
                display_name += f" ({chain_short})"

            st.markdown(f"**Example: {display_name}**")
            with st.container(border=True):
                st.markdown(
                    f"**{display_name}** · Curator: {v11_example['curator']} · "
                    f"Collateral: {v11_example['collateral']}"
                )
                c1, c2, c3, c4 = st.columns(4)
                c1.metric("Share Price", f"${v11_example['share_price']:.4f}",
                          help="V1.1: does not reflect bad debt")
                c2.metric("Share Price Drawdown", f"{v11_example['drawdown']:.2%}",
                          help="Near-zero because V1.1 masks the loss")
                c3.metric("Pre-depeg TVL", format_usd(v11_example["tvl_pre_depeg"]))
                c4.metric("Current TVL", format_usd(v11_example["tvl_now"]),
                          delta=f"-{v11_example['tvl_drop_pct']:.0%}", delta_color="inverse")

                st.caption(
                    f"TVL collapsed from {md_usd(v11_example['tvl_pre_depeg'])} to "
                    f"{md_usd(v11_example['tvl_now'])} (-{v11_example['tvl_drop_pct']:.0%}) "
                    f"while share price continued rising, which is characteristic V1.1 behavior. "
                    f"The reported estimated loss for this vault is ~\\$628K (per MEV Capital post-mortem). "
                    f"Source: on-chain share price data + vault allocation history."
                )

            # Plot the V1.1 vault share price if we have data
            gk = "vault_address" if "vault_address" in prices.columns else "vault_name"
            vdata = prices[prices[gk] == v11_example["group_key"]].sort_values("date").copy()
            if vdata.empty:
                mask = prices["vault_name"].str.startswith(v11_example["vault_name"].split(" (")[0])
                if "chain" in prices.columns:
                    mask = mask & prices["chain"].str.lower().str.contains(v11_example["chain"][:3].lower(), na=False)
                vdata = prices[mask].sort_values("date").copy()

            if not vdata.empty:
                fig = go.Figure()
                fig.add_trace(go.Scatter(
                    x=vdata["date"], y=vdata["share_price"],
                    mode="lines",
                    line=dict(color=ORANGE, width=2, shape="spline", smoothing=0.8),
                    hovertemplate="%{x}<br>$%{y:.4f}<extra></extra>",
                    connectgaps=False, showlegend=False,
                ))
                fig = apply_layout(fig, title=f"{display_name} : V1.1 share price (bad debt hidden)", height=320, show_legend=False)
                fig = depeg_vline(fig)
                fig.update_yaxes(tickformat="$.4f", title="")
                fig.update_xaxes(title="")

                depeg_mask = vdata["date"] <= "2025-11-04"
                if depeg_mask.any():
                    depeg_price = vdata[depeg_mask]["share_price"].iloc[-1]
                    fig.add_annotation(
                        x="2025-11-04", y=depeg_price,
                        text="xUSD depeg date:<br>share price unaffected<br>(V1.1 masking)",
                        showarrow=True, arrowhead=2, arrowcolor=RED,
                        ax=80, ay=-40,
                        font=dict(size=10, color=RED),
                    )
                st.plotly_chart(fig, use_container_width=True)

    with tab2:
        st.markdown("These vaults maintained stable share prices. Proactive curator exits protected depositors.")

        damaged_names = {d["vault_name"].split(" (")[0] for d in damaged_info} if damaged_info else set()
        stable = vaults[~vaults["vault_name"].isin(damaged_names)].sort_values("tvl_usd", ascending=False) if not vaults.empty else pd.DataFrame()
        if not stable.empty:
            st.dataframe(
                stable[["vault_name", "curator", "tvl_usd", "share_price", "response_class"]],
                column_config={
                    "vault_name": "Vault",
                    "curator": "Curator",
                    "tvl_usd": st.column_config.NumberColumn("TVL", format="$%,.0f"),
                    "share_price": st.column_config.NumberColumn("Share Price", format="%.4f"),
                    "response_class": "Response",
                },
                hide_index=True,
                use_container_width=True,
            )

        if not prices.empty:
            stable_names = stable["vault_name"].head(4).tolist() if not stable.empty else []
            mask = prices["vault_name"].isin(stable_names)
            if mask.any():
                fig = px.line(prices[mask], x="date", y="share_price", color="vault_name")
                fig = apply_layout(fig, title="Share Price: Protected Vaults", height=350)
                fig = depeg_vline(fig)
                fig.update_yaxes(tickformat="$.4f", title="")
                fig.update_xaxes(title="")
                fig.update_traces(connectgaps=False)
                st.plotly_chart(fig, use_container_width=True)

    # ── Three-Layer Analysis ────────────────────────────────
    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
    st.subheader("Three-Layer Bad Debt Analysis")

    # Load detailed bad debt data for Layer 1 and Layer 3 computations
    bd_detail = load_bad_debt_detail()

    # Compute Layer 1 from data
    if not bd_detail.empty and "L1_has_bad_debt" in bd_detail.columns:
        l1_count = int(bd_detail["L1_has_bad_debt"].sum())
    elif not bd_detail.empty and "L1_gap_usd" in bd_detail.columns:
        l1_count = int((bd_detail["L1_gap_usd"] < 0).sum())
    else:
        l1_count = 0

    # Compute Layer 2 from markets data (already loaded)
    l2_total = total_bad_debt  # computed above from markets["bad_debt_usd"].sum()

    # Compute Layer 3 from data
    if not bd_detail.empty and "L3_oracle_spot_gap_pct" in bd_detail.columns:
        l3_mispriced = int((bd_detail["L3_oracle_spot_gap_pct"].abs() > 0.05).sum())
    else:
        l3_mispriced = 0

    # Count hardcoded oracles from data
    n_hardcoded_oracles = 0
    if not bd_detail.empty and "oracle_is_hardcoded" in bd_detail.columns:
        n_hardcoded_oracles = int(bd_detail["oracle_is_hardcoded"].sum())

    col1, col2, col3 = st.columns(3)

    with col1:
        with st.container(border=True):
            st.markdown("**Layer 1: Supply-Borrow Gap**")
            st.metric("Markets with gap < 0", str(l1_count))
            st.caption("Oracle-independent check. Counts markets where borrowing exceeds supply at face value.")

    with col2:
        with st.container(border=True):
            st.markdown("**Layer 2: Protocol Bad Debt**")
            st.metric("Total Unrealized", f"${l2_total:,.0f}")
            st.caption("Morpho's internal accounting shows bad debt that hasn't been formally realized.")

    with col3:
        with st.container(border=True):
            st.markdown("**Layer 3: Oracle vs Spot**")
            st.metric("Markets Mispriced (>5%)", f"{l3_mispriced} detected")
            if n_hardcoded_oracles > 0:
                st.caption(
                    f"Paradoxically zero. {n_hardcoded_oracles} oracle{'s are' if n_hardcoded_oracles > 1 else ' is'} "
                    f"hardcoded at ≈\\$1.00, masking the real gap between oracle price and collapsed spot value."
                )
            else:
                st.caption("Compares oracle prices to spot market prices for collateral assets.")
