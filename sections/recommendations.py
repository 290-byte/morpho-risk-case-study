"""Section 10: Recommendations: Proposed improvements for each problem uncovered."""

import streamlit as st
from utils.data_loader import load_markets, load_vaults
from utils.charts import format_usd


def render():
    st.title("Recommendations")
    st.caption(
        "Five improvements, each tied to a specific failure from the analysis."
    )

    # Load data for dynamic references
    markets = load_markets()
    vaults = load_vaults()

    n_hardcoded = 0
    if not markets.empty and "oracle_is_hardcoded" in markets.columns:
        n_hardcoded = int(markets["oracle_is_hardcoded"].sum())

    n_instant_tl = 0
    if not vaults.empty and "timelock_days" in vaults.columns:
        n_instant_tl = len(vaults[vaults["timelock_days"] == 0])
        n_total_vaults = len(vaults)
    else:
        n_total_vaults = 0

    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)

    # ═══════════════════════════════════════════════════════════
    # PROBLEM 1: ORACLE MASKING
    # ═══════════════════════════════════════════════════════════
    st.subheader("1. Oracle Masking")

    col_f, col_s = st.columns(2)
    with col_f:
        with st.container(border=True):
            st.markdown("**The Problem**")
            st.markdown(
                "Chainlink oracle adapters continued reporting xUSD and sdeUSD at "
                "approximately **\\$1.00** while market prices had collapsed to "
                "**\\$0.05-\\$0.30**. "
                "Because liquidation depends entirely on the oracle price, "
                "**zero meaningful liquidations fired**, and borrowers sat with "
                "worthless collateral and no mechanism to recover lender funds."
            )
            if n_hardcoded > 0:
                st.caption(
                    f"Data: {n_hardcoded} market{'s' if n_hardcoded > 1 else ''} "
                    f"used hardcoded oracle prices. See Liquidation Failure page."
                )
    with col_s:
        with st.container(border=True):
            st.markdown("**Proposed Solution: Oracle Circuit Breakers**")
            st.markdown(
                "Implement **deviation thresholds** that flag or pause markets when the "
                "oracle price diverges >5% from on-chain TWAP or DEX spot prices. "
                "This would have detected xUSD's collapse within minutes."
            )
            st.caption(
                "The Steakhouse MetaOracle Deviation Timelock "
                "(proposed June 2025, Cantina-audited) already provides this capability "
                "but was not adopted by affected markets. "
                "Priority: HIGH."
            )

    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)

    # ═══════════════════════════════════════════════════════════
    # PROBLEM 2: TIMELOCK CONFIGURATION
    # ═══════════════════════════════════════════════════════════
    st.subheader("2. Timelock Configuration")

    col_f, col_s = st.columns(2)
    with col_f:
        with st.container(border=True):
            st.markdown("**The Problem**")
            st.markdown(
                "Timelocks are designed to prevent reckless allocation changes, but "
                "during a crisis they also delay emergency exits. Vaults with "
                "**3-day timelocks** (e.g. MEV Capital) could not remove toxic markets "
                "quickly, trapping depositor funds while collateral continued to decline. "
                "Meanwhile, **0-day timelocks** offer no governance protection against "
                "unchecked allocation changes. Neither setting addresses the need "
                "for rapid response during genuine market stress."
            )
            if n_instant_tl > 0:
                st.caption(
                    f"Data: {n_instant_tl} of {n_total_vaults} vaults had 0-day timelocks. "
                    f"See Curator Response page."
                )
    with col_s:
        with st.container(border=True):
            st.markdown("**Proposed Solution: Timelocks + Emergency Bypass**")
            st.markdown(
                "Not about making timelocks longer or shorter. "
                "Pair standard timelocks with **emergency circuit breakers** that can "
                "bypass the delay when oracle deviations exceed defined thresholds. "
                "This preserves the governance protection of timelocks while enabling "
                "rapid response during genuine crises."
            )
            st.caption(
                "Morpho V2's `forceDeallocate` mechanism moves in this direction "
                "but was deployed after the affected markets were created. "
                "Priority: HIGH."
            )

    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)

    # ═══════════════════════════════════════════════════════════
    # PROBLEM 3: LIQUIDITY CONTAGION
    # ═══════════════════════════════════════════════════════════
    st.subheader("3. Liquidity Contagion")

    col_f, col_s = st.columns(2)
    with col_f:
        with st.container(border=True):
            st.markdown("**The Problem**")
            st.markdown(
                "Gauntlet's vaults had **zero toxic exposure**: they exited weeks before "
                "the depeg. Yet their depositors experienced **near-zero withdrawable "
                "liquidity for ~6 hours** on November 4. When toxic vaults pulled liquidity "
                "to service panic withdrawals, they drained shared underlying markets "
                "that clean vaults also relied on."
            )
            st.markdown(
                "**The system self-corrected.** Morpho's AdaptiveCurveIRM pushed borrow rates "
                "4x when utilization hit 100%, incentivizing repayment. Liquidity returned within "
                "hours, not weeks. Gauntlet's Balanced vault gained 35% in TVL over the following "
                "week as depositors fled risky curators. No bad debt spread to clean vaults."
            )
            st.caption(
                "Data: See Liquidity Stress and Contagion Assessment pages. "
                "An arxiv paper (Dec 2025) formalized this: "
                "\"Isolation applies primarily to credit rather than liquidity risk.\""
            )
    with col_s:
        with st.container(border=True):
            st.markdown("**Assessment**")
            st.markdown(
                "Per-vault liquidity reserves are theoretically possible but come with a direct "
                "yield cost: idle capital earning nothing reduces returns for all depositors. In "
                "practice, the rate model already functions as a dynamic reserve mechanism. When "
                "liquidity disappears, rates spike, making borrowing expensive and attracting "
                "new supply. The 6-hour window is the cost of this self-correction."
            )
            st.markdown(
                "The actionable fix is **disclosure, not reserves**. Depositors in Gauntlet's "
                "vaults had no way to see that their vault shared underlying markets with toxic "
                "vaults. Surfacing cross-vault market overlap in the vault interface would let "
                "depositors price liquidity risk before entering a position."
            )
            st.caption(
                "Gauntlet's 6-hour illiquidity was real but temporary. The rate model "
                "resolved it without intervention. The deeper issue is that depositors "
                "could not anticipate it. "
                "Priority: MEDIUM."
            )

    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)

    # ═══════════════════════════════════════════════════════════
    # PROBLEM 4: HIDDEN LOSSES (V1.1)
    # ═══════════════════════════════════════════════════════════
    st.subheader("4. Hidden Losses in V1.1 Vaults")

    col_f, col_s = st.columns(2)
    with col_f:
        with st.container(border=True):
            st.markdown("**The Problem**")
            st.markdown(
                "MetaMorpho V1.1 vaults do **not** auto-realize bad debt in the share price. "
                "The MEV Capital USDC (Arbitrum) vault's share price **kept rising** while "
                "carrying unrealized losses. Only the massive TVL drop (from \\$50M to \\$652K) "
                "revealed the problem. Depositors had no way to see the hidden risk."
            )
            st.caption(
                "Data: See Bad Debt Analysis, V1.1 Vault Mechanics section. "
                "Losses accrue in a `lostAssets` variable, invisible to depositors."
            )
    with col_s:
        with st.container(border=True):
            st.markdown("**Proposed Solution: Mandatory Disclosure**")
            st.markdown(
                "Require explicit **disclosure in the vault UI** when vaults carry "
                "unrealized bad debt from toxic markets. Surface the `lostAssets` "
                "variable or equivalent so depositors can see the true state of the vault, "
                "not just the share price."
            )
            st.caption(
                "The V1.1 design is mechanically sound: deferring losses allows orderly "
                "resolution. But transparency is essential for depositor trust. "
                "Priority: HIGH."
            )

    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)

    # ═══════════════════════════════════════════════════════════
    # PROBLEM 5: VAULT EXPOSURE OPACITY
    # ═══════════════════════════════════════════════════════════
    st.subheader("5. Vault Exposure Opacity")

    col_f, col_s = st.columns(2)
    with col_f:
        with st.container(border=True):
            st.markdown("**The Problem**")
            st.markdown(
                "Determining which underlying markets a vault is exposed to, and "
                "which other vaults share those same markets, requires querying the "
                "Morpho API directly or reading on-chain data. This information is "
                "not surfaced in the standard vault interface. During the November event, "
                "7 vaults held both toxic and clean market positions. Depositors in those "
                "vaults had no straightforward way to assess this cross-exposure before "
                "or during the crisis."
            )
            st.caption(
                "Data: See Contagion Assessment, bridge network visualization."
            )
    with col_s:
        with st.container(border=True):
            st.markdown("**Proposed Solution: Cross-Exposure Disclosure**")
            st.markdown(
                "Surface **vault-to-market allocation data** in the vault UI, including "
                "which other vaults share the same underlying markets. A shared-market "
                "indicator or overlap metric would allow depositors to evaluate indirect "
                "liquidity risk as part of their due diligence."
            )
            st.caption(
                "The data exists on-chain and via the GraphQL API, but accessing it "
                "requires technical effort beyond what most depositors undertake. "
                "Priority: MEDIUM."
            )

    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)

    # ── Summary ──────────────────────────────────────────────
    st.subheader("Summary")

    st.markdown(
        "The November 2025 event revealed that Morpho's **market-level isolation works "
        "as designed for credit risk**: only directly exposed vaults suffered bad debt. "
        "The vulnerabilities lie in the layers above: oracles that can misreport, curators "
        "that can be slow, vaults that can hide losses, and shared liquidity across vaults."
    )

    import pandas as pd
    summary = pd.DataFrame([
        {"Problem": "Oracle Masking", "Root Cause": "Hardcoded oracle prices",
         "Solution": "Oracle circuit breakers (deviation thresholds)", "Priority": "HIGH",
         "Status": "Steakhouse MetaOracle proposed (June 2025)"},
        {"Problem": "Timelock Configuration", "Root Cause": "No emergency bypass for timelocked allocation changes",
         "Solution": "Timelocks + emergency bypass on oracle deviation", "Priority": "HIGH",
         "Status": "V2 forceDeallocate partially addresses"},
        {"Problem": "Liquidity Contagion", "Root Cause": "Shared underlying markets across vaults",
         "Solution": "Disclosure of cross-vault market overlap; rate model self-corrects in hours", "Priority": "MEDIUM",
         "Status": "Rate model worked; disclosure not yet surfaced in UI"},
        {"Problem": "V1.1 Hidden Losses", "Root Cause": "Bad debt deferred, not shown in share price",
         "Solution": "Mandatory lostAssets disclosure in vault UI", "Priority": "HIGH",
         "Status": "No current disclosure"},
        {"Problem": "Vault Exposure Opacity", "Root Cause": "Cross-vault market overlap not surfaced in UI",
         "Solution": "Cross-exposure disclosure in vault interface", "Priority": "MEDIUM",
         "Status": "Data available via API, not shown in UI"},
    ])

    st.dataframe(
        summary,
        column_config={
            "Problem": st.column_config.TextColumn("Problem", width="medium"),
            "Root Cause": st.column_config.TextColumn("Root Cause", width="medium"),
            "Solution": st.column_config.TextColumn("Proposed Solution", width="large"),
            "Priority": st.column_config.TextColumn("Priority", width="small"),
            "Status": st.column_config.TextColumn("Current Status", width="medium"),
        },
        hide_index=True,
        use_container_width=True,
    )

    st.caption(
        "*These recommendations are based on the findings of this analysis and are intended "
        "as discussion points for the case study debrief. They reflect the analyst's assessment "
        "of priorities based on the November 2025 data.*"
    )
