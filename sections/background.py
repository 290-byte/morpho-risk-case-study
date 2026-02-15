"""Section 0: Background: Context and timeline of the xUSD/deUSD depeg event."""

import streamlit as st


def render():
    st.title("Background: xUSD / deUSD Depeg Event")
    st.caption(
        "Context for the \\~\\$285M bad debt event across DeFi lending protocols, "
        "originating from the Stream Finance (xUSD) and Elixir (deUSD) interdependency."
    )

    # ── Key Entities ──────────────────────────────────────────
    st.subheader("Key Entities")

    col1, col2, col3 = st.columns(3)

    with col1:
        with st.container(border=True):
            st.markdown("**Stream Finance (xUSD)**")
            st.markdown(
                "Yield-bearing stablecoin offering 12–18% APY. "
                "Founded February 2024, raised \\$1.5M seed from "
                "[Polychain Capital](https://polychain.capital/). "
                "Reached approximately **\\$204M TVL** by late October 2025."
            )

    with col2:
        with st.container(border=True):
            st.markdown("**Elixir Network (deUSD)**")
            st.markdown(
                "Synthetic dollar protocol, a **separate project** from Stream Finance, "
                "with different team, governance, and smart contracts. "
                "However, Elixir lent **\\$68M USDC** to Stream "
                "(65% of deUSD's total backing) through private Morpho markets, "
                "creating a hidden dependency. "
                "Announced deUSD sunset on November 6, 2025."
            )

    with col3:
        with st.container(border=True):
            st.markdown("**Morpho**")
            st.markdown(
                "Permissionless lending protocol where both xUSD and deUSD "
                "were listed as collateral across 18 public markets and "
                "additional private markets. Isolated market architecture "
                "contained credit risk but did not prevent liquidity contagion."
            )

    st.caption(
        "**Individuals referenced in court filings:** Per *Stream Trading Corp. v. McMeans et al.* "
        "(Case No. 3:25-cv-10524, N.D. Cal.), Caleb McMeans assumed operational control of Stream "
        "in January 2025 and entrusted approximately \\$93M in off-chain assets to external fund "
        "manager Ryan DeMattia. The complaint alleges DeMattia misappropriated these funds to cover "
        "personal margin losses sustained during the October 10 ETH downturn."
    )

    st.info(
        "**Causal chain:** DeMattia fraud → Stream's backing stolen → **xUSD collapsed** (Nov 3–4). "
        "xUSD collapse → Elixir's \\$68M backing evaporated → **deUSD collapsed** (Nov 6–7). "
        "The Balancer V2 exploit (Nov 3) was a **separate, unrelated event** that created "
        "general DeFi panic but had no direct connection to either token."
    )

    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)

    # ── Cross-Collateral Architecture ─────────────────────────
    st.subheader("Cross-Collateral Architecture: xUSD ↔ deUSD")

    st.markdown(
        "The primary amplifying mechanism was a **recursive cross-minting loop** between "
        "xUSD and deUSD, first identified publicly by Yearn developer "
        "[Schlag](https://x.com/) on **October 28, 2025**, six days before the depeg."
    )

    with st.container(border=True):
        st.markdown("**The Minting Loop:**")
        st.markdown(
            "Stream received USDC → swapped to USDT → minted Elixir's deUSD "
            "→ used deUSD to borrow more USDC → minted more xUSD → **repeat**."
        )
        st.markdown(
            "Using approximately **\\$1.9M in USDC**, an estimated **\\$14.5M in xUSD** was created "
            "through these loops, producing a **7.6x capital amplification**."
        )

    st.markdown("")

    col1, col2 = st.columns(2)

    with col1:
        with st.container(border=True):
            st.markdown("**Elixir's Concentrated Exposure**")
            st.markdown(
                "Elixir lent **\\$68M USDC**, representing **65% of deUSD's total backing**, "
                "to Stream through a **private, unlisted Morpho market on Plume** where Stream was "
                "the sole borrower, using its own xUSD as collateral."
            )
            st.markdown(
                "This market was confirmed on-chain: 65.8M xUSD posted as collateral "
                "in an unlisted xUSD/USDC market (86% LLTV) on Plume. At pre-depeg "
                "prices (~\\$1.03), this matches the \\$68M figure from the lawsuit. "
                "The collateral is now worthless, and interest has inflated the nominal "
                "position far beyond the original capital."
            )
            st.caption(
                "Source: Morpho GraphQL API (raw underlying values); Dune Analytics; "
                "*Stream Trading Corp. v. McMeans*, Case No. 3:25-cv-10524"
            )

    with col2:
        with st.container(border=True):
            st.markdown("**Symmetric Failure Risk**")
            st.markdown(
                "Stream simultaneously held approximately **90% of remaining deUSD supply (\\~\\$75M)**. "
                "This created a bilateral dependency: a loss of confidence in xUSD would impair "
                "deUSD's backing, and vice versa, ensuring correlated failure of both assets."
            )
            st.caption(
                "Source: *Stream Trading Corp. v. McMeans*, "
                "Case No. 3:25-cv-10524; on-chain analysis"
            )

    st.markdown("")

    st.markdown(
        "On the same day, on-chain analyst **CBB0FE** independently estimated: "
        "*\"xUSD has \\~\\$170M backing it on-chain. They're borrowing \\~\\$530M from lending "
        "protocols. That's 4.1x leverage.\"*"
    )

    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)

    # ── Fund Loss Event ──────────────────────────────────────
    st.subheader("Fund Loss Event: October 10 – November 3")

    st.markdown(
        "On **October 10, 2025**, during a significant ETH downturn (approximately 21% decline, "
        "\\$20B in market-wide liquidations), the external fund manager's personal leveraged "
        "positions were liquidated. Per the lawsuit complaint, he subsequently used Stream "
        "Protocol assets to cover these losses. This was the beginning of an alleged "
        "misappropriation totaling approximately **\\$93 million**."
    )

    st.markdown(
        "By **November 2**, the fund manager reportedly admitted to Stream's operator that "
        "he had lost \"nearly all\" of the entrusted funds. The complaint further alleges that "
        "approximately \\$2.1M was transferred from Stream wallets to personal wallets via "
        "[Railgun](https://railgun.org/) (a privacy-preserving protocol)."
    )

    st.markdown(
        "On **November 3**, the [Balancer V2 exploit](https://rekt.news/) "
        "(\\$100–128M across 6+ chains) created broader DeFi market stress. "
        "This was a **completely separate event**: a smart contract rounding error, "
        "unrelated to Stream, xUSD, deUSD, or Morpho. The lawsuit timeline confirms "
        "the Stream fund losses occurred on November 2, the day prior. "
        "However, the coincidental timing amplified DeFi-wide panic and may have "
        "accelerated the bank run across lending protocols."
    )

    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)

    # ── Market Impact ────────────────────────────────────────
    st.subheader("Market Impact: November 3–8")

    st.markdown(
        "Stream announced the \\$93M loss on **November 3**, freezing deposits and withdrawals. "
        "xUSD repriced from \\$1.00 to approximately **\\$0.24–\\$0.30** by November 4. "
        "Critically, the Chainlink oracle adapter continued reporting xUSD at approximately "
        "\\$1.00–\\$1.26, preventing any liquidations from executing on Morpho markets."
    )

    st.markdown(
        "On **November 6**, [Elixir announced deUSD's sunset]"
        "(https://www.theblock.co/post/377961/elixir-sunsets-deusd-synthetic-stablecoin-"
        "following-stream-finance-unwinding-aims-full-redemptions), "
        "processing 1:1 USDC redemptions for approximately 80% of non-Stream holders. "
        "deUSD subsequently declined to approximately **\\$0.015** (98%+ decline)."
    )

    st.markdown(
        "By **November 8**, xUSD was trading at \\$0.07–\\$0.14 with minimal volume "
        "(approximately \\$30K daily). An estimated **\\$1 billion** in deposits was withdrawn "
        "from DeFi yield platforms in the following week."
    )

    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)

    # ── Exposure Distribution ────────────────────────────────
    st.subheader("Exposure Distribution: \\~\\$285M Across Protocols")
    st.caption("Source: Yields and More (YAM) exposure map, November 4, 2025")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**By Creditor / Curator**")
        import pandas as pd
        exposure_data = pd.DataFrame([
            {"Creditor": "TelosC", "Exposure": "$123.6M", "Platform": "Euler (Plasma)", "Note": "Largest single curator exposure"},
            {"Creditor": "Elixir Network", "Exposure": "$68M", "Platform": "Morpho (private, Plume)", "Note": "65% of deUSD backing; confirmed on-chain via unlisted xUSD/USDC market"},
            {"Creditor": "MEV Capital", "Exposure": "$25.4M", "Platform": "Morpho + Silo + Euler", "Note": "$628K confirmed Morpho bad debt (Arbitrum)"},
            {"Creditor": "Varlamore", "Exposure": "$19.2M", "Platform": "Silo Finance", "Note": ""},
            {"Creditor": "Re7 Labs", "Exposure": "$14.7M", "Platform": "Euler + Morpho", "Note": "$14.65M USDT0 on Plasma"},
            {"Creditor": "Trevee", "Exposure": "$14M", "Platform": "Multiple", "Note": "Published post-mortem"},
            {"Creditor": "Others", "Exposure": "$5.3M", "Platform": "Various", "Note": "Enclabs, Mithras, TiD"},
        ])
        st.dataframe(
            exposure_data,
            column_config={
                "Creditor": "Creditor",
                "Exposure": "Exposure",
                "Platform": "Primary Platform",
                "Note": "Note",
            },
            hide_index=True,
            use_container_width=True,
        )

    with col2:
        st.markdown("**By Protocol**")
        protocol_data = pd.DataFrame([
            {"Protocol": "Euler Finance", "Bad Debt": "~$137M", "Impact": "TVL -47%. xUSD oracle hardcoded at $1.27 on Plasma"},
            {"Protocol": "Morpho", "Bad Debt": "~$4M (public)", "Impact": "1 of ~320 public vaults affected. $68M in private markets"},
            {"Protocol": "Silo Finance", "Bad Debt": "$19.2M", "Impact": "Only $2.13M repaid. DAO prepared legal action"},
            {"Protocol": "Compound", "Bad Debt": "$0", "Impact": "Emergency pause proposed by Gauntlet (sdeUSD risk)"},
        ])
        st.dataframe(
            protocol_data,
            column_config={
                "Protocol": "Protocol",
                "Bad Debt": "Bad Debt",
                "Impact": "Impact Summary",
            },
            hide_index=True,
            use_container_width=True,
        )

    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)

    # ── Aftermath ────────────────────────────────────────────
    st.subheader("Aftermath: Containment, Litigation, and Structural Reforms")

    st.markdown(
        "**Morpho's isolated market architecture successfully contained credit risk**: only "
        "**1 of \\~320 public vaults** suffered direct bad debt impact. Morpho co-founder "
        "Paul Frambot stated publicly: *\"Losses are a natural consequence of risk-taking, "
        "even when systems operate exactly as designed\"* and *\"Lending infrastructure must "
        "remain separate from risk management.\"*"
    )

    col1, col2 = st.columns(2)

    with col1:
        with st.container(border=True):
            st.markdown("**Litigation**")
            st.markdown(
                "**[Stream Trading Corp. v. McMeans et al.]"
                "(https://www.theblock.co/post/377400/stream-finance-halts-withdrawals-93-million-loss)** "
                ", filed **December 8, 2025** in U.S. District Court, "
                "N.D. California (Case No. 3:25-cv-10524). "
                "Perkins Coie representing Stream Trading Corp. "
                "Civil action for breach of contract and related claims. "
                "No criminal or regulatory charges have been filed as of the analysis date."
            )

    with col2:
        with st.container(border=True):
            st.markdown("**Structural Reforms**")
            st.markdown(
                "Morpho V2 (deployed October 8, prior to the crisis) introduced "
                "`forceDeallocate`, relative caps, and Sentinel monitoring. "
                "The [Steakhouse MetaOracle Deviation Timelock]"
                "(https://forum.morpho.org/t/steakhouse-meta-oracle-deviation-timelock/985) "
                "(proposed June 29, Cantina-audited) would have detected xUSD's price "
                "divergence and enabled liquidations, but was not adopted by affected markets."
            )

    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)

    # ── Sources ──────────────────────────────────────────────
    st.subheader("Sources")

    sources = [
        ("TheBlock: Stream Finance halts withdrawals (Nov 3, 2025)",
         "https://www.theblock.co/post/377400/stream-finance-halts-withdrawals-93-million-loss"),
        ("TheBlock: Elixir sunsets deUSD (Nov 6, 2025)",
         "https://www.theblock.co/post/377961/elixir-sunsets-deusd-synthetic-stablecoin-following-stream-finance-unwinding-aims-full-redemptions"),
        ("Stream Trading Corp. v. McMeans et al., Case No. 3:25-cv-10524 (N.D. Cal.)",
         "https://unicourt.com/case/pc-db5-stream-trading-corp-v-mcmeans-et-al-1510416"),
        ("Morpho Governance: Steakhouse MetaOracle Deviation Timelock",
         "https://forum.morpho.org/t/steakhouse-meta-oracle-deviation-timelock/985"),
        ("Gauntlet: Morpho Market Risk Assessment, November 2025",
         "https://www.gauntlet.xyz/resources/morpho-market-risk-assessment-november-2025"),
        ("Arbitrum Foundation: Entropy Advisors DRIP Assessment (Nov 2025)",
         "https://forum.arbitrum.foundation/t/entropy-advisors-drip-assessment-november-2025"),
        ("Tiger Research: Morpho Blue Analysis: xUSD/deUSD Depeg",
         "https://reports.tiger-research.com/p/morpho-blue-analysis-xusd-deusd-depeg"),
        ("Chorus One: Vault Architecture Analysis",
         "https://chorus.one/articles/morpho-vault-architecture-analysis"),
        ("Yields and More (YAM): Stream Finance \\$285M Exposure Breakdown",
         "https://yieldsandmore.substack.com/p/stream-finance-285m-exposure-breakdown"),
    ]

    for title, link in sources:
        if link:
            st.markdown(f"- [{title}]({link})")
        else:
            st.markdown(f"- {title}")

    # ── Disclaimer ───────────────────────────────────────────
    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
    st.caption(
        "*This analysis relies on publicly available on-chain data, court filings, and "
        "published reports. It does not constitute legal opinion or financial advice. "
        "Allegations described herein are as stated in the referenced complaint and "
        "remain unproven in court as of the analysis date (February 2026).*"
    )
