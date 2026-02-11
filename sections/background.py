"""Section 0: Background Story — The full narrative behind the xUSD/deUSD collapse."""

import streamlit as st


def render():
    st.title("Background — The xUSD / deUSD Collapse")
    st.caption(
        "How a single fund manager's margin call ignited \\$285M in bad debt across DeFi lending."
    )

    # ── The Players ──────────────────────────────────────────
    st.subheader("The Players")

    col1, col2, col3 = st.columns(3)

    with col1:
        with st.container(border=True):
            st.markdown("**Stream Finance (xUSD)**")
            st.markdown(
                "Yield-bearing stablecoin promising 12–18% APY. "
                "Founded February 2024, raised \\$1.5M seed from "
                "[Polychain Capital](https://polychain.capital/). "
                "Reached **\\~\\$204M TVL** by late October 2025."
            )

    with col2:
        with st.container(border=True):
            st.markdown("**Caleb McMeans — \"0xlaw\"**")
            st.markdown(
                "Took over Stream Finance operations in **January 2025**. "
                "Gave fund manager Ryan DeMattia control of \\$90M+ in off-chain assets "
                "despite allegedly having \"no formal relationship\" with him."
            )

    with col3:
        with st.container(border=True):
            st.markdown("**Ryan DeMattia — \"0xDeimos\"**")
            st.markdown(
                "External fund manager entrusted with \\~\\$93M in Stream Protocol assets "
                "for off-chain management. Liquidated on personal leveraged positions "
                "during the Oct 10 ETH crash. Allegedly misappropriated Stream assets "
                "to cover his margin call."
            )

    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)

    # ── The Scheme ───────────────────────────────────────────
    st.subheader("The Circular Dependency: xUSD ↔ deUSD")

    st.markdown(
        "The critical amplifying mechanism was a **recursive cross-minting loop** between "
        "Stream's xUSD and Elixir's deUSD, first exposed by Yearn developer "
        "[Schlag](https://x.com/) on **October 28, 2025** — six days before the collapse."
    )

    with st.container(border=True):
        st.markdown("**The Loop:**")
        st.markdown(
            "Stream received USDC → swapped to USDT → minted Elixir's deUSD "
            "→ used deUSD to borrow more USDC → minted more xUSD → **repeat**."
        )
        st.markdown(
            "Using just **\\$1.9M in USDC**, approximately **\\$14.5M in xUSD** was created "
            "through these loops — a **7.6x capital amplification**."
        )

    st.markdown("")

    col1, col2 = st.columns(2)

    with col1:
        with st.container(border=True):
            st.markdown("**Elixir's Hidden Exposure**")
            st.markdown(
                "Elixir lent **\\$68M USDC** — representing **65% of deUSD's total backing** "
                "— to Stream through **private, hidden Morpho markets** where Stream was "
                "the sole borrower, using its own xUSD as collateral."
            )
            st.caption("Source: BlockEden analysis, lawsuit details, YAM exposure map")

    with col2:
        with st.container(border=True):
            st.markdown("**Mutual Destruction Guaranteed**")
            st.markdown(
                "Stream simultaneously held **\\~90% of remaining deUSD supply (\\~\\$75M)**. "
                "This guaranteed mutual destruction: when xUSD collapsed, deUSD's backing "
                "evaporated; when deUSD collapsed, Stream lost its borrowed capital."
            )
            st.caption("Source: Lawsuit complaint, on-chain analysis")

    st.markdown("")

    st.markdown(
        "On the same day, on-chain analyst **CBB0FE** independently flagged the danger: "
        "*\"xUSD has \\~\\$170M backing it on-chain. They're borrowing \\~\\$530M from lending "
        "protocols. That's 4.1x leverage.\"*"
    )

    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)

    # ── The Fraud ────────────────────────────────────────────
    st.subheader("The Fraud: October 10 – November 3")

    st.markdown(
        "On **October 10, 2025**, during a sharp ETH crash (\\~21% decline, \\$20B in "
        "market-wide liquidations), DeMattia's personal leveraged positions were liquidated. "
        "He allegedly used Stream Protocol assets to cover his margin call — beginning the "
        "misappropriation of approximately **\\$93 million**."
    )

    st.markdown(
        "By **November 2**, DeMattia admitted to McMeans that he had lost \"nearly all\" "
        "of the funds. McMeans allegedly transferred \\~\\$2.1M from Stream wallets to personal "
        "wallets via [Railgun](https://railgun.org/) (a privacy mixer)."
    )

    st.markdown(
        "On **November 3**, the [Balancer V2 exploit](https://rekt.news/) "
        "(\\$100–128M across 6+ chains) created DeFi-wide panic — but this was "
        "**concurrent, not causal**. The lawsuit confirms DeMattia lost the funds on Nov 2, "
        "the day before. Multiple sources noted Balancer became \"the perfect excuse to "
        "divert attention.\""
    )

    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)

    # ── The Cascade ──────────────────────────────────────────
    st.subheader("The Cascade: November 3–8")

    st.markdown(
        "Stream announced the \\$93M loss on **November 3**, freezing deposits and withdrawals. "
        "xUSD crashed from \\$1.00 to **\\$0.24–\\$0.30** by November 4. Critically, Morpho's "
        "Chainlink oracle continued reporting xUSD at approximately \\$1.00–\\$1.26, "
        "completely preventing liquidations."
    )

    st.markdown(
        "On **November 6**, [Elixir announced deUSD's sunset]"
        "(https://www.theblock.co/post/377961/elixir-sunsets-deusd-synthetic-stablecoin-"
        "following-stream-finance-unwinding-aims-full-redemptions), "
        "processing 1:1 USDC redemptions for 80% of non-Stream holders. "
        "deUSD collapsed to **\\$0.015** (98%+ decline)."
    )

    st.markdown(
        "By **November 8**, xUSD was trading at \\$0.07–\\$0.14 with \\~\\$30K daily volume. "
        "Approximately **\\$1 billion** exited DeFi yield platforms in the following week."
    )

    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)

    # ── The $285M Map ────────────────────────────────────────
    st.subheader("Where the \\$285M Ended Up")
    st.caption("Source: Yields and More (YAM) exposure map, Nov 4, 2025")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**By Creditor / Curator**")
        import pandas as pd
        exposure_data = pd.DataFrame([
            {"Creditor": "TelosC", "Exposure": "$123.6M", "Platform": "Euler (Plasma)", "Note": "Largest single curator exposure"},
            {"Creditor": "Elixir Network", "Exposure": "$68M", "Platform": "Morpho (private)", "Note": "65% of deUSD backing; Stream sole borrower"},
            {"Creditor": "MEV Capital", "Exposure": "$25.4M", "Platform": "Morpho + Silo + Euler", "Note": "$628K confirmed Morpho Arb bad debt"},
            {"Creditor": "Varlamore", "Exposure": "$19.2M", "Platform": "Silo Finance", "Note": ""},
            {"Creditor": "Re7 Labs", "Exposure": "$14.7M", "Platform": "Euler + Morpho", "Note": "$14.65M USDT0 on Plasma"},
            {"Creditor": "Trevee", "Exposure": "$14M", "Platform": "Multiple", "Note": "Published own post-mortem"},
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
            {"Protocol": "Euler Finance", "Bad Debt": "~$137M", "Impact": "TVL -47%. xUSD oracle hardcoded at $1.27 on Plasma chain"},
            {"Protocol": "Morpho", "Bad Debt": "~$4M (public)", "Impact": "1 of ~320 vaults affected. $68M in private markets"},
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

    # ── The Aftermath ────────────────────────────────────────
    st.subheader("Aftermath: Isolation Success, a Lawsuit, and Structural Reforms")

    st.markdown(
        "**Morpho's isolation architecture successfully contained damage** — only **1 of \\~320 public vaults** "
        "was directly affected. The co-founders defended the design through social media, with Paul Frambot stating: "
        "*\"Losses are a natural consequence of risk-taking, even when systems operate exactly "
        "as designed\"* and *\"Lending infrastructure must remain separate from risk management.\"*"
    )

    col1, col2 = st.columns(2)

    with col1:
        with st.container(border=True):
            st.markdown("**The Lawsuit**")
            st.markdown(
                "**[Stream Trading Corp. v. McMeans]"
                "(https://www.theblock.co/post/377400/stream-finance-halts-withdrawals-93-million-loss)** "
                "filed **December 8, 2025** in U.S. District Court, "
                "N.D. California (Case No. 3:25-cv-10524). "
                "Perkins Coie representing Stream. Civil suit for breach of contract "
                "against Ryan DeMattia and Caleb McMeans. "
                "No SEC/CFTC/DOJ criminal or regulatory charges filed."
            )

    with col2:
        with st.container(border=True):
            st.markdown("**Structural Reforms**")
            st.markdown(
                "Morpho V2 (deployed Oct 8, before the crisis) introduced "
                "`forceDeallocate`, relative caps, and Sentinel monitoring. "
                "The [Steakhouse MetaOracle Deviation Timelock]"
                "(https://forum.morpho.org/t/steakhouse-meta-oracle-deviation-timelock/985) "
                "(proposed June 29, Cantina-audited) would have detected xUSD's crash "
                "and enabled liquidations — but was not adopted by affected markets."
            )

    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)

    # ── Source Links ─────────────────────────────────────────
    st.subheader("Key Sources")

    sources = [
        ("TheBlock — Stream Finance halts withdrawals",
         "https://www.theblock.co/post/377400/stream-finance-halts-withdrawals-93-million-loss"),
        ("TheBlock — Elixir sunsets deUSD",
         "https://www.theblock.co/post/377961/elixir-sunsets-deusd-synthetic-stablecoin-following-stream-finance-unwinding-aims-full-redemptions"),
        ("Lawsuit — Case No. 3:25-cv-10524 (N.D. Cal.)",
         "https://unicourt.com/case/pc-db5-stream-trading-corp-v-mcmeans-et-al-1510416"),
        ("Morpho Governance — Steakhouse MetaOracle Deviation Timelock",
         "https://forum.morpho.org/t/steakhouse-meta-oracle-deviation-timelock/985"),
        ("Gauntlet — Nov 18 Market Report",
         "https://www.gauntlet.xyz/resources/morpho-market-risk-assessment-november-2025"),
        ("Arbitrum Foundation — Entropy Advisors DRIP Assessment",
         "https://forum.arbitrum.foundation/t/entropy-advisors-drip-assessment-november-2025"),
        ("Tiger Research — Morpho Analysis",
         "https://reports.tiger-research.com/p/morpho-blue-analysis-xusd-deusd-depeg"),
        ("Chorus One — Vault Architecture Analysis",
         "https://chorus.one/articles/morpho-vault-architecture-analysis"),
        ("Yields and More (YAM) — \\$285M Exposure Map",
         "https://yieldsandmore.substack.com/p/stream-finance-285m-exposure-breakdown"),
    ]

    for title, link in sources:
        if link:
            st.markdown(f"- [{title}]({link})")
        else:
            st.markdown(f"- {title}")
