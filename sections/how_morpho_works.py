"""Section 0: How Morpho Works: Primer for prospective integrators."""

import streamlit as st


def render():
    st.title("How Morpho Works")
    st.caption(
        "Quick overview of Morpho's architecture before we get into the November 2025 event."
    )

    # ── Morpho Markets ──────────────────────────────────
    st.subheader("1. Markets: Isolated Lending Pools")

    col1, col2 = st.columns([2, 1])
    with col1:
        st.markdown(
            "A **Morpho market** is a single, isolated lending pool defined by exactly "
            "**one collateral asset** and **one loan asset**, plus a fixed set of parameters "
            "(oracle, liquidation LTV, interest rate model). "
            "There are no shared pools. Each market is independent."
        )
        st.markdown(
            "The whole point of this design: bad debt in one market "
            "**cannot** spread to another. A lender in the WETH/USDC market is "
            "unaffected by a collapse in the xUSD/USDC market, even if both exist on the same protocol."
        )
    with col2:
        with st.container(border=True):
            st.markdown("**Market Parameters**")
            st.caption(
                "Each market is defined by:\n"
                "- Collateral asset (e.g. xUSD)\n"
                "- Loan asset (e.g. USDC)\n"
                "- Oracle (price feed)\n"
                "- LLTV (liquidation threshold)\n"
                "- Interest rate model"
            )

    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)

    # ── MetaMorpho Vaults ────────────────────────────────────
    st.subheader("2. Vaults: Curated Multi-Market Wrappers")

    col1, col2 = st.columns([2, 1])
    with col1:
        st.markdown(
            "Most depositors interact with **MetaMorpho vaults**, not individual markets. "
            "A vault accepts deposits (e.g. USDC) and allocates that capital across "
            "**multiple underlying markets** (sometimes 5, sometimes 80+)."
        )
        st.markdown(
            "The vault issues a **share token** that accrues value over time as interest "
            "is earned. In a V1.0 vault, if one of the underlying markets suffers bad debt, "
            "the share price drops, and the loss is **socialized** across all depositors. "
            "In a V1.1 vault, bad debt is deferred: the share price continues rising "
            "while losses accrue silently in an internal variable."
        )
    with col2:
        with st.container(border=True):
            st.markdown("**Vault Flow**")
            st.caption(
                "Depositor → Vault (USDC)\n"
                "↓\n"
                "Vault allocates to:\n"
                "  → Market A (WETH/USDC)\n"
                "  → Market B (wstETH/USDC)\n"
                "  → Market C (xUSD/USDC)\n"
                "↓\n"
                "Share token accrues yield"
            )

    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)

    # ── Curators ─────────────────────────────────────────────
    st.subheader("3. Curators: The Human Risk Layer")

    st.markdown(
        "Each vault has a **curator**: a team or entity that decides which markets the "
        "vault supplies to, sets **supply caps** per market, and manages allocations over "
        "time. Curators are the risk managers of the vault ecosystem."
    )

    col1, col2, col3 = st.columns(3)
    with col1:
        with st.container(border=True):
            st.markdown("**Allocation Decisions**")
            st.caption(
                "Curators choose which markets to enter, "
                "how much capital to allocate, and when to exit. "
                "These decisions directly determine depositor outcomes."
            )
    with col2:
        with st.container(border=True):
            st.markdown("**Timelocks**")
            st.caption(
                "Most vaults have a timelock (e.g. 3 days) "
                "that delays allocation changes. Designed to prevent "
                "reckless moves, but can also delay emergency exits."
            )
    with col3:
        with st.container(border=True):
            st.markdown("**Accountability**")
            st.caption(
                "Depositors trust their curator's judgment. "
                "Response speed during a crisis varies widely, and "
                "this mattered a lot in November."
            )

    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)

    # ── Oracles & Liquidation ────────────────────────────────
    st.subheader("4. Oracles & Liquidation: The Safety Net")

    col1, col2 = st.columns(2)
    with col1:
        with st.container(border=True):
            st.markdown("**Oracles**")
            st.markdown(
                "Each market has an **oracle** that reports the collateral's price. "
                "Morpho does not enforce oracle choice; the market creator picks the oracle "
                "at deployment, and it **cannot be changed** afterward."
            )
            st.caption(
                "If the oracle reports an incorrect price, the entire liquidation "
                "mechanism breaks. That is what happened here."
            )

    with col2:
        with st.container(border=True):
            st.markdown("**Liquidation**")
            st.markdown(
                "When a borrower's **loan-to-value ratio (LTV)** exceeds the market's "
                "**liquidation LTV (LLTV)**, anyone can liquidate the position by selling "
                "collateral to repay the debt."
            )
            st.caption(
                "Critical dependency: liquidation only triggers based on the oracle price. "
                "If oracle says LTV = 70% (healthy) while reality says LTV = 5,000% "
                "(catastrophically underwater), no liquidation fires."
            )

    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)

    # ── Why This Matters ─────────────────────────────────────
    st.subheader("5. What the Architecture Promises vs. What Actually Happened")

    st.markdown(
        "Morpho's **isolation guarantee** is that bad debt stays in its market. "
        "This is true at the protocol level. But the **vault layer** creates shared exposure: "
        "if a curator allocates depositor funds to both a safe market and a toxic market, "
        "depositors in that vault share the loss."
    )

    st.info(
        "**November 2025 stress-tested all of this.** "
        "Oracles that misreported prices. Curators who ranged from weeks-early to weeks-late. "
        "Vaults that socialized bad debt. Markets that turned out to be isolated "
        "for credit risk but not for liquidity. And a V1.1 mechanism that hid losses from "
        "depositors. The rest of this dashboard walks through each one."
    )

    st.caption(
        "*This page is a simplified overview of Morpho's architecture. "
        "For complete technical documentation, see "
        "[docs.morpho.org](https://docs.morpho.org/).*"
    )
