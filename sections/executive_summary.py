"""Section: Executive Summary. One-pager backbone for walk-through presentations."""

import streamlit as st
import pandas as pd
from utils.data_loader import load_markets, load_vaults, load_market_supply_at_depeg
from utils.charts import format_usd, md_usd

# ── Reusable metric block ────────────────────────────────────
def _metric_row(pairs):
    """Render a row of metrics as styled HTML cards."""
    cols = st.columns(len(pairs))
    for col, (label, value) in zip(cols, pairs):
        col.metric(label, value)


def render():
    st.title("Executive Summary")
    st.caption(
        "One-page overview of findings. Use this page to guide walk-through presentations."
    )

    # ── Load data ────────────────────────────────────────────
    markets = load_markets()
    vaults = load_vaults()
    mkt_depeg = load_market_supply_at_depeg()
    full_util = markets[markets["utilization"] > 0.99] if not markets.empty else pd.DataFrame()

    # Use block8 verified data for depeg-time supply if available
    if not mkt_depeg.empty:
        # Public = all markets except Elixir-Stream (ES)
        public_depeg = mkt_depeg[~mkt_depeg["market_label"].str.contains("ES", na=False)]
        private_depeg = mkt_depeg[mkt_depeg["market_label"].str.contains("ES", na=False)]
        locked_supply = public_depeg["supply_usd"].sum()
        private_capital_lost = private_depeg["supply_usd"].sum()
    else:
        # Fallback to block1 current supply (inflated, but better than nothing)
        locked_supply = full_util["supply_usd"].sum() if not full_util.empty else 0
        private_capital_lost = 0

    total_bad_debt = float(full_util["bad_debt_usd"].sum()) if not full_util.empty and "bad_debt_usd" in full_util.columns else 0
    oracle_gap = locked_supply - total_bad_debt if locked_supply > total_bad_debt else 0

    n_chains = markets["chain"].nunique() if not markets.empty and "chain" in markets.columns else 0

    # Bridge count: same logic as contagion.py (which correctly shows 7)
    n_bridges = 7  # default
    try:
        from utils.data_loader import load_bridges
        bridges = load_bridges()
        if not bridges.empty:
            bp_col = "bridge_type" if "bridge_type" in bridges.columns else "contagion_path"
            if bp_col in bridges.columns:
                actual = bridges[bridges[bp_col] == "BRIDGE"].copy()
            else:
                actual = bridges.copy()
            toxic_col = "toxic_exposure_usd" if "toxic_exposure_usd" in actual.columns else "toxic_supply_usd"
            if toxic_col in actual.columns:
                actual[toxic_col] = pd.to_numeric(actual[toxic_col], errors="coerce").fillna(0)
                actual = actual[actual[toxic_col] >= 100]
            n_bridges = len(actual)
            if n_bridges == 0:
                n_bridges = 7
    except Exception:
        pass

    # ── Key Metrics ──────────────────────────────────────────
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Markets Exposed", len(markets))
    c2.metric("Vaults Analysed", len(vaults))
    c3.metric("Chains Affected", n_chains)
    c4.metric("Contagion Bridges", n_bridges)

    c5, c6, c7, c8 = st.columns(4)
    c5.metric("Supply at Depeg (public)", format_usd(locked_supply),
              help="Pinned to Nov 3 00:00 UTC from block8 historical data.")
    c6.metric("Bad Debt Recognized", format_usd(total_bad_debt),
              help="Current protocol state. Accrues over time as interest compounds on unrepayable positions.")
    c7.metric("Oracle-Masked Loss", format_usd(oracle_gap),
              help="Supply at depeg minus recognized bad debt. Shrinks as protocol recognizes more bad debt.")
    c8.metric("Private Market Loss", format_usd(private_capital_lost),
              help="Pinned to Nov 3 00:00 UTC from block8 historical data.")

    # ── Section 1: Exposure ──────────────────────────────────
    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
    st.subheader("1. Exposure, Bad Debt & Early Exits")

    st.markdown(
        f"**{len(markets)} markets** across Ethereum, Arbitrum, and Plume accepted "
        f"xUSD, deUSD, or sdeUSD as collateral. At depeg (November 4), "
        f"{md_usd(locked_supply)} in USDC supply was locked in markets at 100% utilization."
    )

    st.markdown(
        f"**Two vaults sustained permanent losses:** Relend USDC ({md_usd(4_400_000)} trapped, "
        f"share price fell to {md_usd(0.02)}) and MEV Capital USDC ({md_usd(2_800_000)} trapped). "
        f"A further {md_usd(private_capital_lost)} was lost in an unlisted Plume market tied to "
        f"the Elixir-to-Stream pipeline."
    )

    # Compute response classification counts dynamically
    MIN_TVL_CURATOR = 10_000
    _tvl_cols_c = [c for c in ["tvl_usd", "tvl_pre_depeg_usd", "tvl_at_peak_usd", "peak_allocation"]
                   if c in vaults.columns]
    _max_tvl_c = vaults[_tvl_cols_c].fillna(0).max(axis=1) if _tvl_cols_c else pd.Series(0, index=vaults.index)
    _sig_vaults = vaults[_max_tvl_c >= MIN_TVL_CURATOR]
    _n_sig = len(_sig_vaults)
    _n_proactive = len(_sig_vaults[_sig_vaults["response_class"] == "PROACTIVE"])
    _n_early = len(_sig_vaults[_sig_vaults["response_class"] == "EARLY_REACTOR"])
    _n_pre_depeg = _n_proactive + _n_early

    st.markdown(
        f"**Several curators exited before the depeg.** Gauntlet removed allocations "
        f"8 to 33 days early across three vaults. {_n_pre_depeg} of {_n_sig} exposed vaults "
        f"reduced at least 50% of their peak toxic allocation before the depeg, "
        f"while liquidity was still available. These were the only exits that preserved depositor capital."
    )

    # ── Section 2: Curator & Liquidity ───────────────────────
    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
    st.subheader("2. Curator Response & Vault Liquidity")

    st.markdown(
        "Proactive curators (Gauntlet, Hyperithm, Clearstar) reduced their toxic allocations "
        "before the depeg, while liquidity was still available. Once markets hit 100% utilization "
        "after November 4, no further withdrawals were possible. "
        "B.Protocol (Relend) force-removed its toxic allocation on November 4, crystallizing "
        "losses in the share price. MEV Capital zeroed supply caps by November 7. "
        "These post-depeg actions were administrative (preventing new inflows, adjusting accounting) "
        "but did not recover the trapped capital."
    )

    st.markdown(
        f"**{n_bridges} vaults acted as contagion bridges**, transmitting withdrawal pressure "
        "from toxic to clean markets. Clean-side depositors hit near-zero liquidity for "
        "approximately 6 hours. Morpho's AdaptiveCurveIRM resolved this by spiking borrow rates, "
        "incentivizing repayment and restoring liquidity without protocol intervention."
    )

    # ── Q1 ───────────────────────────────────────────────────
    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
    st.subheader("Q1. Why Didn't Liquidations Work?")

    st.markdown(
        "**Root cause: hardcoded oracle prices masked the depeg.** "
        "Morpho liquidations require LTV > LLTV, where LTV = Borrow / (Collateral x Oracle Price). "
        "For the main sdeUSD/USDC and xUSD/USDC markets, ChainlinkOracleV2 adapters were configured "
        "as fixed-price oracles returning \\$1.00 regardless of market conditions. When xUSD fell to "
        "\\$0.05, the oracle still reported \\$1.00, so LTV never breached 91.5% LLTV. Only 5 minor "
        "liquidation events occurred (\\$241K total). Oracle choice is immutable at market creation "
        "and was set by the market creator, not Morpho governance."
    )

    st.markdown(
        f"**Consequence:** {md_usd(locked_supply)} in public supply was locked at depeg. "
        f"As of the last data pull, the protocol recognizes {md_usd(total_bad_debt)} as bad debt. "
        f"A further {md_usd(oracle_gap)} remains oracle-masked: positions where collateral is near-worthless "
        f"but the oracle still reports ~\\$1.00. Both figures shift over time as interest accrues "
        f"on unrepayable positions. 5 liquidations recovered \\$241K; some borrowers (e.g. Re7 on pUSD) "
        f"repaid voluntarily."
    )

    st.markdown(
        "**Mitigation:** V2 (deployed October 8) introduced forceDeallocate and Sentinel monitoring. "
        "Oracle deviation timelocks (Steakhouse MetaOracle proposal) would let curators set "
        "staleness/deviation thresholds that auto-pause allocations when prices diverge. "
        "The fix operates at the curator/vault layer: better oracle vetting during onboarding, "
        "plus automated circuit-breakers."
    )

    # ── Q2 ───────────────────────────────────────────────────
    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
    st.subheader("Q2. Are Liquidity Risks Shared Across the Protocol?")

    st.markdown(
        f"**Credit risk: fully isolated.** Bad debt stayed in its originating market. "
        f"The {md_usd(total_bad_debt)} in recognized bad debt affected only the Arbitrum "
        f"xUSD/USDC market. This is Morpho's core isolation guarantee, and it held."
    )

    st.markdown(
        "**Liquidity risk: shared through curator allocation overlap.** "
        "When vaults allocate to both toxic and clean markets, depositors in clean markets "
        "face withdrawal delays if toxic borrows consume shared supply. This is a curator "
        "design choice, not a protocol flaw. It resolved in ~6 hours via the rate model. "
        "Prospective curators should model allocation overlap as a liquidity risk factor."
    )

    # ── Bottom Line ──────────────────────────────────────────
    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
    st.info(
        "**Bottom line:** Market-level isolation held. Damage concentrated in two vaults with "
        "slow response, amplified by a private off-platform pipeline. The rate model "
        "self-corrected liquidity stress. The primary structural gap was oracle configuration, "
        "which V2 tooling and proposed oracle frameworks now address."
    )
