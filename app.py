"""
Morpho xUSD/deUSD Risk Case Study Dashboard
=============================================
Interactive analysis of the Nov 2025 depeg event's impact on Morpho Blue.

Run: streamlit run app.py
"""

import streamlit as st

st.set_page_config(
    page_title="Morpho Risk Case Study — xUSD/deUSD Depeg",
    page_icon="🔬",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Custom CSS ──────────────────────────────────────────────
st.markdown("""
<style>
    /* Tighten metric cards */
    [data-testid="stMetric"] {
        background: rgba(99, 102, 241, 0.08);
        border: 1px solid rgba(99, 102, 241, 0.2);
        border-radius: 8px;
        padding: 12px 16px;
    }
    [data-testid="stMetricLabel"] { font-size: 0.8rem; }
    [data-testid="stMetricValue"] { font-size: 1.4rem; }
    
    /* Section dividers */
    .section-divider {
        border-top: 1px solid rgba(255,255,255,0.1);
        margin: 1.5rem 0;
    }
    
    /* Hide default footer */
    footer { visibility: hidden; }
</style>
""", unsafe_allow_html=True)

# ── Navigation ──────────────────────────────────────────────
from sections import overview, market_exposure, bad_debt, curator_response
from sections import liquidity_stress, liquidation_failure, contagion

pages = {
    "Case Study": [
        st.Page(overview.render, title="Overview & Timeline", icon="📋", url_path="overview"),
        st.Page(market_exposure.render, title="Market Exposure", icon="🎯", url_path="markets"),
        st.Page(bad_debt.render, title="Bad Debt Analysis", icon="💀", url_path="bad-debt"),
    ],
    "Response & Stress": [
        st.Page(curator_response.render, title="Curator Response", icon="🏷️", url_path="curators"),
        st.Page(liquidity_stress.render, title="Liquidity Stress", icon="📉", url_path="stress"),
    ],
    "Risk Outcomes": [
        st.Page(liquidation_failure.render, title="Liquidation Failure", icon="⚡", url_path="liquidation"),
        st.Page(contagion.render, title="Contagion Assessment", icon="🕸️", url_path="contagion"),
    ],
}

nav = st.navigation(pages)

# ── Sidebar info ────────────────────────────────────────────
with st.sidebar:
    st.caption("Morpho Risk Case Study")
    st.caption("xUSD / deUSD Depeg — Nov 2025")
    st.divider()
    st.caption("Data: Morpho GraphQL API + on-chain")
    st.caption("Analysis date: Feb 8, 2026")

nav.run()
