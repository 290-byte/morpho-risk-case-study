"""
Morpho xUSD/deUSD Risk Case Study Dashboard
=============================================
Interactive analysis of the Nov 2025 depeg event's impact on Morpho.

Run: streamlit run app.py
"""

import streamlit as st

st.set_page_config(
    page_title="Morpho Risk Case Study",
    layout="wide",
    initial_sidebar_state="expanded",
)

# -- Custom CSS (Morpho-inspired) --------------------------------
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600&display=swap');

    /* Global font and color */
    html, body, [class*="css"], .main, .block-container,
    p, span, div, label, li, td, th, a,
    [data-testid="stMarkdownContainer"],
    [data-testid="stMarkdownContainer"] p,
    [data-testid="stMarkdownContainer"] span,
    [data-testid="stText"],
    .element-container {
        font-family: 'Inter', 'Helvetica Neue', sans-serif;
        font-weight: 400;
        color: #000000 !important;
    }

    /* Headings */
    h1, h2, h3, h4,
    [data-testid="stHeading"],
    [data-testid="stHeading"] * {
        font-family: 'Inter', 'Helvetica Neue', sans-serif !important;
        font-weight: 500 !important;
        color: #000000 !important;
    }
    h1 { font-size: 1.75rem !important; }

    /* Captions slightly lighter */
    [data-testid="stCaptionContainer"],
    [data-testid="stCaptionContainer"] * {
        color: #555555 !important;
    }

    /* Force white backgrounds everywhere */
    .main, .block-container, [data-testid="stAppViewContainer"],
    [data-testid="stHeader"], [data-testid="stToolbar"] {
        background-color: #FFFFFF !important;
    }

    /* Plotly charts white bg */
    [data-testid="stPlotlyChart"],
    [data-testid="stPlotlyChart"] > div,
    iframe {
        background-color: #FFFFFF !important;
    }

    /* Metric cards */
    [data-testid="stMetric"] {
        background: #F7F8FA;
        border: 1px solid #E5E7EB;
        border-radius: 8px;
        padding: 12px 16px;
    }
    [data-testid="stMetricLabel"],
    [data-testid="stMetricLabel"] * {
        font-size: 0.78rem;
        color: #000000 !important;
        font-weight: 400;
    }
    [data-testid="stMetricValue"],
    [data-testid="stMetricValue"] * {
        font-size: 1.35rem;
        font-weight: 500;
        color: #000000 !important;
    }

    /* Section dividers */
    .section-divider {
        border-top: 1px solid #E5E7EB;
        margin: 1.5rem 0;
    }

    /* Sidebar */
    [data-testid="stSidebar"] {
        background-color: #F7F8FA;
        border-right: 1px solid #E5E7EB;
    }
    [data-testid="stSidebar"] * {
        color: #111111 !important;
    }

    /* Tabs */
    .stTabs [data-baseweb="tab-list"] {
        gap: 2px;
    }
    .stTabs [data-baseweb="tab"] {
        font-weight: 400;
        font-size: 0.85rem;
    }

    /* Dataframes */
    [data-testid="stDataFrame"] {
        border: 1px solid #E5E7EB;
        border-radius: 8px;
    }

    /* Hide default footer */
    footer { visibility: hidden; }

    /* Multiselect chips */
    [data-baseweb="tag"] {
        background-color: #EFF6FF !important;
        border: 1px solid #BFDBFE !important;
        color: #1D4ED8 !important;
    }

    /* Buttons */
    .stButton > button[kind="primary"] {
        background-color: #2470FF;
        border: none;
    }
</style>
""", unsafe_allow_html=True)

# -- Navigation ---------------------------------------------------
from sections import (
    executive_summary, background, overview, damage_summary,
    market_exposure, bad_debt, curator_response,
    liquidity_stress, liquidation_failure, contagion,
    recommendations, admin, how_morpho_works
)

pages = {
    "": [
        st.Page(executive_summary.render, title="Executive Summary", url_path="summary", default=True),
    ],
    "Context": [
        st.Page(how_morpho_works.render, title="How Morpho Works", url_path="primer"),
        st.Page(background.render, title="Background", url_path="background"),
        st.Page(overview.render, title="Timeline & Prices", url_path="timeline"),
    ],
    "Root Cause": [
        st.Page(liquidation_failure.render, title="Liquidation Failure", url_path="liquidation"),
    ],
    "Impact": [
        st.Page(market_exposure.render, title="Market Exposure", url_path="markets"),
        st.Page(bad_debt.render, title="Bad Debt Analysis", url_path="bad-debt"),
        st.Page(damage_summary.render, title="Damage Summary", url_path="damage"),
        st.Page(contagion.render, title="Contagion Assessment", url_path="contagion"),
    ],
    "Crisis Response": [
        st.Page(curator_response.render, title="Curator Response", url_path="curators"),
        st.Page(liquidity_stress.render, title="Liquidity Stress", url_path="stress"),
    ],
    "Conclusions": [
        st.Page(recommendations.render, title="Recommendations", url_path="recommendations"),
    ],
    "Admin": [
        st.Page(admin.render, title="Data & Methodology", url_path="admin"),
    ],
}

nav = st.navigation(pages)

# -- Generate snapshot on each app load ---------------------------
try:
    from utils.snapshot import write_snapshot
    write_snapshot()
except Exception:
    pass

# -- Sidebar info -------------------------------------------------
with st.sidebar:
    st.caption("Morpho Risk Case Study")
    st.caption("xUSD / deUSD Depeg, Nov 2025")
    st.divider()

    # ── Site Index ───────────────────────────────────────────
    with st.expander("Page Index", expanded=False):
        st.markdown("""
**Executive Summary**
- Key metrics, Q1 & Q2 answers, bottom line

**Context**
- How Morpho Works - Markets, vaults, curators, oracles
- Background - Key entities, fund loss, market impact
- Timeline & Prices - Token prices, event timeline

**Root Cause**
- Liquidation Failure - Oracle masking, LTV analysis

**Impact**
- Market Exposure - Public/private markets, vault allocations
- Bad Debt Analysis - Per-market debt, share price impact
- Damage Summary - Taxonomy, permanent vs temporary
- Contagion Assessment - Exposure network, bridge vaults

**Crisis Response**
- Curator Response - Timeline, classification, timelocks
- Liquidity Stress - Utilization, TVL dynamics, rankings

**Conclusions**
- Recommendations - Five structural improvements

**Admin**
- Data & Methodology - Pipeline, field inventory, sources
""")

    st.divider()
    st.caption("Data: Morpho GraphQL API + on-chain")

    # Show warnings for any missing pipeline data files
    from utils.data_loader import show_data_warnings
    show_data_warnings()

nav.run()
