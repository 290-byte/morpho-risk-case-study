# Morpho Risk Case Study — xUSD / deUSD Depeg (Nov 2025)

Interactive Streamlit dashboard analyzing the impact of the xUSD/deUSD stablecoin depeg on Morpho Blue protocol.

**[🔗 Live Dashboard](https://morpho-risk-case-study.streamlit.app)** *(update URL after deployment)*

---

## Key Findings

- **$3.64M bad debt** across 18 markets on 3 chains (Ethereum, Arbitrum, Plume)
- **Zero liquidations** — hardcoded Chainlink oracles reported ≈$1.00 while tokens traded at $0.002–0.05
- **33 vaults exposed** — 15 curators exited proactively, 2 vaults suffered share price damage
- **4 contagion bridges** — vaults that bridged toxic and clean markets, socializing losses

## Dashboard Sections

| Section | Description |
|---------|-------------|
| **Overview** | Timeline, key metrics, asset price collapse |
| **Market Exposure** | 18 toxic markets, collateral breakdown, vault mapping |
| **Bad Debt** | $3.64M quantification, share price impacts, three-layer analysis |
| **Curator Response** | Response classification (proactive → very late), timelock analysis |
| **Liquidity Stress** | Utilization spikes, TVL outflows, vault stress rankings |
| **Liquidation Failure** | Oracle vs market prices, LTV analysis, borrower concentration |
| **Contagion** | Multi-exposure mapping, contagion bridges, resilience recommendations |

## Running Locally

```bash
# Clone
git clone https://github.com/YOUR_USERNAME/morpho-risk-case-study.git
cd morpho-risk-case-study

# Install dependencies
pip install -r requirements.txt

# Generate sample data (or place your own CSVs in data/)
python generate_data.py

# Run dashboard
streamlit run app.py
```

## Data Sources

All data was collected via the **Morpho GraphQL API** (`blue-api.morpho.org/graphql`) and on-chain queries. The `queries/` directory contains the Python scripts used to fetch each dataset — see `queries/README.md` for methodology.

Pre-computed CSVs in `data/` allow the dashboard to run without API keys.

```
data/               ← Pre-loaded CSVs (dashboard reads from here)
queries/            ← Query scripts (write to data/)
```

To refresh data from live APIs:
```bash
python queries/run_all.py  # Requires API access
```

## Architecture

```
app.py                    → Streamlit entrypoint + navigation
sections/                 → 7 page modules (one per analysis section)
utils/data_loader.py      → @st.cache_data loaders for all CSVs
utils/charts.py           → Reusable Plotly chart helpers
data/                     → Pre-computed CSV files
queries/                  → GraphQL query scripts (show-your-work)
```

Key design decisions:
- **`st.navigation`** multi-page app — only current page executes per rerun
- **Plotly Express** for charts — full financial chart support, interactive zoom/hover
- **Progressive disclosure** — metrics → charts → detail tables per section
- **Decoupled data pipeline** — dashboard reads CSVs, queries write CSVs, independently

## Tech Stack

- Python 3.11+
- Streamlit 1.40+
- Plotly 5.18+
- Pandas 2.0+

---

*Case study for Morpho Risk Analyst position — analysis date: Feb 8, 2026*
