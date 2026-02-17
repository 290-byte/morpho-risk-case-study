# Morpho Risk Case Study - xUSD / deUSD Depeg (November 2025)

Interactive Streamlit dashboard analyzing the impact of the xUSD/deUSD stablecoin depeg on Morpho Blue.

**[Live Dashboard](https://morpho-risk-case-study.streamlit.app)**

---

## Key Findings

- **$15.1M** in public supply locked at depeg across 18 markets on 3 chains (Ethereum, Arbitrum, Plume)
- **$4.5M** recognized as bad debt by the protocol (accrues over time as interest compounds)
- **$10.5M** oracle-masked: collateral near-worthless but oracle still reports ~$1.00
- **$70.9M** lost in a private Elixir-Stream market on Plume
- **5 liquidations** totaling $241K - hardcoded oracles prevented the liquidation engine from firing
- **13 of 20** exposed vaults reduced allocation before the depeg, preserving depositor capital
- **7 contagion bridges** - vaults that bridged toxic and clean markets, creating withdrawal delays

## Dashboard Sections

| Section | Description |
|---------|-------------|
| Executive Summary | Top-line metrics, exposure, curator exits, and research questions |
| How Morpho Works | Primer on markets, vaults, curators, and oracles |
| Background | Timeline of the xUSD/deUSD collapse |
| Timeline & Prices | Asset price charts and event timeline |
| Liquidation Failure | Oracle vs spot prices, LTV analysis, why liquidations did not fire |
| Market Exposure | 18 toxic markets, supply breakdown, vault mapping |
| Bad Debt | Protocol-level and vault-level bad debt, share price analysis |
| Damage Summary | All 7 damage categories with evidence |
| Contagion | Multi-exposure mapping, contagion bridges, liquidity spillover |
| Curator Response | Response classification, pre-depeg vs post-depeg actions |
| Liquidity Stress | Utilization spikes, rate model response, vault net flows |
| Recommendations | Five structural improvements tied to specific failures |
| Data & Methodology | Pipeline runner, data freshness notes, field inventory |

## Running Locally

```bash
git clone https://github.com/290-byte/morpho-risk-case-study.git
cd morpho-risk-case-study

pip install -r requirements.txt

streamlit run app.py
```

Pre-computed CSVs in `data/` allow the dashboard to run without API access. Large files are stored as `.csv.gz` and loaded transparently by the data loader.

## Refreshing Data

The `queries/` directory contains Python scripts that fetch data from the Morpho GraphQL API. Run them via the admin page in the dashboard, or from the command line:

```bash
python queries/runner.py                    # Run all blocks
python queries/runner.py block1_markets     # Run a specific block
```

## Architecture

```
app.py                       Streamlit entrypoint + navigation
sections/                    13 page modules (one per analysis section)
utils/data_loader.py         Cached loaders for all CSVs (.csv and .csv.gz)
utils/charts.py              Reusable Plotly chart helpers
data/                        Pre-computed CSV files (some gzipped)
queries/                     GraphQL query scripts
```

## Data Freshness

Most metrics are pinned to historical timestamps (block8 hourly data at Nov 3 00:00 UTC) and do not change between pipeline runs. Two exceptions: **Bad Debt Recognized** and **Oracle-Masked Loss** come from the protocol's current state and accrue over time as interest compounds on unrepayable positions.

---

*Case study for Morpho Risk Analyst position - February 2026*
