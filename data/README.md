# Data Files

Pre-computed CSV files used by the Streamlit dashboard. Generated from Morpho GraphQL API queries (Feb 8, 2026).

| File | Rows | Description |
|------|------|-------------|
| `markets.csv` | 18 | All toxic markets (xUSD/deUSD/sdeUSD collateral) across 3 chains |
| `vaults.csv` | 33 | Exposed vaults with curator, TVL, response classification, share price |
| `share_prices_daily.csv` | ~4,660 | Daily share prices for key vaults (Sep 2025 – Jan 2026) |
| `asset_prices.csv` | ~4,400 | Hourly xUSD, deUSD, sdeUSD prices (Oct–Dec 2025) |
| `vault_net_flows.csv` | ~210 | Daily TVL changes during Nov 1–15 stress period |
| `market_utilization_hourly.csv` | ~2,000 | Hourly utilization rates for 6 key markets |
| `ltv_analysis.csv` | 7 | LLTV vs true LTV for markets with active borrowers |
| `borrower_concentration.csv` | 7 | Borrower distribution per active market |
| `contagion_bridges.csv` | 4 | Vaults that bridge toxic ↔ clean markets |
| `exposure_summary.csv` | 4 | Distribution of single vs multi-market exposure |
| `timeline_events.csv` | 13 | Key events from Sep 2025 – Dec 2025 |

## Regenerating

From project root:
```bash
# Using test data generator (offline):
python generate_data.py

# Using live API queries:
python queries/run_all.py
```

## Source

All data sourced from Morpho GraphQL API (`blue-api.morpho.org/graphql`) unless noted.
