# Query Methodology

## Data Source

All data was collected from the **Morpho GraphQL API** at `https://blue-api.morpho.org/graphql`.

This is Morpho's official read-only API that provides access to markets, vaults, positions, allocations, events, and on-chain state across all supported chains.

## Query Strategy

| Block | Query | Output | Method |
|-------|-------|--------|--------|
| 1.1 | Toxic market discovery | `markets.csv` | Scan all 3,133 markets across 8 chains, filter for xUSD/deUSD/sdeUSD collateral |
| 1.2 | Vault exposure mapping | `vaults.csv` | 3-phase discovery: current positions → reallocation history → individual vault fetch |
| 2.1 | Share price history | `share_prices_daily.csv` | Daily + hourly share prices for all 33 vaults (Sep 2025 – Jan 2026) |
| 2.2 | Bad debt quantification | `markets.csv` (bad_debt columns) | Protocol-level bad debt from market state + supply/borrow gaps |
| 3.1 | Curator response | `vaults.csv` (response columns) | Classify based on reallocation timing vs Nov 4 depeg date |
| 3.2 | Market utilization | `market_utilization_hourly.csv` | Hourly utilization rates during Nov 1–15 stress period |
| 3.3 | Vault net flows | `vault_net_flows.csv` | Daily TVL changes during stress period |
| 5.1 | Oracle configs | `ltv_analysis.csv` | Oracle type, LLTV thresholds, oracle vs true LTV |
| 5.2 | Asset prices | `asset_prices.csv` | xUSD, deUSD, sdeUSD market prices (Oct–Dec 2025) |
| 5.3 | Borrower positions | `borrower_concentration.csv` | Active borrower analysis per market |
| 6.1 | Vault multi-exposure | `exposure_summary.csv` | Cross-market exposure mapping for all affected vaults |
| 6.2 | Contagion bridges | `contagion_bridges.csv` | Vaults that bridge toxic ↔ clean markets |

## Key Design Decisions

**GraphQL over Dune:** We used Morpho's GraphQL API rather than Dune Analytics because:
1. Real-time data (not dependent on indexer lag)
2. Direct access to protocol-internal fields (share prices, bad debt accounting)
3. Cross-chain coverage (Ethereum, Arbitrum, Plume) from a single endpoint
4. No SQL query limits or rate concerns for comprehensive scans

**3-Phase Vault Discovery:** Simple current-position queries miss vaults that already exited. Our 3-phase approach (current → reallocation history → individual fetch) found 20 additional vaults that a naive query would have missed.

**Oracle-Independent Analysis:** We calculated bad debt three ways (supply gap, protocol accounting, oracle vs spot) to avoid relying solely on potentially stale oracle data.

## API Access

The Morpho GraphQL API is publicly accessible — no API key required:
```bash
curl -X POST https://blue-api.morpho.org/graphql \
  -H "Content-Type: application/json" \
  -d '{"query": "{ markets(first: 5) { items { uniqueKey loanAsset { symbol } collateralAsset { symbol } } } }"}'
```

## Running Queries

```bash
# Install dependencies
pip install requests pandas

# Run all queries and regenerate CSVs
python run_all.py
```

Note: Full query execution takes ~10 minutes due to pagination across 3,133 markets and 33 vaults.
