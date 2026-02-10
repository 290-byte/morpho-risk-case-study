# Query Pipeline

## Architecture

```
runner.py              ← Orchestrator: patches paths + runs blocks in order
test_block.py          ← CLI: run & inspect a single block locally
fetch_dex_prices.py    ← GeckoTerminal DEX prices (supplemental)

block1_query_markets_graphql.py    ← Scan all chains for toxic markets
block1_query_vaults_graphql.py     ← 3-phase vault discovery
block2_query_bad_debt_markets.py   ← Per-market bad debt quantification
block2_query_share_prices.py       ← Daily + hourly vault share prices
block3_curator_response.py         ← Allocation timeseries + classification
block3b_liquidity_stress.py        ← Utilization + vault net flows
block5_liquidation_breakdown.py    ← Oracle configs, borrowers, LTV analysis
block6_contagion_analysis.py       ← Cross-market exposure + contagion bridges
```

## Dependency Chain

```
block1_markets ──→ block1_vaults ──→ block2_share_prices
       │                  │              ↓
       │                  ├──→ block3_curator
       │                  │              ↓
       ├──→ block2_bad_debt   block3b_liquidity (needs block2 + block3)
       ├──→ block5_liquidation
       └──→ block6_contagion (also needs Dune CSVs — optional)
```

## Running Locally

```bash
# Full pipeline (all blocks in order)
python queries/runner.py

# Single block with output inspection
python queries/test_block.py block1_markets --inspect

# Run from a specific block onwards
python queries/runner.py --from block2_bad_debt

# Skip blocks whose inputs are missing
python queries/runner.py --skip-missing

# Custom data directory
python queries/runner.py --data-dir ./test_data

# List all blocks
python queries/runner.py --list

# Fetch DEX prices (separate, no API key needed)
python queries/fetch_dex_prices.py
```

## How the Runner Works

The original scripts all use `PROJECT_ROOT / "04-data-exports/raw/graphql/"` for I/O.
The runner:

1. Creates a `data/_workspace/04-data-exports/raw/graphql/` mirrored directory
2. Copies existing CSVs from `data/` into it
3. Monkey-patches `PROJECT_ROOT` in each block module
4. Calls `main()`
5. Syncs output CSVs back to `data/` (flat)

This means **zero changes to the original query scripts** — they run exactly as-is.

## From Streamlit

The dashboard has a **⚙️ Data Management** page that:
- Shows current CSV status (rows, freshness)
- Lets you select and run blocks via the Streamlit UI
- Clears `@st.cache_data` after refresh so charts update

## Data Source

All data from the **Morpho GraphQL API** at `https://blue-api.morpho.org/graphql`.
Public access, no API key required.
