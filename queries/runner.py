"""
Query Runner — Runs block query scripts in dependency order.

Scripts read/write from: PROJECT_ROOT / data / <file>.csv
Runner patches PROJECT_ROOT to repo root so scripts find the right path.

Usage:
    python queries/runner.py                    # Run all blocks
    python queries/runner.py block1_markets     # Run single block
    python queries/runner.py --from block2_bad_debt
    python queries/runner.py --list
"""

import sys
import time
import argparse
import importlib
from pathlib import Path

QUERIES_DIR = Path(__file__).parent
REPO_ROOT = QUERIES_DIR.parent
DATA_DIR = REPO_ROOT / "data"

BLOCKS = [
    {
        "name": "block1_markets",
        "module": "block1_query_markets_graphql",
        "description": "Scan all chains for toxic collateral markets",
        "outputs": ["block1_markets_graphql.csv"],
        "inputs": [],
    },
    {
        "name": "block1_vaults",
        "module": "block1_query_vaults_graphql",
        "description": "3-phase vault discovery (current + historical + individual)",
        "outputs": ["block1_vaults_graphql.csv"],
        "inputs": ["block1_markets_graphql.csv"],
    },
    {
        "name": "block2_bad_debt",
        "module": "block2_query_markets",
        "description": "Market-level bad debt (realized + unrealized) + oracle architecture + state",
        "outputs": ["block2_bad_debt_by_market.csv"],
        "inputs": ["block1_markets_graphql.csv"],
    },
    {
        "name": "block2_share_prices",
        "module": "block2_query_share_prices",
        "description": "Daily + hourly share prices for all vaults",
        "outputs": [
            "block2_share_prices_daily.csv",
            "block2_share_prices_hourly.csv",
            "block2_share_price_summary.csv",
        ],
        "inputs": ["block1_vaults_graphql.csv"],
    },
    {
        "name": "block3_curator_A1",
        "module": "block3_curator_response_A1",
        "description": "Curator allocation timeseries (Part A1)",
        "outputs": ["block3_allocation_timeseries.csv"],
        "inputs": ["block1_vaults_graphql.csv", "block1_markets_graphql.csv"],
    },
    {
        "name": "block3_curator_A2",
        "module": "block3_curator_response_A2",
        "description": "Curator admin events (Part A2)",
        "outputs": ["block3_admin_events.csv"],
        "inputs": ["block1_vaults_graphql.csv", "block1_markets_graphql.csv"],
    },
    {
        "name": "block3_curator_B",
        "module": "block3_curator_response_B",
        "description": "Curator reallocations + classification (Part B)",
        "outputs": ["block3_reallocations.csv", "block3_curator_profiles.csv"],
        "inputs": [
            "block1_vaults_graphql.csv",
            "block1_markets_graphql.csv",
            "block3_allocation_timeseries.csv",
            "block3_admin_events.csv",
        ],
    },
    {
        "name": "block3b_liquidity",
        "module": "block3b_liquidity_stress",
        "description": "Market utilization + vault net flows during stress period",
        "outputs": [
            "block3_market_utilization_hourly.csv",
            "block3_market_utilization_daily.csv",
            "block3_vault_net_flows.csv",
            "block3_stress_comparison.csv",
        ],
        "inputs": [
            "block1_markets_graphql.csv",
            "block2_share_prices_daily.csv",
            "block2_share_price_summary.csv",
            "block3_allocation_timeseries.csv",
            "block3_curator_profiles.csv",
        ],
    },
    {
        "name": "block5_liquidation",
        "module": "block5_liquidation_breakdown",
        "description": "Oracle configs, borrower positions, liquidation events, LTV analysis",
        "outputs": [
            "block5_oracle_configs.csv",
            "block5_asset_prices.csv",
            "block5_collateral_at_risk.csv",
            "block5_borrower_positions.csv",
            "block5_liquidation_events.csv",
            "block5_ltv_analysis.csv",
        ],
        "inputs": ["block1_markets_graphql.csv"],
    },
    {
        "name": "block6_contagion",
        "module": "block6_contagion_analysis",
        "description": "Multi-exposure mapping + contagion bridges",
        "outputs": [
            "block6_vault_market_exposure.csv",
            "block6_vault_full_allocations.csv",
            "block6_public_allocator_config.csv",
            "block6_vault_reallocations.csv",
            "block6_pa_reallocations.csv",
            "block6_contagion_summary.csv",
        ],
        "inputs": ["block1_vaults_graphql.csv", "block1_markets_graphql.csv"],
    },
    {
        "name": "block7_withdrawals",
        "module": "block7_vault_withdrawals",
        "description": "Daily TVL timeseries for damaged vaults (bank-run analysis)",
        "outputs": [
            "block7_vault_tvl_daily.csv",
        ],
        "inputs": [],  # Reads block2_share_price_summary.csv if available, falls back to hardcoded list
    },
    {
        "name": "block8_plume_deep_dive",
        "module": "block8_query_plume_deep_dive",
        "description": "Plume sdeUSD/pUSD transaction history and oracle comparison (confirmed market resolved without loss)",
        "outputs": [
            "block8_plume_transactions.csv",
            "block8_plume_market_history.csv",
            "block8_plume_borrower_positions.csv",
            "block8_eth_transactions.csv",
            "block8_eth_market_history.csv",
            "block8_oracle_comparison.csv",
        ],
        "inputs": [],
    },
]


def patch_and_run(block: dict):
    """Import block module, ensure PROJECT_ROOT points to repo root, call main()."""
    module_name = block["module"]
    print(f"\n{'=' * 70}")
    print(f"▶ Running: {block['name']} — {block['description']}")
    print(f"{'=' * 70}")

    queries_dir = str(QUERIES_DIR)
    if queries_dir not in sys.path:
        sys.path.insert(0, queries_dir)

    if module_name in sys.modules:
        mod = importlib.reload(sys.modules[module_name])
    else:
        mod = importlib.import_module(module_name)

    # Ensure PROJECT_ROOT is repo root (scripts use PROJECT_ROOT / "data")
    if hasattr(mod, "PROJECT_ROOT"):
        mod.PROJECT_ROOT = REPO_ROOT

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    start = time.time()
    try:
        mod.main()
        elapsed = time.time() - start
        print(f"\n  ✅ {block['name']} completed in {elapsed:.1f}s")
    except Exception as e:
        elapsed = time.time() - start
        print(f"\n  ❌ {block['name']} FAILED after {elapsed:.1f}s: {e}")
        raise


def check_inputs(block: dict) -> list:
    return [f for f in block["inputs"] if not (DATA_DIR / f).exists()]


def run_blocks(block_names: list, skip_missing: bool = False):
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    for block in BLOCKS:
        if block["name"] not in block_names:
            continue
        missing = check_inputs(block)
        if missing:
            if skip_missing:
                print(f"\n⚠️  Skipping {block['name']} — missing inputs: {missing}")
                continue
            else:
                print(f"\n❌ Cannot run {block['name']} — missing inputs: {missing}")
                sys.exit(1)
        patch_and_run(block)
    print(f"\n{'=' * 70}")
    print(f"✅ Pipeline complete. CSVs in: {DATA_DIR}")
    print(f"{'=' * 70}")


def list_blocks():
    print("\nAvailable blocks:\n")
    for b in BLOCKS:
        print(f"  {b['name']:25s} — {b['description']}")
        if b["inputs"]:
            print(f"  {'':25s}   inputs: {', '.join(b['inputs'])}")
        print(f"  {'':25s}   outputs: {', '.join(b['outputs'])}")
        print()


def main():
    parser = argparse.ArgumentParser(description="Morpho query pipeline runner")
    parser.add_argument("blocks", nargs="*", help="Block names to run (default: all)")
    parser.add_argument("--list", action="store_true", help="List available blocks")
    parser.add_argument("--from", dest="from_block", help="Run from this block onwards")
    parser.add_argument("--skip-missing", action="store_true", help="Skip blocks with missing inputs")

    args = parser.parse_args()

    if args.list:
        list_blocks()
        return

    all_names = [b["name"] for b in BLOCKS]

    if args.from_block:
        if args.from_block not in all_names:
            print(f"Unknown block: {args.from_block}")
            list_blocks()
            sys.exit(1)
        idx = all_names.index(args.from_block)
        block_names = all_names[idx:]
    elif args.blocks:
        for name in args.blocks:
            if name not in all_names:
                print(f"Unknown block: {name}")
                list_blocks()
                sys.exit(1)
        block_names = args.blocks
    else:
        block_names = all_names

    print(f"📋 Will run: {' → '.join(block_names)}")
    print(f"📁 Data dir: {DATA_DIR}")
    run_blocks(block_names, skip_missing=args.skip_missing)


if __name__ == "__main__":
    main()
