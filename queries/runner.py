"""
Query Runner — Adapter layer for block query scripts.

Each original block script uses:
    PROJECT_ROOT / "04-data-exports" / "raw" / "graphql" / <file>.csv

This adapter creates that directory structure under a workspace dir,
patches PROJECT_ROOT in each module, and runs them in dependency order.

Usage:
    # Run all blocks (full pipeline)
    python queries/runner.py

    # Run a single block
    python queries/runner.py block1_markets

    # Run from a specific block onwards
    python queries/runner.py --from block2_bad_debt

    # List available blocks
    python queries/runner.py --list

    # Use custom data directory
    python queries/runner.py --data-dir ./my_data
"""

import sys
import time
import shutil
import argparse
import importlib
from pathlib import Path

# ── Directory Setup ─────────────────────────────────────────
QUERIES_DIR = Path(__file__).parent
REPO_ROOT = QUERIES_DIR.parent
DEFAULT_DATA_DIR = REPO_ROOT / "data"

# Expected subdirectory structure (what the scripts expect under PROJECT_ROOT)
GQL_SUBDIR = Path("04-data-exports") / "raw" / "graphql"
DUNE_SUBDIR = Path("04-data-exports") / "raw" / "dune"

# ── Block Registry ──────────────────────────────────────────
# Order matters — each block depends on outputs from earlier blocks.
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
        "module": "block2_query_bad_debt_markets",
        "description": "Per-market bad debt quantification (3-layer analysis)",
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
        "name": "block3_curator_A",
        "module": "block3_curator_response_A",
        "description": "Curator allocation timeseries + admin events (Part A)",
        "outputs": [
            "block3_allocation_timeseries.csv",
            "block3_admin_events.csv",
        ],
        "inputs": ["block1_vaults_graphql.csv", "block1_markets_graphql.csv"],
    },
    {
        "name": "block3_curator_B",
        "module": "block3_curator_response_B",
        "description": "Curator reallocations + classification (Part B)",
        "outputs": [
            "block3_reallocations.csv",
            "block3_curator_profiles.csv",
        ],
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
        "inputs": ["block1_markets_graphql.csv"],
    },
]


def setup_workspace(data_dir: Path) -> Path:
    """
    Create the directory structure the scripts expect.
    Returns the 'fake' PROJECT_ROOT that will be patched into each module.

    Scripts expect: PROJECT_ROOT / 04-data-exports / raw / graphql / <file>.csv
    We create:      data_dir / _workspace / 04-data-exports / raw / graphql/
    """
    workspace = data_dir / "_workspace"
    gql_path = workspace / GQL_SUBDIR
    dune_path = workspace / DUNE_SUBDIR

    gql_path.mkdir(parents=True, exist_ok=True)
    dune_path.mkdir(parents=True, exist_ok=True)

    # Sync existing CSVs from data_dir into the workspace gql dir
    # (so downstream blocks can find upstream outputs)
    for csv_file in data_dir.glob("*.csv"):
        target = gql_path / csv_file.name
        if not target.exists():
            shutil.copy2(csv_file, target)

    return workspace


def sync_outputs_back(data_dir: Path):
    """Copy any new CSVs from workspace back to data_dir (flat)."""
    workspace_gql = data_dir / "_workspace" / GQL_SUBDIR
    if not workspace_gql.exists():
        return

    count = 0
    for csv_file in workspace_gql.glob("*.csv"):
        target = data_dir / csv_file.name
        shutil.copy2(csv_file, target)
        count += 1

    if count:
        print(f"   📁 Synced {count} CSVs back to {data_dir}")


def patch_and_run(block: dict, workspace: Path):
    """
    Import a block module, patch its PROJECT_ROOT, and call main().
    """
    module_name = block["module"]
    print(f"\n{'=' * 70}")
    print(f"▶ Running: {block['name']} — {block['description']}")
    print(f"  Module: {module_name}")
    print(f"{'=' * 70}")

    # Ensure queries/ is on the path
    queries_dir = str(QUERIES_DIR)
    if queries_dir not in sys.path:
        sys.path.insert(0, queries_dir)

    # Import (or reload) the module
    if module_name in sys.modules:
        mod = importlib.reload(sys.modules[module_name])
    else:
        mod = importlib.import_module(module_name)

    # Patch PROJECT_ROOT to our workspace
    if hasattr(mod, "PROJECT_ROOT"):
        original = mod.PROJECT_ROOT
        mod.PROJECT_ROOT = workspace
        print(f"  Patched PROJECT_ROOT: {original} → {workspace}")
    else:
        print(f"  ⚠️  No PROJECT_ROOT found in module — may fail")

    # Also ensure the output directory exists
    gql_dir = workspace / GQL_SUBDIR
    gql_dir.mkdir(parents=True, exist_ok=True)

    # Run
    start = time.time()
    try:
        mod.main()
        elapsed = time.time() - start
        print(f"\n  ✅ {block['name']} completed in {elapsed:.1f}s")
    except Exception as e:
        elapsed = time.time() - start
        print(f"\n  ❌ {block['name']} FAILED after {elapsed:.1f}s: {e}")
        raise


def check_inputs(block: dict, data_dir: Path) -> list:
    """Check which input files are missing."""
    workspace_gql = data_dir / "_workspace" / GQL_SUBDIR
    missing = []
    for inp in block["inputs"]:
        # Check both workspace and data_dir
        if not (workspace_gql / inp).exists() and not (data_dir / inp).exists():
            missing.append(inp)
    return missing


def run_blocks(block_names: list, data_dir: Path, skip_missing: bool = False):
    """Run a list of blocks in order."""
    workspace = setup_workspace(data_dir)

    for block in BLOCKS:
        if block["name"] not in block_names:
            continue

        # Check inputs
        missing = check_inputs(block, data_dir)
        if missing:
            if skip_missing:
                print(f"\n⚠️  Skipping {block['name']} — missing inputs: {missing}")
                continue
            else:
                print(f"\n❌ Cannot run {block['name']} — missing inputs: {missing}")
                print(f"   Run upstream blocks first, or use --skip-missing")
                sys.exit(1)

        patch_and_run(block, workspace)

        # Sync outputs back to data_dir after each block
        sync_outputs_back(data_dir)

    print(f"\n{'=' * 70}")
    print(f"✅ Pipeline complete. CSVs in: {data_dir}")
    print(f"{'=' * 70}")


def list_blocks():
    """Print available blocks and their dependencies."""
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
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR, help="Data directory")
    parser.add_argument("--skip-missing", action="store_true", help="Skip blocks with missing inputs")

    args = parser.parse_args()

    if args.list:
        list_blocks()
        return

    # Determine which blocks to run
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
    print(f"📁 Data dir: {args.data_dir}")

    args.data_dir.mkdir(parents=True, exist_ok=True)
    run_blocks(block_names, args.data_dir, skip_missing=args.skip_missing)


if __name__ == "__main__":
    main()
