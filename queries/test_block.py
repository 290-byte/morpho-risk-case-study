#!/usr/bin/env python
"""
Quick test — run a single block and inspect outputs.

Usage:
    # From repo root:
    python queries/test_block.py block1_markets
    python queries/test_block.py block2_bad_debt --inspect
    python queries/test_block.py --list
"""

import sys
import argparse
from pathlib import Path

# Add queries to path
sys.path.insert(0, str(Path(__file__).parent))

from runner import BLOCKS, setup_workspace, sync_outputs_back, patch_and_run, list_blocks
import pandas as pd


def inspect_outputs(block: dict, data_dir: Path):
    """Print summary of each output CSV."""
    print(f"\n{'─' * 50}")
    print(f"📊 Output Inspection")
    print(f"{'─' * 50}")

    for csv_name in block["outputs"]:
        csv_path = data_dir / csv_name
        if csv_path.exists():
            df = pd.read_csv(csv_path)
            print(f"\n  {csv_name}: {len(df)} rows × {len(df.columns)} cols")
            print(f"  Columns: {', '.join(df.columns[:10])}" + 
                  (f"... +{len(df.columns)-10}" if len(df.columns) > 10 else ""))
            if len(df) > 0:
                print(f"  Preview:")
                print(df.head(3).to_string(index=False, max_colwidth=40))
        else:
            print(f"\n  ⚠️  {csv_name}: NOT FOUND")


def main():
    parser = argparse.ArgumentParser(description="Test a single query block")
    parser.add_argument("block", nargs="?", help="Block name to run")
    parser.add_argument("--list", action="store_true", help="List available blocks")
    parser.add_argument("--inspect", action="store_true", help="Print output summary after run")
    parser.add_argument("--data-dir", type=Path, default=Path(__file__).parent.parent / "data")
    args = parser.parse_args()

    if args.list:
        list_blocks()
        return

    if not args.block:
        print("Specify a block name. Use --list to see options.")
        sys.exit(1)

    block_names = [b["name"] for b in BLOCKS]
    if args.block not in block_names:
        print(f"Unknown block: {args.block}")
        list_blocks()
        sys.exit(1)

    block = next(b for b in BLOCKS if b["name"] == args.block)

    print(f"🧪 Testing: {block['name']}")
    print(f"📁 Data dir: {args.data_dir}")

    args.data_dir.mkdir(parents=True, exist_ok=True)
    workspace = setup_workspace(args.data_dir)

    # Check inputs
    workspace_gql = workspace / "04-data-exports" / "raw" / "graphql"
    missing = []
    for inp in block["inputs"]:
        if not (workspace_gql / inp).exists() and not (args.data_dir / inp).exists():
            missing.append(inp)

    if missing:
        print(f"\n⚠️  Missing inputs: {missing}")
        print(f"   Run upstream blocks first, or place CSVs in {args.data_dir}")
        sys.exit(1)

    patch_and_run(block, workspace)
    sync_outputs_back(args.data_dir)

    if args.inspect:
        inspect_outputs(block, args.data_dir)

    print(f"\n✅ Done. Outputs in: {args.data_dir}")


if __name__ == "__main__":
    main()
