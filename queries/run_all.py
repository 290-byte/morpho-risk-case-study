"""
Run all data collection queries and save to data/ directory.

This orchestrates the full data pipeline:
1. Block 1: Market discovery + vault exposure mapping
2. Block 2: Share price history + bad debt quantification
3. Block 3: Curator response + utilization + net flows
4. Block 5: Oracle analysis + borrower positions
5. Block 6: Contagion analysis

Usage: python queries/run_all.py

Note: This requires network access to the Morpho GraphQL API.
For offline use, the pre-computed CSVs in data/ are sufficient.
"""

import sys
from pathlib import Path

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

print("=" * 60)
print("  Morpho Risk Case Study — Data Collection Pipeline")
print("=" * 60)
print()
print("⚠️  Full query scripts are in the original analysis repo.")
print("    This placeholder shows the execution structure.")
print()
print("    To generate test data, run from the project root:")
print("    $ python generate_data.py")
print()
print("    To run actual API queries, add the individual query")
print("    scripts to this directory and import them here.")
print()

# When you add the actual query scripts, the structure would be:
#
# from queries.block1_markets import run as run_markets
# from queries.block1_vaults import run as run_vaults
# from queries.block2_share_prices import run as run_share_prices
# ...etc
#
# run_markets()
# run_vaults()
# run_share_prices()
# ...
