"""Admin page — Reload data from Morpho API."""

import streamlit as st
import pandas as pd
import subprocess
import sys
from pathlib import Path
from datetime import datetime

DATA_DIR = Path(__file__).parent.parent / "data"
QUERIES_DIR = Path(__file__).parent.parent / "queries"


def get_csv_status() -> list:
    """Check which CSVs exist and their freshness."""
    files = sorted(DATA_DIR.glob("*.csv"))
    rows = []
    for f in files:
        stat = f.stat()
        rows.append({
            "file": f.name,
            "rows": sum(1 for _ in open(f)) - 1,
            "size_kb": stat.st_size / 1024,
            "modified": datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M"),
        })
    return rows


def render():
    st.title("Data Management")
    st.caption("Reload data from Morpho GraphQL API, or view current data status.")

    # -- Current Data Status ------------------------------------
    st.subheader("Current Data Files")

    csv_files = get_csv_status()
    if csv_files:
        df_status = pd.DataFrame(csv_files)
        st.dataframe(
            df_status,
            column_config={
                "file": "File",
                "rows": st.column_config.NumberColumn("Rows", format="%d"),
                "size_kb": st.column_config.NumberColumn("Size (KB)", format="%.1f"),
                "modified": "Last Modified",
            },
            hide_index=True,
            use_container_width=True,
        )
        total_rows = sum(f["rows"] for f in csv_files)
        st.caption(f"{len(csv_files)} files, {total_rows:,} total rows")
    else:
        st.warning("No data files found. Run the pipeline to fetch data.")

    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)

    # -- Pipeline Runner ----------------------------------------
    st.subheader("Reload Data from API")

    st.markdown("""
    Runs the query pipeline against the Morpho GraphQL API.
    Each block fetches fresh data and overwrites the CSVs.

    **Block execution order:**
    1. **Markets** — Scan all chains for toxic collateral
    2. **Vaults** — 3-phase discovery (current + historical + individual)
    3. **Bad Debt** — Per-market quantification
    4. **Share Prices** — Daily + hourly for all vaults
    5. **Curator Response** — Allocation timeseries + classification
    6. **Liquidity Stress** — Utilization + net flows
    7. **Liquidation** — Oracle configs, borrowers, LTV
    8. **Contagion** — Cross-market exposure mapping
    """)

    blocks = [
        ("block1_markets", "Block 1: Markets (scan all chains)"),
        ("block1_vaults", "Block 1: Vaults (3-phase discovery)"),
        ("block2_bad_debt", "Block 2: Bad Debt (per-market)"),
        ("block2_share_prices", "Block 2: Share Prices (daily + hourly)"),
        ("block3_curator", "Block 3: Curator Response"),
        ("block3b_liquidity", "Block 3b: Liquidity Stress"),
        ("block5_liquidation", "Block 5: Liquidation Breakdown"),
        ("block6_contagion", "Block 6: Contagion Analysis"),
    ]

    col1, col2 = st.columns([2, 1])

    with col1:
        selected = st.multiselect(
            "Select blocks to run:",
            options=[b[0] for b in blocks],
            default=[b[0] for b in blocks],
            format_func=lambda x: next(label for name, label in blocks if name == x),
            key="block_select",
        )

    with col2:
        skip_missing = st.checkbox("Skip blocks with missing inputs", value=True)

    if st.button("Run Pipeline", type="primary", use_container_width=True):
        if not selected:
            st.warning("Select at least one block to run.")
            return

        runner_path = QUERIES_DIR / "runner.py"
        if not runner_path.exists():
            st.error(f"Runner not found at {runner_path}")
            return

        cmd = [
            sys.executable, str(runner_path),
            "--data-dir", str(DATA_DIR),
        ]
        if skip_missing:
            cmd.append("--skip-missing")
        cmd.extend(selected)

        st.markdown("---")
        st.markdown("**Pipeline Output:**")

        with st.status("Running pipeline...", expanded=True) as status:
            try:
                process = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    timeout=600,
                    cwd=str(QUERIES_DIR.parent),
                )

                if process.stdout:
                    st.code(process.stdout, language="text")
                if process.stderr:
                    st.code(process.stderr, language="text")

                if process.returncode == 0:
                    status.update(label="Pipeline complete", state="complete")
                    st.success("Data refreshed. Navigate to other pages to see updated results.")
                    st.cache_data.clear()
                else:
                    status.update(label="Pipeline failed", state="error")
                    st.error(f"Exit code: {process.returncode}")

            except subprocess.TimeoutExpired:
                status.update(label="Pipeline timed out", state="error")
                st.error("Pipeline exceeded 10-minute timeout. Try running individual blocks.")
            except Exception as e:
                status.update(label="Error", state="error")
                st.error(f"Failed to run pipeline: {e}")
