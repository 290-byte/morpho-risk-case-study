"""Section: Data Management: pipeline overview, data freshness, and pipeline runner."""

import streamlit as st
import pandas as pd
import subprocess
import sys
import html
from pathlib import Path
from datetime import datetime, timezone

from utils.data_loader import load_csv

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
REPO_ROOT = Path(__file__).resolve().parent.parent
QUERIES_DIR = REPO_ROOT / "queries"
RUNNER_PATH = QUERIES_DIR / "runner.py"

# ── Mapping: admin block_id → runner block name(s) ──
# Each admin card may map to multiple runner blocks (e.g. block3 = A1 + A2 + B + 3b)
BLOCK_RUNNER_MAP = {
    "block1": ["block1_markets", "block1_vaults"],
    "block2": ["block2_bad_debt", "block2_share_prices"],
    "block3": ["block3_curator_A1", "block3_curator_A2", "block3_curator_B", "block3b_liquidity"],
    "block5": ["block5_liquidation"],
    "block6": ["block6_contagion"],
    "block7": ["block7_withdrawals"],
    "block8": ["block8_plume_deep_dive"],
}


def _file_age_str(path: Path) -> str:
    """Human-readable age of a file (e.g. '3h ago', '2d ago')."""
    if not path.exists():
        return "-"
    mtime = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
    now = datetime.now(tz=timezone.utc)
    delta = now - mtime
    secs = int(delta.total_seconds())
    if secs < 60:
        return "just now"
    if secs < 3600:
        return f"{secs // 60}m ago"
    if secs < 86400:
        return f"{secs // 3600}h ago"
    days = secs // 86400
    if days == 1:
        return "1d ago"
    return f"{days}d ago"


def _file_mtime_iso(path: Path) -> str:
    if not path.exists():
        return "-"
    mtime = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
    return mtime.strftime("%Y-%m-%d %H:%M UTC")


def _run_pipeline_streaming(block_names: list, log_widget) -> tuple:
    """
    Run the query pipeline via subprocess with live-streaming output.

    Args:
        block_names: list of runner block names (empty = all blocks)
        log_widget:  a st.empty() placeholder, updated on each line

    Returns:
        (success: bool, full_output: str)
    """
    if not RUNNER_PATH.exists():
        log_widget.code("Runner not found. Check queries/runner.py", language="text")
        return False, f"Runner not found at {RUNNER_PATH}"

    cmd = [sys.executable, "-u", str(RUNNER_PATH), "--skip-missing"] + block_names
    #                       ^^^ -u = unbuffered stdout for real-time streaming

    buffer = []
    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,   # merge stderr into stdout stream
            text=True,
            bufsize=1,                  # line-buffered
            cwd=str(REPO_ROOT),
        )

        for line in proc.stdout:
            buffer.append(line.rstrip("\n"))
            # Keep a scrolling window: show last 80 lines so it doesn't get huge
            visible = buffer[-80:] if len(buffer) > 80 else buffer
            log_widget.code("\n".join(visible), language="text")

        proc.wait(timeout=600)
        full_output = "\n".join(buffer)
        return (proc.returncode == 0), full_output

    except subprocess.TimeoutExpired:
        proc.kill()
        buffer.append("\n⏱ Pipeline timed out after 10 minutes.")
        full_output = "\n".join(buffer)
        log_widget.code(full_output, language="text")
        return False, full_output
    except Exception as e:
        buffer.append(f"\n❌ Error: {e}")
        full_output = "\n".join(buffer)
        log_widget.code(full_output, language="text")
        return False, full_output


def render():
    st.title("Data Management")

    st.markdown(
        "All data in this dashboard is sourced from the **Morpho GraphQL API** and "
        "on-chain contract reads. The pipeline is organized into query blocks that can be "
        "run independently or together."
    )

    # ── Source Code ──────────────────────────────────────────
    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
    st.subheader("Source Code")

    st.markdown(
        "The full source code for this case study, including all query scripts, "
        "the dashboard application, and data processing logic, is available on GitHub:"
    )
    st.markdown(
        "**[github.com/290-byte/morpho-risk-case-study]"
        "(https://github.com/290-byte/morpho-risk-case-study)**"
    )
    st.caption(
        "Includes: GraphQL query scripts, on-chain data fetchers, "
        "Streamlit dashboard code, Dockerfile, and documentation."
    )

    # ── Pipeline Runner ───────────────────────────────────────
    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
    st.subheader("Refresh Pipeline")

    runner_exists = RUNNER_PATH.exists()

    if not runner_exists:
        st.warning(
            f"Pipeline runner not found at `{RUNNER_PATH}`. "
            "Ensure `queries/runner.py` exists in the repository root."
        )

    st.caption(
        "Run individual query blocks or the full pipeline to refresh data from the "
        "Morpho GraphQL API. Each block writes CSVs to the `data/` directory. "
        "The dashboard reloads automatically after a refresh."
    )

    col_run_all, col_spacer = st.columns([1, 3])
    with col_run_all:
        run_all = st.button(
            "🔄 Run Full Pipeline",
            disabled=not runner_exists,
            type="primary",
            use_container_width=True,
        )

    if run_all:
        with st.status("Running full pipeline...", expanded=True) as status:
            st.write("Executing all blocks in dependency order...")
            log_area = st.empty()
            success, output = _run_pipeline_streaming([], log_area)
            if success:
                status.update(label="✅ Pipeline complete", state="complete")
                st.cache_data.clear()
                st.rerun()
            else:
                status.update(label="❌ Pipeline failed", state="error")

    # ── Query Pipeline (per-block cards) ──────────────────────
    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
    st.subheader("Query Pipeline")

    blocks = [
        ("Block 1: Markets & Vaults", "block1",
         "Fetches all Morpho markets using xUSD, deUSD, and sdeUSD as collateral, "
         "plus all MetaMorpho vaults with current or historical exposure to those markets.",
         ["block1_markets_graphql.csv", "block1_vaults_graphql.csv"]),
        ("Block 2: Share Prices & Bad Debt", "block2",
         "Queries daily share price time series for all affected vaults and computes "
         "drawdown metrics. Calculates bad debt per market from supply/borrow gaps and "
         "oracle vs spot price analysis.",
         ["block2_share_prices_daily.csv", "block2_share_prices_hourly.csv",
          "block2_share_price_summary.csv", "block2_bad_debt_by_market.csv"]),
        ("Block 3: Curator Response", "block3",
         "Pulls vault admin events (allocation changes, supply cap modifications), "
         "daily allocation time series, market utilization, and stress comparisons "
         "to reconstruct curator behavior during the crisis.",
         ["block3_admin_events.csv", "block3_allocation_timeseries.csv",
          "block3_curator_profiles.csv", "block3_reallocations.csv",
          "block3_market_utilization_daily.csv", "block3_market_utilization_hourly.csv",
          "block3_stress_comparison.csv", "block3_vault_net_flows.csv"]),
        ("Block 5: Liquidation Analysis", "block5",
         "Examines liquidation mechanics: utilization rates, oracle configurations, "
         "LTV analysis, borrower positions, and why liquidations failed for these markets.",
         ["block5_oracle_configs.csv", "block5_ltv_analysis.csv",
          "block5_borrower_positions.csv", "block5_collateral_at_risk.csv",
          "block5_asset_prices.csv", "block5_liquidation_events.csv"]),
        ("Block 6: Contagion Mapping", "block6",
         "Maps vault-to-market exposure pairs, identifies contagion bridges "
         "(vaults that hold both toxic and clean market positions), and "
         "analyzes public allocator configurations.",
         ["block6_vault_market_exposure.csv", "block6_vault_allocation_summary.csv",
          "block6_contagion_bridges.csv", "block6_market_connections.csv",
          "block6_vault_full_allocations.csv", "block6_vault_reallocations.csv"]),
        ("Block 7: Bank-Run Analysis", "block7",
         "Daily TVL timeseries for damaged vaults. Combined with block3 allocation data, "
         "proves bank-run dynamics: most TVL was safely withdrawn, with the share price crash "
         "caused by curator force-removal of toxic markets, not by total TVL destruction.",
         ["block7_vault_tvl_daily.csv"]),
        ("Block 8: Plume Deep Dive", "block8",
         "Transaction-level verification of the Plume sdeUSD/pUSD market, where block3 "
         "utilization data alone was inconclusive. Confirmed the sole borrower repaid "
         "voluntarily (no liquidation), allowing vaults to withdraw in full. Also fetches "
         "the Ethereum sdeUSD/USDC market for oracle comparison.",
         ["block8_plume_transactions.csv", "block8_plume_market_history.csv",
          "block8_plume_borrower_positions.csv", "block8_eth_transactions.csv",
          "block8_eth_market_history.csv", "block8_oracle_comparison.csv"]),
        ("Timeline", "timeline",
         "Curated timeline of key events during the depeg crisis, with sources and links.",
         ["timeline_events.csv"]),
    ]

    for title, block_id, description, files in blocks:
        with st.container(border=True):
            col_info, col_action = st.columns([5, 1])

            with col_info:
                st.markdown(f"**{title}**")
                st.caption(description)

                # Check which files exist + show freshness
                found = []
                missing = []
                for f in files:
                    fpath = DATA_DIR / f
                    if fpath.exists():
                        age = _file_age_str(fpath)
                        found.append(f"`{f}` ({age})")
                    else:
                        missing.append(f)

                if found:
                    st.markdown("✅ " + " · ".join(found))
                if missing:
                    st.markdown(
                        "⚠️ Missing: " + " · ".join(f"`{f}`" for f in missing)
                    )

            with col_action:
                # Only show run button for blocks that have runner mappings
                runner_blocks = BLOCK_RUNNER_MAP.get(block_id, [])
                can_run = runner_exists and len(runner_blocks) > 0
                btn_key = f"run_{block_id}"

                if st.button(
                    "▶ Run",
                    key=btn_key,
                    disabled=not can_run,
                    use_container_width=True,
                ):
                    st.session_state[f"_running_{block_id}"] = True

        # Show runner output below the card (outside container for full width)
        if st.session_state.get(f"_running_{block_id}"):
            with st.status(f"Running {title}...", expanded=True) as status:
                block_names = BLOCK_RUNNER_MAP[block_id]
                st.write(f"Executing: `{' → '.join(block_names)}`")
                log_area = st.empty()
                success, output = _run_pipeline_streaming(block_names, log_area)
                if success:
                    status.update(label=f"✅ {title} complete", state="complete")
                else:
                    status.update(label=f"❌ {title} failed", state="error")
            st.session_state[f"_running_{block_id}"] = False
            st.cache_data.clear()

    # ── Data Files ───────────────────────────────────────────
    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
    st.subheader("Loaded Data Files")

    if DATA_DIR.exists():
        csv_files = sorted(DATA_DIR.glob("*.csv"))
        if csv_files:
            file_info = []
            for f in csv_files:
                try:
                    df = pd.read_csv(f, nrows=0)
                    n_rows = sum(1 for _ in open(f)) - 1
                    file_info.append({
                        "File": f.name,
                        "Rows": n_rows,
                        "Columns": len(df.columns),
                        "Size": f"{f.stat().st_size / 1024:.1f} KB",
                        "Last Updated": _file_mtime_iso(f),
                        "Age": _file_age_str(f),
                    })
                except Exception:
                    file_info.append({
                        "File": f.name,
                        "Rows": "-",
                        "Columns": "-",
                        "Size": f"{f.stat().st_size / 1024:.1f} KB",
                        "Last Updated": _file_mtime_iso(f),
                        "Age": _file_age_str(f),
                    })

            st.dataframe(pd.DataFrame(file_info), hide_index=True, use_container_width=True)
        else:
            st.info("No CSV files found in the data directory.")
    else:
        st.warning("Data directory not found. Run the query pipeline first.")

    # ── Snapshot ─────────────────────────────────────────────
    snapshot_path = DATA_DIR / "snapshot.txt"
    if snapshot_path.exists():
        st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
        st.subheader("Current Snapshot")
        st.caption(
            f"Auto-generated summary of all dashboard metrics. "
            f"Last generated: {_file_mtime_iso(snapshot_path)}"
        )
        snapshot_text = snapshot_path.read_text(encoding="utf-8")
        escaped = html.escape(snapshot_text)
        st.markdown(
            f'<div style="max-height:600px; overflow-y:auto; background:#f8f9fa; '
            f'padding:16px; border-radius:8px; border:1px solid #e2e8f0; '
            f'font-family:monospace; font-size:12px; white-space:pre-wrap;">'
            f'{escaped}</div>',
            unsafe_allow_html=True,
        )
