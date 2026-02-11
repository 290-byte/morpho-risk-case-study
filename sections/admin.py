"""Section: Data Management — pipeline overview and source code reference."""

import streamlit as st
import pandas as pd
from pathlib import Path
from utils.data_loader import load_csv

DATA_DIR = Path(__file__).resolve().parent.parent / "data"


def render():
    st.title("Data Management")

    st.markdown(
        "All data in this dashboard is sourced from the **Morpho Blue GraphQL API** and "
        "on-chain contract reads. The pipeline is organized into query blocks that can be "
        "run independently or together."
    )

    # ── Source Code ──────────────────────────────────────────
    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
    st.subheader("Source Code")

    st.markdown(
        "The full source code for this case study — including all query scripts, "
        "the dashboard application, and data processing logic — is available on GitHub:"
    )
    st.markdown(
        "**[github.com/290-byte/morpho-risk-case-study]"
        "(https://github.com/290-byte/morpho-risk-case-study)**"
    )
    st.caption(
        "Includes: GraphQL query scripts, on-chain data fetchers, "
        "Streamlit dashboard code, Dockerfile, and documentation."
    )

    # ── Pipeline Overview ────────────────────────────────────
    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
    st.subheader("Query Pipeline")

    blocks = [
        ("Block 1 — Markets & Vaults", "block1",
         "Fetches all Morpho Blue markets using xUSD, deUSD, and sdeUSD as collateral, "
         "plus all MetaMorpho vaults with current or historical exposure to those markets.",
         ["block1_markets_graphql.csv", "block1_vaults_graphql.csv"]),
        ("Block 2 — Share Prices & Bad Debt", "block2",
         "Queries daily share price time series for all affected vaults and computes "
         "drawdown metrics. Calculates bad debt per market from supply/borrow gaps and "
         "oracle vs spot price analysis.",
         ["block2_share_prices_daily.csv", "block2_share_prices_hourly.csv",
          "block2_share_price_summary.csv", "block2_bad_debt_by_market.csv"]),
        ("Block 3 — Curator Response", "block3",
         "Pulls vault admin events (allocation changes, supply cap modifications), "
         "daily allocation time series, market utilization, and stress comparisons "
         "to reconstruct curator behavior during the crisis.",
         ["block3_admin_events.csv", "block3_allocation_timeseries.csv",
          "block3_curator_profiles.csv", "block3_reallocations.csv",
          "block3_market_utilization_daily.csv", "block3_market_utilization_hourly.csv",
          "block3_stress_comparison.csv", "block3_vault_net_flows.csv"]),
        ("Block 5 — Liquidation Analysis", "block5",
         "Examines liquidation mechanics: utilization rates, oracle configurations, "
         "LTV analysis, borrower positions, and why liquidations failed for these markets.",
         ["block5_oracle_configs.csv", "block5_ltv_analysis.csv",
          "block5_borrower_positions.csv", "block5_collateral_at_risk.csv",
          "block5_asset_prices.csv", "block5_liquidation_events.csv"]),
        ("Block 6 — Contagion Mapping", "block6",
         "Maps vault-to-market exposure pairs, identifies contagion bridges "
         "(vaults that hold both toxic and clean market positions), and "
         "analyzes public allocator configurations.",
         ["block6_vault_market_exposure.csv", "block6_vault_allocation_summary.csv",
          "block6_contagion_bridges.csv", "block6_market_connections.csv",
          "block6_vault_full_allocations.csv", "block6_vault_reallocations.csv"]),
        ("Timeline", "timeline",
         "Curated timeline of key events during the depeg crisis, with sources and links.",
         ["timeline_events.csv"]),
    ]

    for title, block_id, description, files in blocks:
        with st.container(border=True):
            st.markdown(f"**{title}**")
            st.caption(description)

            # Check which files exist
            found = []
            missing = []
            for f in files:
                if (DATA_DIR / f).exists():
                    found.append(f)
                else:
                    missing.append(f)

            if found:
                st.markdown(
                    "✅ " + " · ".join(f"`{f}`" for f in found)
                )
            if missing:
                st.markdown(
                    "⚠️ Missing: " + " · ".join(f"`{f}`" for f in missing)
                )

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
                    })
                except Exception:
                    file_info.append({
                        "File": f.name,
                        "Rows": "—",
                        "Columns": "—",
                        "Size": f"{f.stat().st_size / 1024:.1f} KB",
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
        st.caption("Auto-generated summary of all dashboard metrics. Regenerated on each app reload.")
        snapshot_text = snapshot_path.read_text(encoding="utf-8")
        import html
        escaped = html.escape(snapshot_text)
        st.markdown(
            f'<div style="max-height:600px; overflow-y:auto; background:#f8f9fa; '
            f'padding:16px; border-radius:8px; border:1px solid #e2e8f0; '
            f'font-family:monospace; font-size:12px; white-space:pre-wrap;">'
            f'{escaped}</div>',
            unsafe_allow_html=True,
        )
