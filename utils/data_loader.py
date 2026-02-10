"""
Centralized data loading — adapts real block*.csv outputs to section schemas.

STRICT MODE: Only loads real pipeline output (block*.csv files).
No fallback to generated/fake data. If a block file is missing,
returns an empty DataFrame and the section should show an appropriate message.

Exception: timeline_events.csv is editorial (hand-written), not generated.

Block file mapping:
    block1_markets_graphql.csv      → load_markets()
    block1_vaults_graphql.csv       → load_vaults()
    block2_share_prices_daily.csv   → load_share_prices()
    block2_share_price_summary.csv  → merged into load_vaults()
    block3_curator_profiles.csv     → merged into load_vaults()
    block3_vault_net_flows.csv      → load_net_flows()
    block3_market_utilization_hourly.csv → load_utilization()
    block5_asset_prices.csv         → load_asset_prices()
    block5_ltv_analysis.csv         → load_ltv()
    block5_borrower_positions.csv   → load_borrowers()
    block6_contagion_bridges.csv    → load_bridges()
    block6_vault_allocation_summary.csv → load_exposure_summary()
    timeline_events.csv             → load_timeline()  (editorial)
"""

import streamlit as st
import pandas as pd
import numpy as np
from pathlib import Path

# Dashboard reads from data/ — the runner syncs pipeline outputs here.
DATA_DIR = Path(__file__).parent.parent / "data"

# All block files the dashboard expects
_EXPECTED_FILES = [
    "block1_markets_graphql.csv",
    "block1_vaults_graphql.csv",
    "block2_share_prices_daily.csv",
    "block2_share_price_summary.csv",
    "block3_curator_profiles.csv",
    "block5_asset_prices.csv",
]
# ── helpers ─────────────────────────────────────────────────────

def _read(filename: str) -> pd.DataFrame:
    """Read CSV from data dir, return empty DataFrame if missing."""
    path = DATA_DIR / filename
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(path)
    # Normalize: some block scripts use "blockchain", others use "chain"
    if "blockchain" in df.columns and "chain" not in df.columns:
        df = df.rename(columns={"blockchain": "chain"})
    return df
def _market_label(row) -> str:
    """Build a short human-readable market label."""
    collat = row.get("collateral_symbol", row.get("collateral", "?"))
    loan = row.get("loan_symbol", row.get("loan", "?"))
    chain = row.get("chain", "")
    short_chain = str(chain)[:3].title() if chain else ""
    return f"{collat}/{loan} ({short_chain})"
def show_data_warnings():
    """Call from app.py to display any missing data warnings. Checks fresh each time."""
    missing = [f for f in _EXPECTED_FILES if not (DATA_DIR / f).exists()]
    if missing:
        st.sidebar.warning(
            f"⚠️ Missing data files: {', '.join(missing)}. "
            "Run the query pipeline to generate them."
        )
# ═══════════════════════════════════════════════════════════════
#  LOADERS — one per logical dataset the sections consume
# ═══════════════════════════════════════════════════════════════
def load_markets() -> pd.DataFrame:
    """
    Source: block1_markets_graphql.csv
    Section expects: chain, collateral, loan, lltv, supply_usd, borrow_usd,
    liquidity_usd, utilization, bad_debt_usd, bad_debt_share, status,
    oracle_type, whitelisted, market_label
    """
    df = _read("block1_markets_graphql.csv")
    if df.empty:
        return df

    rename = {
        "collateral_symbol": "collateral",
        "loan_symbol": "loan",
        "total_supply_usd": "supply_usd",
        "total_borrow_usd": "borrow_usd",
        "listed": "whitelisted",
    }
    df = df.rename(columns={k: v for k, v in rename.items() if k in df.columns})

    # Market label
    if "market_label" not in df.columns:
        df["market_label"] = df.apply(_market_label, axis=1)

    # Status — derive from utilization + bad debt
    if "status" not in df.columns:
        if "bad_debt_status" in df.columns:
            df["status"] = df["bad_debt_status"]
        else:
            def _status(r):
                util = float(r.get("utilization", 0) or 0)
                bd = float(r.get("bad_debt_usd", 0) or 0)
                if bd > 1000:
                    return "BAD_DEBT"
                if util >= 0.99:
                    return "AT_RISK_100PCT"
                if util >= 0.90:
                    return "AT_RISK_HIGH"
                return "ACTIVE"
            df["status"] = df.apply(_status, axis=1)

    # Bad debt share
    if "bad_debt_share" not in df.columns:
        supply = pd.to_numeric(df.get("supply_usd", 0), errors="coerce").fillna(0)
        bd = pd.to_numeric(df.get("bad_debt_usd", 0), errors="coerce").fillna(0)
        df["bad_debt_share"] = np.where(supply > 0, bd / supply, 0)

    # Ensure numeric
    for col in ["supply_usd", "borrow_usd", "liquidity_usd", "utilization",
                 "bad_debt_usd", "bad_debt_share", "lltv"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    return df

def load_vaults() -> pd.DataFrame:
    """
    Source: block1_vaults_graphql.csv + block3_curator_profiles.csv + block2_share_price_summary.csv
    Section expects: vault_name, chain, curator, tvl_usd, exposure_usd,
    collateral, status, discovery, listed, timelock_days, share_price,
    share_price_drawdown, peak_allocation, response_class, response_date,
    days_before_depeg
    """
    vaults_raw = _read("block1_vaults_graphql.csv")
    if vaults_raw.empty:
        return vaults_raw

    # block1_vaults is per vault-market pair — aggregate to per vault
    vaults_raw["supply_assets_usd"] = pd.to_numeric(
        vaults_raw.get("supply_assets_usd", 0), errors="coerce"
    ).fillna(0)
    vaults_raw["vault_total_assets_usd"] = pd.to_numeric(
        vaults_raw.get("vault_total_assets_usd", 0), errors="coerce"
    ).fillna(0)

    # One row per vault: take first for scalar fields, sum for exposure
    agg = vaults_raw.groupby("vault_address", as_index=False).agg(
        vault_name=("vault_name", "first"),
        chain=("chain", "first"),
        curator_name=("curator_name", "first"),
        vault_total_assets_usd=("vault_total_assets_usd", "first"),
        exposure_usd=("supply_assets_usd", "sum"),
        collateral_symbol=("collateral_symbol", lambda x: ", ".join(sorted(set(x.dropna().astype(str))))),
        exposure_status=("exposure_status", "first"),
        discovery_method=("discovery_method", "first"),
        vault_listed=("vault_listed", "first"),
        timelock=("timelock", "first"),
        vault_share_price=("vault_share_price", "first"),
    )

    # Rename to section schema
    df = agg.rename(columns={
        "curator_name": "curator",
        "vault_total_assets_usd": "tvl_usd",
        "collateral_symbol": "collateral",
        "exposure_status": "status",
        "discovery_method": "discovery",
        "vault_listed": "listed",
        "vault_share_price": "share_price",
    })

    # Timelock: raw is in seconds → convert to days
    df["timelock"] = pd.to_numeric(df.get("timelock", 0), errors="coerce").fillna(0)
    df["timelock_days"] = (df["timelock"] / 86400).round(1)

    # ── Merge curator profiles (response classification) ─────
    profiles = _read("block3_curator_profiles.csv")
    if not profiles.empty:
        prof_cols = ["vault_address"]
        for c in ["response_class", "days_vs_depeg", "earliest_action_date",
                   "peak_toxic_supply_usd"]:
            if c in profiles.columns:
                prof_cols.append(c)

        prof = profiles[prof_cols].drop_duplicates("vault_address")
        prof["vault_address"] = prof["vault_address"].str.lower()
        df["vault_address"] = df["vault_address"].str.lower()
        df = df.merge(prof, on="vault_address", how="left")

        col_rename = {
            "days_vs_depeg": "days_before_depeg",
            "earliest_action_date": "response_date",
            "peak_toxic_supply_usd": "peak_allocation",
        }
        df = df.rename(columns={k: v for k, v in col_rename.items() if k in df.columns})

    # ── Merge share price summary (drawdown) ─────────────────
    sp_summary = _read("block2_share_price_summary.csv")
    if not sp_summary.empty and "max_drawdown_pct" in sp_summary.columns:
        sp = sp_summary[["vault_address", "max_drawdown_pct"]].drop_duplicates("vault_address")
        sp["vault_address"] = sp["vault_address"].str.lower()
        sp = sp.rename(columns={"max_drawdown_pct": "share_price_drawdown"})
        df = df.merge(sp, on="vault_address", how="left")

    # ── Fill missing columns with defaults ───────────────────
    defaults = {
        "response_class": "UNKNOWN",
        "days_before_depeg": 0,
        "response_date": None,
        "share_price_drawdown": 0,
        "peak_allocation": 0,
    }
    for col, default in defaults.items():
        if col not in df.columns:
            df[col] = default
        else:
            df[col] = df[col].fillna(default) if default is not None else df[col]

    # Ensure numerics
    for col in ["tvl_usd", "exposure_usd", "share_price", "share_price_drawdown",
                 "days_before_depeg", "timelock_days", "peak_allocation"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # share_price_drawdown: block2 gives as percentage (e.g. -3.13), section expects fraction (-0.0313)
    if df["share_price_drawdown"].abs().max() > 1:
        df["share_price_drawdown"] = df["share_price_drawdown"] / 100

    # Filter out test/invalid vault names
    if "vault_name" in df.columns:
        df = df[~df["vault_name"].str.contains("Duplicated Key|\\(Deployer\\)", case=False, na=False)]

    return df

def load_share_prices() -> pd.DataFrame:
    """
    Source: block2_share_prices_daily.csv
    Section expects: date, vault_name, share_price (+ vault_address, chain if available)
    """
    df = _read("block2_share_prices_daily.csv")
    if df.empty:
        return df
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])
    if "share_price" in df.columns:
        df["share_price"] = pd.to_numeric(df["share_price"], errors="coerce")
    return df

def load_asset_prices() -> pd.DataFrame:
    """
    Source: block5_asset_prices.csv
    Section expects: timestamp, asset, price_usd
    """
    df = _read("block5_asset_prices.csv")
    if df.empty:
        return df

    # Map real block5 columns → section schema
    if "symbol" in df.columns and "asset" not in df.columns:
        df = df.rename(columns={"symbol": "asset"})

    # Use datetime column for timestamp (more precise than date)
    if "datetime" in df.columns:
        df["timestamp"] = pd.to_datetime(df["datetime"])
    elif "date" in df.columns:
        df["timestamp"] = pd.to_datetime(df["date"])

    df["price_usd"] = pd.to_numeric(df["price_usd"], errors="coerce")

    # Some assets (xUSD) exist on multiple chains — deduplicate by
    # keeping one price per (asset, date), preferring chain_id=1 (Ethereum)
    if "chain_id" in df.columns and "date" in df.columns:
        df = df.sort_values(["asset", "timestamp", "chain_id"])
        df = df.drop_duplicates(subset=["asset", "date"], keep="first")

    # Sort by asset + time to avoid Plotly drawing diagonals across gaps
    df = df.sort_values(["asset", "timestamp"]).reset_index(drop=True)

    return df

def load_net_flows() -> pd.DataFrame:
    """
    Source: block3_vault_net_flows.csv
    Section expects: date, vault_name, tvl_usd, daily_flow_usd, daily_flow_pct
    """
    df = _read("block3_vault_net_flows.csv")
    if df.empty:
        return df

    rename = {
        "total_assets_usd": "tvl_usd",
        "net_flow_usd": "daily_flow_usd",
        "net_flow_pct": "daily_flow_pct",
    }
    df = df.rename(columns={k: v for k, v in rename.items() if k in df.columns})

    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])

    for col in ["tvl_usd", "daily_flow_usd", "daily_flow_pct"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    return df

def load_utilization() -> pd.DataFrame:
    """
    Source: block3_market_utilization_hourly.csv
    Section expects: timestamp, market, utilization
    """
    df = _read("block3_market_utilization_hourly.csv")
    if df.empty:
        return df

    # Build market label
    if "market" not in df.columns:
        df["market"] = df.apply(_market_label, axis=1)

    # Timestamp
    if "datetime" in df.columns:
        df["timestamp"] = pd.to_datetime(df["datetime"])
    elif "timestamp" in df.columns:
        # If it's unix timestamp, convert
        ts = pd.to_numeric(df["timestamp"], errors="coerce")
        if ts.median() > 1e9:  # unix epoch
            df["timestamp"] = pd.to_datetime(ts, unit="s", utc=True)
        else:
            df["timestamp"] = pd.to_datetime(df["timestamp"])

    df["utilization"] = pd.to_numeric(df["utilization"], errors="coerce").fillna(0)

    # Deduplicate and sort to prevent diagonal lines
    if "market_unique_key" in df.columns:
        df = df.drop_duplicates(subset=["market", "timestamp"], keep="first")
    df = df.sort_values(["market", "timestamp"]).reset_index(drop=True)

    # Insert None rows at gaps > 6 hours to break Plotly lines
    parts = []
    for market, grp in df.groupby("market"):
        grp = grp.sort_values("timestamp").reset_index(drop=True)
        gaps = grp["timestamp"].diff()
        gap_indices = gaps[gaps > pd.Timedelta(hours=6)].index
        if len(gap_indices) > 0:
            rows = []
            for i, row in grp.iterrows():
                if i in gap_indices:
                    gap_row = row.copy()
                    gap_row["utilization"] = None
                    gap_row["timestamp"] = row["timestamp"] - pd.Timedelta(seconds=1)
                    rows.append(gap_row)
                rows.append(row)
            parts.append(pd.DataFrame(rows))
        else:
            parts.append(grp)
    if parts:
        df = pd.concat(parts, ignore_index=True)

    return df

def load_ltv() -> pd.DataFrame:
    """
    Source: block5_ltv_analysis.csv
    Section expects: market, lltv_pct, oracle_ltv_pct, true_ltv_pct,
    borrow_usd, price_gap_pct, status, liquidations_count
    """
    df = _read("block5_ltv_analysis.csv")
    if df.empty:
        return df

    # Build market label
    if "market" not in df.columns:
        df["market"] = df.apply(_market_label, axis=1)

    # Rename status
    if "liquidation_status" in df.columns and "status" not in df.columns:
        df = df.rename(columns={"liquidation_status": "status"})

    # Add liquidations_count (always 0 — that's the whole point of this section)
    if "liquidations_count" not in df.columns:
        df["liquidations_count"] = 0

    for col in ["lltv_pct", "oracle_ltv_pct", "true_ltv_pct", "borrow_usd", "price_gap_pct"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    return df

def load_borrowers() -> pd.DataFrame:
    """
    Source: block5_borrower_positions.csv
    Section expects: market, num_borrowers, total_borrow_usd,
    top_borrower_pct, concentration
    """
    df = _read("block5_borrower_positions.csv")
    if df.empty:
        return df

    # Filter to actual borrowers
    if "position_type" in df.columns:
        df = df[df["position_type"] == "borrower"].copy()

    if df.empty:
        return pd.DataFrame(columns=["market", "num_borrowers", "total_borrow_usd",
                                      "top_borrower_pct", "concentration"])

    # Build market label for grouping
    df["market"] = df.apply(_market_label, axis=1)
    df["borrow_assets_usd"] = pd.to_numeric(df["borrow_assets_usd"], errors="coerce").fillna(0)

    # Aggregate per market
    groups = []
    for market, grp in df.groupby("market"):
        total = grp["borrow_assets_usd"].sum()
        top = grp["borrow_assets_usd"].max()
        top_pct = (top / total * 100) if total > 0 else 0
        n = len(grp)

        if top_pct >= 90:
            conc = "EXTREME"
        elif top_pct >= 70:
            conc = "HIGH"
        elif top_pct >= 50:
            conc = "MODERATE"
        else:
            conc = "LOW"

        groups.append({
            "market": market,
            "num_borrowers": n,
            "total_borrow_usd": round(total, 2),
            "top_borrower_pct": round(top_pct, 1),
            "concentration": conc,
        })

    return pd.DataFrame(groups)

def load_bridges() -> pd.DataFrame:
    """
    Source: block6_contagion_bridges.csv
    Section expects: vault_name, toxic_markets, toxic_exposure_usd,
    clean_markets, clean_exposure_usd, bridge_type
    """
    df = _read("block6_contagion_bridges.csv")
    if df.empty:
        return df

    rename = {
        "n_toxic_markets": "toxic_markets",
        "toxic_supply_usd": "toxic_exposure_usd",
        "n_clean_markets": "clean_markets",
        "clean_supply_usd": "clean_exposure_usd",
        "contagion_path": "bridge_type",
    }
    df = df.rename(columns={k: v for k, v in rename.items() if k in df.columns})

    for col in ["toxic_exposure_usd", "clean_exposure_usd"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    return df

def load_exposure_summary() -> pd.DataFrame:
    """
    Source: block6_vault_allocation_summary.csv
    Section expects: category, count
    """
    df = _read("block6_vault_allocation_summary.csv")
    if df.empty:
        return df

    # Categorize vaults by toxic market count
    df["n_toxic_markets"] = pd.to_numeric(df.get("n_toxic_markets", 0), errors="coerce").fillna(0)

    # Also load bridges to identify bridge vaults
    bridges = _read("block6_contagion_bridges.csv")
    bridge_addrs = set()
    if not bridges.empty and "vault_address" in bridges.columns and "contagion_path" in bridges.columns:
        bridge_addrs = set(
            bridges.loc[bridges["contagion_path"] == "BRIDGE", "vault_address"]
            .str.lower()
        )

    categories = {"Single Market (1)": 0, "Multi-Market (2)": 0,
                   "High Risk (3+)": 0, "Contagion Bridge": 0}

    for _, row in df.iterrows():
        addr = str(row.get("vault_address", "")).lower()
        n = int(row["n_toxic_markets"])
        if addr in bridge_addrs:
            categories["Contagion Bridge"] += 1
        elif n >= 3:
            categories["High Risk (3+)"] += 1
        elif n == 2:
            categories["Multi-Market (2)"] += 1
        elif n == 1:
            categories["Single Market (1)"] += 1

    return pd.DataFrame([
        {"category": k, "count": v}
        for k, v in categories.items() if v > 0
    ])

def load_timeline() -> pd.DataFrame:
    """
    Source: timeline_events.csv (editorial, hand-written — not generated)
    Section expects: date, event, category, severity
    """
    df = _read("timeline_events.csv")
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])
    return df
# ── Generic loader (for admin page / ad-hoc use) ────────────
def load_csv(filename: str) -> pd.DataFrame:
    """Load any CSV from the data directory with caching."""
    return _read(filename)
