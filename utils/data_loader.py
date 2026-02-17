"""
Centralized data loading -- adapts real block*.csv outputs to section schemas.

STRICT MODE: Only loads real pipeline output (block*.csv files).
No fallback to generated/fake data. If a block file is missing,
returns an empty DataFrame and the section should show an appropriate message.

Exception: timeline_events.csv is editorial (hand-written), not generated.

Block file mapping:
    block1_markets_graphql.csv      -> load_markets()
    block1_vaults_graphql.csv       -> load_vaults()
    block2_share_prices_daily.csv   -> load_share_prices()
    block2_share_price_summary.csv  -> merged into load_vaults()
    block3_curator_profiles.csv     -> merged into load_vaults()
    block3_vault_net_flows.csv      -> load_net_flows()
    block3_market_utilization_hourly.csv -> load_utilization()
    block5_asset_prices.csv         -> load_asset_prices()
    block5_ltv_analysis.csv         -> load_ltv()
    block5_borrower_positions.csv   -> load_borrowers()
    block6_contagion_bridges.csv    -> load_bridges()
    block6_vault_allocation_summary.csv -> load_exposure_summary()
    timeline_events.csv             -> load_timeline()  (editorial)

    Block8 (verified hourly ground truth):
    block8_*_market_history.csv     -> load_market_history_combined(), load_market_supply_at_depeg()
    block8_vault_tvl_hourly.csv     -> load_vault_tvl_hourly(), load_vault_tvl_at_depeg()
    block8_vault_allocation_hourly.csv -> load_vault_allocation_hourly(), load_vault_toxic_exposure_at_depeg()
"""

import streamlit as st
import pandas as pd
import numpy as np
from pathlib import Path

# Dashboard reads from data/ -- the runner syncs pipeline outputs here.
DATA_DIR = Path(__file__).parent.parent / "data"

# All block files the dashboard expects
_EXPECTED_FILES = [
    "block1_markets_graphql.csv",
    "block1_vaults_graphql.csv",
    "block2_share_prices_daily.csv",
    "block2_share_price_summary.csv",
    "block3_curator_profiles.csv",
    "block5_asset_prices.csv",
    # Block8 ground-truth hourly data
    "block8_eth_market_history.csv",
    "block8_vault_allocation_hourly.csv",
    "block8_vault_tvl_hourly.csv",
]
# ── helpers ─────────────────────────────────────────────────────

def _read(filename: str) -> pd.DataFrame:
    """Read CSV from data dir, return empty DataFrame if missing. Supports .csv.gz transparently."""
    path = DATA_DIR / filename
    gz_path = DATA_DIR / (filename + ".gz") if not filename.endswith(".gz") else path
    if gz_path.exists():
        df = pd.read_csv(gz_path, compression="gzip")
    elif path.exists():
        df = pd.read_csv(path)
    else:
        return pd.DataFrame()
    # Normalize: some block scripts use "blockchain", others use "chain"
    if "blockchain" in df.columns and "chain" not in df.columns:
        df = df.rename(columns={"blockchain": "chain"})
    if "chain" in df.columns:
        df["chain"] = df["chain"].astype(str).str.title()
    return df
def _market_label(row) -> str:
    """Build a short human-readable market label."""
    collat = row.get("collateral_symbol", row.get("collateral", "?"))
    loan = row.get("loan_symbol", row.get("loan", "?"))
    chain = row.get("chain", "")
    short_chain = str(chain)[:3].title() if chain else ""
    return f"{collat}/{loan} ({short_chain})" if short_chain else f"{collat}/{loan}"
def show_data_warnings():
    """Call from app.py to display any missing data warnings. Checks fresh each time."""
    missing = [f for f in _EXPECTED_FILES if not (DATA_DIR / f).exists() and not (DATA_DIR / (f + ".gz")).exists()]
    if missing:
        st.sidebar.warning(
            f"⚠️ Missing data files: {', '.join(missing)}. "
            "Run the query pipeline to generate them."
        )
# ═══════════════════════════════════════════════════════════════
#  LOADERS -- one per logical dataset the sections consume
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

    # Status -- derive from utilization + bad debt
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

    # block1_vaults is per vault-market pair -- aggregate to per vault
    vaults_raw["supply_assets_usd"] = pd.to_numeric(
        vaults_raw.get("supply_assets_usd", 0), errors="coerce"
    ).fillna(0)
    vaults_raw["vault_total_assets_usd"] = pd.to_numeric(
        vaults_raw.get("vault_total_assets_usd", 0), errors="coerce"
    ).fillna(0)

    # One row per vault: take first for scalar fields, sum for exposure
    agg_dict = {
        "vault_name": ("vault_name", "first"),
        "chain": ("chain", "first"),
        "curator_name": ("curator_name", "first"),
        "vault_total_assets_usd": ("vault_total_assets_usd", "first"),
        "exposure_usd": ("supply_assets_usd", "sum"),
        "collateral_symbol": ("collateral_symbol", lambda x: ", ".join(sorted(set(x.dropna().astype(str))))),
        "exposure_status": ("exposure_status", "first"),
        "discovery_method": ("discovery_method", "first"),
        "vault_listed": ("vault_listed", "first"),
        "timelock": ("timelock", "first"),
        "vault_share_price": ("vault_share_price", "first"),
    }
    # Carry deposit_asset_symbol through if available (needed for TVL inflation fix)
    if "deposit_asset_symbol" in vaults_raw.columns:
        agg_dict["deposit_asset_symbol"] = ("deposit_asset_symbol", "first")
    agg = vaults_raw.groupby("vault_address", as_index=False).agg(**agg_dict)

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

    # Timelock: raw is in seconds -> convert to days
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

    # ── Merge share price summary (drawdown + peak/trough TVL) ─
    sp_summary = _read("block2_share_price_summary.csv")
    if not sp_summary.empty and "max_drawdown_pct" in sp_summary.columns:
        sp_cols = ["vault_address", "max_drawdown_pct"]
        # Also grab peak/trough/pre-depeg TVL if available
        for extra in ["tvl_at_peak_usd", "tvl_at_trough_usd", "tvl_pre_depeg_usd",
                       "tvl_pre_depeg_native", "estimated_loss_usd"]:
            if extra in sp_summary.columns:
                sp_cols.append(extra)
        sp = sp_summary[sp_cols].drop_duplicates("vault_address")
        sp["vault_address"] = sp["vault_address"].str.lower()
        sp = sp.rename(columns={"max_drawdown_pct": "share_price_drawdown"})
        df = df.merge(sp, on="vault_address", how="left")

        # ── TVL INFLATION FIX ────────────────────────────────────
        # The API's historicalState.totalAssetsUsd returns inflated values
        # (market-level supply, not vault-level deposits).
        # tvl_pre_depeg_native (from totalAssets BigInt / 10^decimals) is correct.
        # For stablecoin vaults (USDC, USDT, DAI), native ≈ USD.
        if "tvl_pre_depeg_native" in df.columns:
            stablecoin_assets = {"USDC", "USDT", "DAI", "FRAX", "PYUSD", "GHO", "LUSD", "USDL"}
            is_stablecoin = df.get("deposit_asset_symbol", pd.Series(dtype=str)).str.upper().isin(stablecoin_assets)
            has_native = df["tvl_pre_depeg_native"].notna() & (df["tvl_pre_depeg_native"] > 0)
            mask = is_stablecoin & has_native

            # Override inflated USD TVL with correct native TVL
            df.loc[mask, "tvl_pre_depeg_usd"] = df.loc[mask, "tvl_pre_depeg_native"]
            df.loc[mask, "tvl_at_peak_usd"] = df.loc[mask, "tvl_pre_depeg_native"]

            # Recalculate estimated loss using corrected TVL
            if "share_price_drawdown" in df.columns:
                dd = df["share_price_drawdown"].abs()
                df.loc[mask, "estimated_loss_usd"] = df.loc[mask, "tvl_pre_depeg_usd"] * dd[mask]

    # ── Fill missing columns with defaults ───────────────────
    defaults = {
        "response_class": "UNKNOWN",
        "days_before_depeg": 0,
        "response_date": None,
        "share_price_drawdown": 0,
        "peak_allocation": 0,
        "tvl_at_peak_usd": 0,
        "tvl_at_trough_usd": 0,
        "tvl_pre_depeg_usd": 0,
        "tvl_pre_depeg_native": 0,
        "estimated_loss_usd": 0,
    }
    for col, default in defaults.items():
        if col not in df.columns:
            df[col] = default
        else:
            df[col] = df[col].fillna(default) if default is not None else df[col]

    # Ensure numerics
    for col in ["tvl_usd", "exposure_usd", "share_price", "share_price_drawdown",
                 "days_before_depeg", "timelock_days", "peak_allocation",
                 "tvl_at_peak_usd", "tvl_at_trough_usd", "tvl_pre_depeg_usd",
                 "tvl_pre_depeg_native", "estimated_loss_usd"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # share_price_drawdown: block2 gives as percentage (e.g. -3.13), section expects fraction (-0.0313)
    if df["share_price_drawdown"].abs().max() > 1:
        df["share_price_drawdown"] = df["share_price_drawdown"] / 100

    # Filter out test/invalid vault names
    if "vault_name" in df.columns:
        df = df[~df["vault_name"].str.contains("Duplicated Key|\\(Deployer\\)", case=False, na=False)]

    return df

def load_bad_debt_detail() -> pd.DataFrame:
    """
    Source: block2_bad_debt_by_market.csv (from block2_query_markets.py)
    Full bad-debt breakdown per market:
      - Layer 2: unrealized (badDebt) + realized (realizedBadDebt)
      - Oracle architecture: feed addresses, vault conversions, descriptions
      - Layer 1: supply-borrow gap
      - Layer 3: oracle price vs spot price
      - Warnings: BadDebtUnrealized metadata with badDebtShare
    """
    df = _read("block2_bad_debt_by_market.csv")
    if df.empty:
        return df

    # Build a friendly market label
    if "market_label" not in df.columns:
        df["market_label"] = df.apply(
            lambda r: f"{r.get('collateral_symbol','?')}/{r.get('loan_symbol','?')}"
                      f" ({r.get('chain','')})", axis=1)

    # Ensure numeric for key columns
    num_cols = [
        "L2_bad_debt_usd", "L2_realized_bad_debt_usd", "L2_total_bad_debt_usd",
        "L1_supply_usd", "L1_borrow_usd", "L1_gap_usd", "L1_collateral_usd",
        "L3_oracle_price_raw", "L3_oracle_price_normalized",
        "L3_oracle_spot_gap_pct", "L3_oracle_spot_gap_usd",
        "oracle_base_vault_conversion", "oracle_quote_vault_conversion",
        "supply_usd", "borrow_usd", "utilization", "liquidity_usd",
        "collateral_spot_price", "loan_spot_price",
        "lltv_pct", "oracle_ltv_pct", "true_ltv_pct",
        "warning_bad_debt_share", "warning_bad_debt_usd",
    ]
    for col in num_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # Oracle architecture: use pre-computed if available, else derive
    ZERO_ADDR = "0x0000000000000000000000000000000000000000"
    if "oracle_architecture" not in df.columns:
        def _classify_oracle(r):
            bf1 = str(r.get("oracle_base_feed_one", ""))
            bv  = str(r.get("oracle_base_vault", ""))
            has_feed = bf1 not in ("", "nan", ZERO_ADDR)
            has_vault = bv not in ("", "nan", ZERO_ADDR)
            if has_feed and has_vault:
                return "feed+vault"
            if has_feed:
                return "feed-based"
            if has_vault:
                return "vault-based"
            return "fixed-price"
        df["oracle_architecture"] = df.apply(_classify_oracle, axis=1)

    # Normalize oracle price: use pre-computed if available, else derive
    if "oracle_price_normalized" not in df.columns:
        def _normalize_price(r):
            raw = float(r.get("L3_oracle_price_raw", 0) or 0)
            sf = float(r.get("oracle_scale_factor", 1) or 1)
            if sf > 0 and raw > 0:
                return raw / sf
            return 0.0
        df["oracle_price_normalized"] = df.apply(_normalize_price, axis=1)

    # Build oracle description summary (human-readable)
    def _oracle_desc_summary(r):
        parts = []
        for col in ["oracle_base_feed_one_desc", "feed_base_one_desc",
                     "oracle_base_feed_two_desc", "feed_base_two_desc",
                     "oracle_quote_feed_one_desc", "feed_quote_one_desc",
                     "feed_base_vault_desc", "oracle_base_vault_vendor",
                     "feed_quote_vault_desc", "oracle_quote_vault_vendor"]:
            val = str(r.get(col, ""))
            if val and val != "nan" and val != "":
                parts.append(val)
        return " | ".join(parts) if parts else ""
    if "oracle_desc_summary" not in df.columns:
        df["oracle_desc_summary"] = df.apply(_oracle_desc_summary, axis=1)

    return df


def load_reallocations() -> pd.DataFrame:
    """
    Source: block3_reallocations.csv (from block3_curator_response_B.py)
    Vault reallocation events: assets moved in/out of markets by curators.
    """
    df = _read("block3_reallocations.csv")
    if df.empty:
        return df

    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_numeric(df["timestamp"], errors="coerce")
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
    if "assets" in df.columns:
        df["assets"] = pd.to_numeric(df["assets"], errors="coerce").fillna(0)
    if "shares" in df.columns:
        df["shares"] = pd.to_numeric(df["shares"], errors="coerce").fillna(0)

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

    # Map real block5 columns -> section schema
    if "symbol" in df.columns and "asset" not in df.columns:
        df = df.rename(columns={"symbol": "asset"})

    # Use datetime column for timestamp (more precise than date)
    if "datetime" in df.columns:
        df["timestamp"] = pd.to_datetime(df["datetime"])
    elif "date" in df.columns:
        df["timestamp"] = pd.to_datetime(df["date"])

    df["price_usd"] = pd.to_numeric(df["price_usd"], errors="coerce")

    # Some assets (xUSD) exist on multiple chains -- deduplicate by
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

    # Add liquidations_count (always 0 -- that's the whole point of this section)
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

def load_pre_depeg_exposure() -> pd.DataFrame:
    """
    Compute per-vault toxic exposure on Nov 3 2025 (day before xUSD depeg)
    from block3_allocation_timeseries.csv.

    This is the correct vault-level allocation data (supply_assets_usd per vault
    per toxic market per day). The block2 totalAssetsUsd field is MARKET-level
    and should NOT be used for vault TVL.

    Returns DataFrame with columns:
        vault_address, vault_name, chain, chain_id, curator_name,
        toxic_exposure_pre_depeg (sum of all toxic market allocations on Nov 3),
        n_toxic_markets, peak_toxic_exposure, peak_toxic_date
    """
    alloc = _read("block3_allocation_timeseries.csv")
    if alloc.empty or "supply_assets_usd" not in alloc.columns:
        return pd.DataFrame()

    alloc["supply_assets_usd"] = pd.to_numeric(alloc["supply_assets_usd"], errors="coerce").fillna(0)
    if "date" not in alloc.columns:
        return pd.DataFrame()

    # Normalize chain column
    if "blockchain" in alloc.columns and "chain" not in alloc.columns:
        alloc.rename(columns={"blockchain": "chain"}, inplace=True)

    # Pre-depeg: latest data point on or before Nov 3 2025
    pre_depeg = alloc[alloc["date"] <= "2025-11-03"]

    rows = []
    group_key = "vault_address" if "vault_address" in alloc.columns else "vault_name"
    for gid, g in alloc.groupby(group_key):
        g_pre = pre_depeg[pre_depeg[group_key] == gid] if not pre_depeg.empty else pd.DataFrame()

        # Pre-depeg exposure: sum across toxic markets on latest pre-depeg date
        pre_depeg_val = 0.0
        if not g_pre.empty:
            latest_date = g_pre["date"].max()
            day_data = g_pre[g_pre["date"] == latest_date]
            pre_depeg_val = day_data["supply_assets_usd"].sum()

        # Peak exposure across entire timeseries
        daily_totals = g.groupby("date")["supply_assets_usd"].sum()
        peak_val = daily_totals.max() if len(daily_totals) > 0 else 0
        peak_date = daily_totals.idxmax() if len(daily_totals) > 0 and peak_val > 0 else None

        # Count distinct toxic markets this vault was allocated to
        n_markets = g["market_unique_key"].nunique() if "market_unique_key" in g.columns else 0

        # Grab vault metadata from first row
        first = g.iloc[0]
        rows.append({
            "vault_address": gid if group_key == "vault_address" else first.get("vault_address", ""),
            "vault_name": first.get("vault_name", "?"),
            "chain": first.get("chain", ""),
            "chain_id": int(first.get("chain_id", 0)),
            "curator_name": first.get("curator_name", ""),
            "toxic_exposure_pre_depeg": pre_depeg_val,
            "peak_toxic_exposure": peak_val,
            "peak_toxic_date": peak_date,
            "n_toxic_markets": n_markets,
        })

    df = pd.DataFrame(rows)
    return df


def load_timeline() -> pd.DataFrame:
    """
    Source: timeline_events.csv (editorial, hand-written -- not generated)
    Section expects: date, event, category, severity
    """
    df = _read("timeline_events.csv")
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])
    return df
# ═══════════════════════════════════════════════════════════════
#  BLOCK 8 LOADERS -- verified hourly data (ground truth)
# ═══════════════════════════════════════════════════════════════

# Canonical toxic-market registry: market_id -> metadata + CSV filename
TOXIC_MARKETS = {
    "0x0f9563442d64ab3bd3bcb27058db0b0d4046a4c46f0acd811dacae9551d2b129": {
        "label": "sdeUSD/USDC Ethereum",
        "short": "sdeUSD/USDC Eth",
        "history_csv": "block8_eth_market_history.csv",
        "chain": "Ethereum",
        "chain_id": 1,
        "loan_decimals": 6,
    },
    "0x8d009383866dffaac5fe25af684e93f8dd5a98fed1991c298624ecc3a860f39f": {
        "label": "sdeUSD/pUSD Plume",
        "short": "sdeUSD/pUSD Plume",
        "history_csv": "block8_plume_market_history.csv",
        "chain": "Plume",
        "chain_id": 98866,
        "loan_decimals": 18,
    },
    "0x9e90aec7d768403dacc9dd0d8320307fda3f980eed4df43e3e52168a1c667709": {
        "label": "xUSD/USDC Arbitrum",
        "short": "xUSD/USDC Arb",
        "history_csv": "block8_arb_market_history.csv",
        "chain": "Arbitrum",
        "chain_id": 42161,
        "loan_decimals": 6,
    },
    "0xbd1ad3b968f5f0552dbd8cf1989a62881407c5cccf9e49fb3657c8731caf0c1f": {
        "label": "deUSD/USDC Ethereum",
        "short": "deUSD/USDC Eth",
        "history_csv": "block8_eth_deusd_market_history.csv",
        "chain": "Ethereum",
        "chain_id": 1,
        "loan_decimals": 6,
    },
    "0x82e7ab8ccabaac59b5f397507ed031ebf19a9a5b2657c00c93bc2423cd0a890d": {
        "label": "xUSD/USDC Plume [Elixir-Stream]",
        "short": "xUSD/USDC Plume (ES)",
        "history_csv": "block8_elixir_stream_market_history.csv",
        "chain": "Plume",
        "chain_id": 98866,
        "loan_decimals": 6,  # Both tokens 6-decimal; raw / 1e6 = USD
    },
}

DEPEG_TS = 1762128000  # Nov 3 00:00 UTC


def load_market_history_combined() -> pd.DataFrame:
    """
    Load all 5 toxic market hourly histories from block8 CSVs into one DataFrame.
    Columns: market_id, market_label, chain, timestamp, datetime, date,
             supply_usd, borrow_usd, utilization

    The Elixir-Stream market (0x82e7) has empty supply_usd because the API
    tags its loan asset as "unrecognized_loan_asset". We fall back to
    supply_assets (raw BigInt from the API) divided by 10^loan_decimals.
    Confirmed: USDC = 6 decimals, so 70888707404749 / 1e6 = $70.9M.
    """
    frames = []
    for mkt_id, info in TOXIC_MARKETS.items():
        df = _read(info["history_csv"])
        if df.empty:
            continue
        df["market_id"] = mkt_id
        df["market_label"] = info["short"]
        df["chain"] = info["chain"]
        # Normalize numeric columns (empty strings -> NaN -> 0)
        for col in ["supply_usd", "borrow_usd", "supply_assets", "borrow_assets",
                     "utilization", "timestamp"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

        # Fallback for unpriced markets: convert raw BigInt supply_assets to USD
        # Each market's loan_decimals is known from the TOXIC_MARKETS config
        loan_dec = info.get("loan_decimals", 6)
        divisor = 10 ** loan_dec
        if "supply_usd" in df.columns and "supply_assets" in df.columns:
            if df["supply_usd"].sum() == 0 and df["supply_assets"].sum() > 0:
                df["supply_usd"] = df["supply_assets"] / divisor
        if "borrow_usd" in df.columns and "borrow_assets" in df.columns:
            if df["borrow_usd"].sum() == 0 and df["borrow_assets"].sum() > 0:
                df["borrow_usd"] = df["borrow_assets"] / divisor

        frames.append(df)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def load_market_supply_at_depeg() -> pd.DataFrame:
    """
    Get each toxic market's supply and borrow at depeg (Nov 3 00:00 UTC).
    Returns: market_id, market_label, chain, supply_usd, borrow_usd, utilization
    """
    combined = load_market_history_combined()
    if combined.empty:
        return pd.DataFrame()

    rows = []
    for mkt_id, info in TOXIC_MARKETS.items():
        mkt = combined[combined["market_id"] == mkt_id]
        if mkt.empty:
            continue
        pre = mkt[mkt["timestamp"] <= DEPEG_TS]
        if pre.empty:
            continue
        snap = pre.loc[pre["timestamp"].idxmax()]
        rows.append({
            "market_id": mkt_id,
            "market_label": info["short"],
            "chain": info["chain"],
            "supply_usd": float(snap.get("supply_usd", 0)),
            "borrow_usd": float(snap.get("borrow_usd", 0)),
            "utilization": float(snap.get("utilization", 0)),
        })
    return pd.DataFrame(rows)


def load_vault_allocation_hourly() -> pd.DataFrame:
    """
    Source: block8_vault_allocation_hourly.csv
    Per vault, per market, per hour allocation in USD.
    Columns: vault_address, vault_name, chain_id, market_id, market_label,
             timestamp, datetime, date, supply_assets_usd
    """
    df = _read("block8_vault_allocation_hourly.csv")
    if df.empty:
        return df
    df["supply_assets_usd"] = pd.to_numeric(df["supply_assets_usd"], errors="coerce").fillna(0)
    df["timestamp"] = pd.to_numeric(df["timestamp"], errors="coerce").fillna(0)
    return df


def load_vault_tvl_hourly() -> pd.DataFrame:
    """
    Source: block8_vault_tvl_hourly.csv
    Per vault, per hour total TVL in USD.
    Columns: vault_address, vault_name, chain_id, timestamp, datetime, date, total_assets_usd
    """
    df = _read("block8_vault_tvl_hourly.csv")
    if df.empty:
        return df
    df["total_assets_usd"] = pd.to_numeric(df["total_assets_usd"], errors="coerce").fillna(0)
    df["timestamp"] = pd.to_numeric(df["timestamp"], errors="coerce").fillna(0)
    return df


def load_vault_toxic_exposure_at_depeg() -> pd.DataFrame:
    """
    For each vault, compute its allocation to each toxic market at depeg (Nov 3 00:00).
    Returns: vault_address, vault_name, chain_id, market_id, market_label,
             supply_usd_at_depeg, peak_supply_usd, peak_datetime
    All from block8_vault_allocation_hourly.csv -- no hardcoded values.
    """
    alloc = load_vault_allocation_hourly()
    if alloc.empty:
        return pd.DataFrame()

    toxic_ids = set(TOXIC_MARKETS.keys())
    toxic = alloc[alloc["market_id"].isin(toxic_ids)].copy()
    if toxic.empty:
        return pd.DataFrame()

    rows = []
    for (vault_addr, mkt_id), grp in toxic.groupby(["vault_address", "market_id"]):
        # Snapshot at depeg
        pre = grp[grp["timestamp"] <= DEPEG_TS]
        if pre.empty:
            depeg_val = 0.0
        else:
            snap = pre.loc[pre["timestamp"].idxmax()]
            depeg_val = float(snap["supply_assets_usd"])

        # Peak across full timeseries
        peak_idx = grp["supply_assets_usd"].idxmax()
        peak_val = float(grp.loc[peak_idx, "supply_assets_usd"])
        peak_dt = grp.loc[peak_idx, "datetime"] if "datetime" in grp.columns else ""

        # Only include if either depeg or peak > 0
        if depeg_val > 0 or peak_val > 100:
            info = TOXIC_MARKETS.get(mkt_id, {})
            rows.append({
                "vault_address": vault_addr,
                "vault_name": grp.iloc[0]["vault_name"],
                "chain_id": int(grp.iloc[0].get("chain_id", 0)),
                "market_id": mkt_id,
                "market_label": info.get("short", grp.iloc[0].get("market_label", "")),
                "supply_usd_at_depeg": depeg_val,
                "peak_supply_usd": peak_val,
                "peak_datetime": peak_dt,
            })

    df = pd.DataFrame(rows)
    # Filter out test vaults
    if not df.empty and "vault_name" in df.columns:
        df = df[~df["vault_name"].str.contains("Duplicated Key", case=False, na=False)]

    # Disambiguate duplicate vault names by appending chain
    if not df.empty:
        chain_map = {1: "Eth", 42161: "Arb", 98866: "Plume"}
        name_counts = df["vault_name"].value_counts()
        dupes = set(name_counts[name_counts > 1].index)
        if dupes:
            mask = df["vault_name"].isin(dupes)
            df.loc[mask, "vault_name"] = df.loc[mask].apply(
                lambda r: f"{r['vault_name']} ({chain_map.get(r['chain_id'], r['chain_id'])})",
                axis=1,
            )

    return df


def load_vault_tvl_at_depeg() -> pd.DataFrame:
    """
    Each vault's total TVL at depeg (Nov 3 00:00 UTC).
    Source: block8_vault_tvl_hourly.csv
    Returns: vault_address, vault_name, chain_id, tvl_at_depeg
    """
    tvl = load_vault_tvl_hourly()
    if tvl.empty:
        return pd.DataFrame()

    pre = tvl[tvl["timestamp"] <= DEPEG_TS]
    if pre.empty:
        return pd.DataFrame()

    idx = pre.groupby("vault_address")["timestamp"].idxmax()
    snap = pre.loc[idx, ["vault_address", "vault_name", "chain_id", "total_assets_usd"]].copy()
    snap = snap.rename(columns={"total_assets_usd": "tvl_at_depeg"})
    # Filter out test vaults
    snap = snap[~snap["vault_name"].str.contains("Duplicated Key", case=False, na=False)]
    return snap


# ── Generic loader (for admin page / ad-hoc use) ────────────
def load_csv(filename: str) -> pd.DataFrame:
    """Load any CSV from the data directory with caching."""
    return _read(filename)
