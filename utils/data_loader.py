"""
Centralized data loading with Streamlit caching.
Each CSV is loaded exactly once and cached across reruns.
"""

import streamlit as st
import pandas as pd
from pathlib import Path

DATA_DIR = Path(__file__).parent.parent / "data"


@st.cache_data(ttl=3600)
def load_csv(filename: str) -> pd.DataFrame:
    """Load a CSV from the data directory with caching."""
    path = DATA_DIR / filename
    if not path.exists():
        st.error(f"Missing data file: {filename}")
        return pd.DataFrame()
    return pd.read_csv(path)


def load_markets() -> pd.DataFrame:
    return load_csv("markets.csv")


def load_vaults() -> pd.DataFrame:
    return load_csv("vaults.csv")


def load_share_prices() -> pd.DataFrame:
    df = load_csv("share_prices_daily.csv")
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])
    return df


def load_asset_prices() -> pd.DataFrame:
    df = load_csv("asset_prices.csv")
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df


def load_net_flows() -> pd.DataFrame:
    df = load_csv("vault_net_flows.csv")
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])
    return df


def load_utilization() -> pd.DataFrame:
    df = load_csv("market_utilization_hourly.csv")
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df


def load_ltv() -> pd.DataFrame:
    return load_csv("ltv_analysis.csv")


def load_borrowers() -> pd.DataFrame:
    return load_csv("borrower_concentration.csv")


def load_bridges() -> pd.DataFrame:
    return load_csv("contagion_bridges.csv")


def load_exposure_summary() -> pd.DataFrame:
    return load_csv("exposure_summary.csv")


def load_timeline() -> pd.DataFrame:
    df = load_csv("timeline_events.csv")
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])
    return df
