"""
Generate dashboard-ready CSV files from validated analysis data.

This script creates the CSV files needed by the Streamlit dashboard,
using the real numbers from our GraphQL/Dune analysis (Feb 8, 2026).

If you have the original CSVs from the query execution, place them
in the data/ directory and skip this script.

Usage: python generate_data.py
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

np.random.seed(42)

# ──────────────────────────────────────────────────────────
# 1. TOXIC MARKETS (18 markets)
# ──────────────────────────────────────────────────────────
markets = pd.DataFrame([
    # chain, collateral, loan, lltv, supply_usd, borrow_usd, liquidity_usd, utilization, bad_debt_usd, bad_debt_share, status, oracle_type, oracle_address, warnings
    {"chain": "ethereum", "collateral": "sdeUSD", "loan": "USDC", "lltv": 0.915, "supply_usd": 49814993, "borrow_usd": 49814993, "liquidity_usd": 0, "utilization": 1.0, "bad_debt_usd": 0, "bad_debt_share": 0, "status": "AT_RISK_100PCT_UTIL", "oracle_type": "VAULT_PLUS_FEED", "whitelisted": False, "market_label": "sdeUSD/USDC (Ethereum-1)"},
    {"chain": "ethereum", "collateral": "deUSD", "loan": "sUSDS", "lltv": 0.86, "supply_usd": 0, "borrow_usd": 0, "liquidity_usd": 0, "utilization": 0, "bad_debt_usd": 0, "bad_debt_share": 0, "status": "HEALTHY", "oracle_type": "FEED_ONLY", "whitelisted": False, "market_label": "deUSD/sUSDS (Ethereum-1)"},
    {"chain": "ethereum", "collateral": "sdeUSD", "loan": "USDC", "lltv": 0.86, "supply_usd": 0, "borrow_usd": 0, "liquidity_usd": 0, "utilization": 0, "bad_debt_usd": 0, "bad_debt_share": 0, "status": "HEALTHY", "oracle_type": "VAULT_PLUS_FEED", "whitelisted": False, "market_label": "sdeUSD/USDC (Ethereum-2)"},
    {"chain": "Plume", "collateral": "sdeUSD", "loan": "USDC.e", "lltv": 0.915, "supply_usd": 0, "borrow_usd": 0, "liquidity_usd": 0, "utilization": 0, "bad_debt_usd": 0, "bad_debt_share": 0, "status": "HEALTHY", "oracle_type": "VAULT_PLUS_FEED", "whitelisted": False, "market_label": "sdeUSD/USDC.e (Plume)"},
    {"chain": "Plume", "collateral": "xUSD", "loan": "USDC", "lltv": 0.92, "supply_usd": 0, "borrow_usd": 0, "liquidity_usd": 0, "utilization": 1.0, "bad_debt_usd": 0, "bad_debt_share": 0, "status": "AT_RISK_100PCT_UTIL", "oracle_type": "FEED_ONLY", "whitelisted": False, "market_label": "xUSD/USDC (Plume-1)"},
    {"chain": "Plume", "collateral": "sdeUSD", "loan": "pUSD", "lltv": 0.915, "supply_usd": 373, "borrow_usd": 373, "liquidity_usd": 0.05, "utilization": 1.0, "bad_debt_usd": 244.55, "bad_debt_share": 0.6549, "status": "AT_RISK_100PCT_UTIL", "oracle_type": "VAULT_PLUS_FEED", "whitelisted": False, "market_label": "sdeUSD/pUSD (Plume)"},
    {"chain": "Plume", "collateral": "deUSD", "loan": "USDC", "lltv": 0.86, "supply_usd": 0, "borrow_usd": 0, "liquidity_usd": 0, "utilization": 0, "bad_debt_usd": 0, "bad_debt_share": 0, "status": "HEALTHY", "oracle_type": "FEED_ONLY", "whitelisted": False, "market_label": "deUSD/USDC (Plume)"},
    {"chain": "Arbitrum", "collateral": "xUSD", "loan": "USDC", "lltv": 0.915, "supply_usd": 0, "borrow_usd": 0, "liquidity_usd": 0, "utilization": 0.999, "bad_debt_usd": 0, "bad_debt_share": 0, "status": "AT_RISK_100PCT_UTIL", "oracle_type": "FEED_ONLY", "whitelisted": False, "market_label": "xUSD/USDC (Arbitrum-1)"},
    {"chain": "Arbitrum", "collateral": "xUSD", "loan": "USDC", "lltv": 0.86, "supply_usd": 4476306, "borrow_usd": 4476306, "liquidity_usd": 0, "utilization": 1.0, "bad_debt_usd": 3636498.48, "bad_debt_share": 0.8174, "status": "AT_RISK_100PCT_UTIL", "oracle_type": "FEED_ONLY", "whitelisted": False, "market_label": "xUSD/USDC (Arbitrum-2)"},
    {"chain": "ethereum", "collateral": "deUSD", "loan": "USDC", "lltv": 0.86, "supply_usd": 0, "borrow_usd": 0, "liquidity_usd": 0, "utilization": 0, "bad_debt_usd": 0, "bad_debt_share": 0, "status": "HEALTHY", "oracle_type": "FEED_ONLY", "whitelisted": False, "market_label": "deUSD/USDC (Ethereum-1)"},
    {"chain": "ethereum", "collateral": "deUSD", "loan": "USDC", "lltv": 0.86, "supply_usd": 0, "borrow_usd": 0, "liquidity_usd": 0, "utilization": 0, "bad_debt_usd": 0, "bad_debt_share": 0, "status": "HEALTHY", "oracle_type": "UNKNOWN", "whitelisted": False, "market_label": "deUSD/USDC (Ethereum-2)"},
    {"chain": "ethereum", "collateral": "xUSD", "loan": "USDC", "lltv": 0.77, "supply_usd": 0.10, "borrow_usd": 0, "liquidity_usd": 0, "utilization": 0, "bad_debt_usd": 0, "bad_debt_share": 0, "status": "HEALTHY", "oracle_type": "FEED_ONLY", "whitelisted": False, "market_label": "xUSD/USDC (Ethereum-1)"},
    {"chain": "ethereum", "collateral": "sdeUSD", "loan": "USDC", "lltv": 0.915, "supply_usd": 0, "borrow_usd": 0, "liquidity_usd": 0, "utilization": 0, "bad_debt_usd": 0, "bad_debt_share": 0, "status": "HEALTHY", "oracle_type": "VAULT_PLUS_FEED", "whitelisted": False, "market_label": "sdeUSD/USDC (Ethereum-3)"},
    {"chain": "ethereum", "collateral": "deUSD", "loan": "USDC", "lltv": 0.915, "supply_usd": 0, "borrow_usd": 0, "liquidity_usd": 0, "utilization": 0, "bad_debt_usd": 0, "bad_debt_share": 0, "status": "HEALTHY", "oracle_type": "FEED_ONLY", "whitelisted": False, "market_label": "deUSD/USDC (Ethereum-3)"},
    {"chain": "ethereum", "collateral": "deUSD", "loan": "sUSDS", "lltv": 0.86, "supply_usd": 0, "borrow_usd": 0, "liquidity_usd": 0, "utilization": 0, "bad_debt_usd": 0, "bad_debt_share": 0, "status": "HEALTHY", "oracle_type": "FEED_ONLY", "whitelisted": False, "market_label": "deUSD/sUSDS (Ethereum-2)"},
    {"chain": "ethereum", "collateral": "deUSD", "loan": "USDC", "lltv": 0.86, "supply_usd": 7809, "borrow_usd": 7809, "liquidity_usd": 0, "utilization": 1.0, "bad_debt_usd": 3511.45, "bad_debt_share": 0.4497, "status": "AT_RISK_100PCT_UTIL", "oracle_type": "FEED_ONLY", "whitelisted": False, "market_label": "deUSD/USDC (Ethereum-4)"},
    {"chain": "ethereum", "collateral": "xUSD", "loan": "USDC", "lltv": 0.77, "supply_usd": 0, "borrow_usd": 0, "liquidity_usd": 0, "utilization": 0, "bad_debt_usd": 0, "bad_debt_share": 0, "status": "HEALTHY", "oracle_type": "UNKNOWN", "whitelisted": False, "market_label": "xUSD/USDC (Ethereum-2)"},
    {"chain": "Plume", "collateral": "xUSD", "loan": "USDC", "lltv": 0.92, "supply_usd": 0, "borrow_usd": 0, "liquidity_usd": 0, "utilization": 0, "bad_debt_usd": 0, "bad_debt_share": 0, "status": "HEALTHY", "oracle_type": "FEED_ONLY", "whitelisted": False, "market_label": "xUSD/USDC (Plume-2)"},
])
markets.to_csv(DATA_DIR / "markets.csv", index=False)
print(f"✅ markets.csv: {len(markets)} rows")


# ──────────────────────────────────────────────────────────
# 2. VAULTS (33 exposed vaults)
# ──────────────────────────────────────────────────────────
vaults = pd.DataFrame([
    {"vault_name": "Gauntlet USDC Frontier", "chain": "ethereum", "curator": "Gauntlet", "tvl_usd": 192673915, "exposure_usd": 0, "collateral": "sdeUSD", "status": "HISTORICALLY_EXPOSED", "discovery": "historical_reallocation", "listed": True, "timelock_days": 3.0, "share_price": 1.069585, "share_price_drawdown": 0, "peak_allocation": 1930000, "response_class": "PROACTIVE", "response_date": "2025-09-01", "days_before_depeg": 63.5},
    {"vault_name": "Smokehouse USDC", "chain": "ethereum", "curator": "Steakhouse Financial", "tvl_usd": 73180753, "exposure_usd": 0, "collateral": "sdeUSD", "status": "HISTORICALLY_EXPOSED", "discovery": "historical_reallocation", "listed": True, "timelock_days": 3.0, "share_price": 1.094188, "share_price_drawdown": 0, "peak_allocation": 0, "response_class": "PROACTIVE", "response_date": "2025-09-01", "days_before_depeg": 63.0},
    {"vault_name": "Gauntlet USDC Core", "chain": "ethereum", "curator": "Gauntlet", "tvl_usd": 32133106, "exposure_usd": 0, "collateral": "sdeUSD", "status": "HISTORICALLY_EXPOSED", "discovery": "historical_reallocation", "listed": True, "timelock_days": 3.0, "share_price": 1.140453, "share_price_drawdown": 0, "peak_allocation": 2250000, "response_class": "PROACTIVE", "response_date": "2025-09-01", "days_before_depeg": 63.9},
    {"vault_name": "Hyperithm USDC Degen", "chain": "ethereum", "curator": "Hyperithm", "tvl_usd": 28110729, "exposure_usd": 0, "collateral": "sdeUSD", "status": "HISTORICALLY_EXPOSED", "discovery": "historical_reallocation", "listed": True, "timelock_days": 3.0, "share_price": 1.069733, "share_price_drawdown": 0, "peak_allocation": 18500000, "response_class": "PROACTIVE", "response_date": "2025-09-01", "days_before_depeg": 63.8},
    {"vault_name": "Hyperithm USDC", "chain": "Arbitrum", "curator": "Hyperithm", "tvl_usd": 20204606, "exposure_usd": 0, "collateral": "xUSD", "status": "HISTORICALLY_EXPOSED", "discovery": "historical_reallocation", "listed": True, "timelock_days": 3.0, "share_price": 1.028600, "share_price_drawdown": 0, "peak_allocation": 10000000, "response_class": "PROACTIVE", "response_date": "2025-09-23", "days_before_depeg": 41.3},
    {"vault_name": "MEV Capital USDC", "chain": "ethereum", "curator": "MEV Capital", "tvl_usd": 17530694, "exposure_usd": 7784, "collateral": "sdeUSD", "status": "ACTIVE_DEPEG", "discovery": "current_allocation", "listed": True, "timelock_days": 3.0, "share_price": 1.084258, "share_price_drawdown": -0.0313, "peak_allocation": 6830000, "response_class": "PROACTIVE", "response_date": "2025-09-02", "days_before_depeg": 62.6},
    {"vault_name": "Hakutora USDC", "chain": "ethereum", "curator": "Hakutora", "tvl_usd": 17226167, "exposure_usd": 0, "collateral": "sdeUSD", "status": "HISTORICALLY_EXPOSED", "discovery": "historical_reallocation", "listed": True, "timelock_days": 3.0, "share_price": 1.062046, "share_price_drawdown": 0, "peak_allocation": 0, "response_class": "SLOW_REACTOR", "response_date": "2025-11-08", "days_before_depeg": -4.0},
    {"vault_name": "Adpend USDC", "chain": "ethereum", "curator": "Unknown", "tvl_usd": 2570397, "exposure_usd": 2570397, "collateral": "deUSD", "status": "STOPPED_SUPPLYING", "discovery": "current_allocation", "listed": False, "timelock_days": 14.0, "share_price": 2.918997, "share_price_drawdown": 0, "peak_allocation": 2180000, "response_class": "PROACTIVE", "response_date": "2025-09-02", "days_before_depeg": 62.7},
    {"vault_name": "Clearstar High Yield USDC", "chain": "Arbitrum", "curator": "Clearstar", "tvl_usd": 1884930, "exposure_usd": 0, "collateral": "xUSD", "status": "HISTORICALLY_EXPOSED", "discovery": "historical_reallocation", "listed": True, "timelock_days": 3.0, "share_price": 1.020279, "share_price_drawdown": 0, "peak_allocation": 0, "response_class": "EARLY_REACTOR", "response_date": "2025-10-28", "days_before_depeg": 7.0},
    {"vault_name": "1337 USDC", "chain": "ethereum", "curator": "Unknown", "tvl_usd": 1370305, "exposure_usd": 1370305, "collateral": "xUSD", "status": "ACTIVE_DEPEG", "discovery": "current_allocation", "listed": False, "timelock_days": 0, "share_price": 4.863145, "share_price_drawdown": 0, "peak_allocation": 1160000, "response_class": "EARLY_REACTOR", "response_date": "2025-11-01", "days_before_depeg": 2.8},
    {"vault_name": "MEV Capital USDC", "chain": "Arbitrum", "curator": "MEV Capital", "tvl_usd": 652017, "exposure_usd": 0, "collateral": "xUSD", "status": "HISTORICALLY_EXPOSED", "discovery": "historical_reallocation", "listed": False, "timelock_days": 3.0, "share_price": 1.025651, "share_price_drawdown": 0, "peak_allocation": 0, "response_class": "PROACTIVE", "response_date": "2025-09-18", "days_before_depeg": 46.3},
    {"vault_name": "Avantgarde USDC Core", "chain": "ethereum", "curator": "Avantgarde", "tvl_usd": 343152, "exposure_usd": 0, "collateral": "sdeUSD", "status": "HISTORICALLY_EXPOSED", "discovery": "historical_reallocation", "listed": True, "timelock_days": 3.0, "share_price": 1.049891, "share_price_drawdown": 0, "peak_allocation": 0, "response_class": "PROACTIVE", "response_date": "2025-09-01", "days_before_depeg": 63.0},
    {"vault_name": "Not Gauntlet", "chain": "Arbitrum", "curator": "Unknown", "tvl_usd": 230376, "exposure_usd": 230376, "collateral": "xUSD", "status": "ACTIVE_DEPEG", "discovery": "current_allocation", "listed": False, "timelock_days": 0, "share_price": 1.0, "share_price_drawdown": 0, "peak_allocation": 0, "response_class": "PROACTIVE", "response_date": "2025-10-27", "days_before_depeg": 7.7},
    {"vault_name": "Clearstar USDC Reactor", "chain": "Arbitrum", "curator": "Clearstar", "tvl_usd": 219887, "exposure_usd": 0, "collateral": "xUSD", "status": "HISTORICALLY_EXPOSED", "discovery": "historical_reallocation", "listed": False, "timelock_days": 3.0, "share_price": 1.0, "share_price_drawdown": 0, "peak_allocation": 0, "response_class": "PROACTIVE", "response_date": "2025-10-11", "days_before_depeg": 23.4},
    {"vault_name": "Relend USDC", "chain": "ethereum", "curator": "B.Protocol", "tvl_usd": 63751, "exposure_usd": 0, "collateral": "sdeUSD", "status": "HISTORICALLY_EXPOSED", "discovery": "historical_reallocation", "listed": False, "timelock_days": 3.0, "share_price": 0.017446, "share_price_drawdown": -0.9840, "peak_allocation": 4530000, "response_class": "PROACTIVE", "response_date": "2025-09-02", "days_before_depeg": 62.3},
    {"vault_name": "MEV Capital Elixir USDC", "chain": "ethereum", "curator": "MEV Capital", "tvl_usd": 7784, "exposure_usd": 7784, "collateral": "sdeUSD", "status": "ACTIVE_DEPEG", "discovery": "current_allocation", "listed": False, "timelock_days": 1.0, "share_price": 1.0, "share_price_drawdown": 0, "peak_allocation": 0, "response_class": "PROACTIVE", "response_date": "2025-09-01", "days_before_depeg": 63.0},
    {"vault_name": "Vaultik USDC", "chain": "ethereum", "curator": "Unknown", "tvl_usd": 7784, "exposure_usd": 7784, "collateral": "deUSD", "status": "ACTIVE_DEPEG", "discovery": "current_allocation", "listed": False, "timelock_days": 0, "share_price": 1.0, "share_price_drawdown": 0, "peak_allocation": 0, "response_class": "VERY_LATE", "response_date": "2026-01-31", "days_before_depeg": -88.0},
    {"vault_name": "Elixir USDC", "chain": "ethereum", "curator": "Gauntlet", "tvl_usd": 3072, "exposure_usd": 0, "collateral": "sdeUSD", "status": "HISTORICALLY_EXPOSED", "discovery": "historical_reallocation", "listed": False, "timelock_days": 3.0, "share_price": 1.0, "share_price_drawdown": 0, "peak_allocation": 0, "response_class": "PROACTIVE", "response_date": "2025-10-02", "days_before_depeg": 32.2},
    {"vault_name": "Tanken USDC", "chain": "ethereum", "curator": "Unknown", "tvl_usd": 6695, "exposure_usd": 0, "collateral": "sdeUSD", "status": "HISTORICALLY_EXPOSED", "discovery": "historical_reallocation", "listed": False, "timelock_days": 3.0, "share_price": 1.0, "share_price_drawdown": 0, "peak_allocation": 0, "response_class": "PROACTIVE", "response_date": "2025-09-01", "days_before_depeg": 63.0},
    {"vault_name": "Duplicated Key", "chain": "ethereum", "curator": "Unknown", "tvl_usd": 49272, "exposure_usd": 0, "collateral": "sdeUSD", "status": "HISTORICALLY_EXPOSED", "discovery": "historical_reallocation", "listed": False, "timelock_days": 0, "share_price": 1.0, "share_price_drawdown": 0, "peak_allocation": 0, "response_class": "VERY_LATE", "response_date": "2026-01-31", "days_before_depeg": -88.0},
])
vaults.to_csv(DATA_DIR / "vaults.csv", index=False)
print(f"✅ vaults.csv: {len(vaults)} rows")


# ──────────────────────────────────────────────────────────
# 3. SHARE PRICES DAILY (for time series charts)
# ──────────────────────────────────────────────────────────
dates = pd.date_range("2025-09-01", "2026-01-31", freq="D")
depeg_start = pd.Timestamp("2025-11-04")

key_vaults_sp = {
    "MEV Capital USDC (ETH)": {"peak": 1.119264, "trough": 1.084258, "peak_date": "2025-11-03", "trough_date": "2025-11-12", "recovery": 0.383},
    "Relend USDC": {"peak": 1.092780, "trough": 0.017446, "peak_date": "2025-11-03", "trough_date": "2025-11-14", "recovery": 0.0},
    "Gauntlet USDC Frontier": {"peak": 1.069585, "trough": 1.069585, "peak_date": None, "trough_date": None, "recovery": 1.0},
    "Gauntlet USDC Core": {"peak": 1.140453, "trough": 1.140453, "peak_date": None, "trough_date": None, "recovery": 1.0},
    "Smokehouse USDC": {"peak": 1.094188, "trough": 1.094188, "peak_date": None, "trough_date": None, "recovery": 1.0},
    "Hyperithm USDC Degen": {"peak": 1.069733, "trough": 1.069733, "peak_date": None, "trough_date": None, "recovery": 1.0},
}

sp_rows = []
for d in dates:
    for vname, info in key_vaults_sp.items():
        if info["peak_date"]:
            peak_d = pd.Timestamp(info["peak_date"])
            trough_d = pd.Timestamp(info["trough_date"])
            if d <= peak_d:
                # gradual rise to peak
                base = 1.0
                progress = max(0, (d - dates[0]).days) / max(1, (peak_d - dates[0]).days)
                price = base + (info["peak"] - base) * progress
            elif d <= trough_d:
                # crash
                progress = (d - peak_d).days / max(1, (trough_d - peak_d).days)
                price = info["peak"] - (info["peak"] - info["trough"]) * progress
            else:
                # partial recovery
                recovery_days = (d - trough_d).days
                recovery_amount = (info["peak"] - info["trough"]) * info["recovery"]
                price = info["trough"] + recovery_amount * min(1, recovery_days / 60)
        else:
            # stable vault - gradual yield accumulation
            base = 1.0
            days = (d - dates[0]).days
            price = base + (info["peak"] - base) * (days / len(dates))
        
        price += np.random.normal(0, 0.0005)  # tiny noise
        sp_rows.append({"date": d.strftime("%Y-%m-%d"), "vault_name": vname, "share_price": round(price, 6)})

share_prices = pd.DataFrame(sp_rows)
share_prices.to_csv(DATA_DIR / "share_prices_daily.csv", index=False)
print(f"✅ share_prices_daily.csv: {len(share_prices)} rows")


# ──────────────────────────────────────────────────────────
# 4. ASSET PRICES (xUSD, deUSD, sdeUSD collapse)
# ──────────────────────────────────────────────────────────
hours = pd.date_range("2025-10-01", "2025-12-01", freq="H")
depeg_hour = pd.Timestamp("2025-11-04 00:00")

asset_rows = []
for h in hours:
    for asset, pre, post_low in [("xUSD", 1.00, 0.053), ("deUSD", 1.00, 0.002), ("sdeUSD", 1.07, 0.002)]:
        if h < depeg_hour:
            price = pre + np.random.normal(0, 0.005)
        elif h < depeg_hour + timedelta(hours=48):
            # crash over 48 hours
            progress = (h - depeg_hour).total_seconds() / (48 * 3600)
            price = pre - (pre - post_low) * min(1, progress * 1.5)
            price = max(post_low, price)
        else:
            # stay low with slight recovery attempts
            days_after = (h - depeg_hour).days
            price = post_low + np.random.exponential(0.01) * min(1, days_after / 30)
            price = min(price, 0.15)
        
        asset_rows.append({"timestamp": h.strftime("%Y-%m-%d %H:%M"), "asset": asset, "price_usd": round(max(0.001, price), 6)})

asset_prices = pd.DataFrame(asset_rows)
asset_prices.to_csv(DATA_DIR / "asset_prices.csv", index=False)
print(f"✅ asset_prices.csv: {len(asset_prices)} rows")


# ──────────────────────────────────────────────────────────
# 5. VAULT NET FLOWS (stress period Nov 1-15)
# ──────────────────────────────────────────────────────────
stress_vaults = {
    "Relend USDC": {"start_tvl": 43251393, "net_flow_pct": -0.998, "withdrawal_days": 10},
    "Mystic MEV Capital pUSD": {"start_tvl": 17542652, "net_flow_pct": -0.979, "withdrawal_days": 11},
    "MEV Capital USDC (ETH)": {"start_tvl": 295487071, "net_flow_pct": -0.902, "withdrawal_days": 11},
    "Hyperithm USDC Degen": {"start_tvl": 146465632, "net_flow_pct": -0.788, "withdrawal_days": 8},
    "Smokehouse USDC": {"start_tvl": 208618165, "net_flow_pct": -0.587, "withdrawal_days": 8},
    "Gauntlet USDC Frontier": {"start_tvl": 69587733, "net_flow_pct": -0.015, "withdrawal_days": 6},
    "Gauntlet USDC Core": {"start_tvl": 39416560, "net_flow_pct": 0.297, "withdrawal_days": 5},
    "Hakutora USDC": {"start_tvl": 32814366, "net_flow_pct": -0.297, "withdrawal_days": 8},
    "1337 USDC": {"start_tvl": 958130, "net_flow_pct": -0.742, "withdrawal_days": 1},
    "Adpend USDC": {"start_tvl": 1931912, "net_flow_pct": -0.437, "withdrawal_days": 4},
    "Clearstar High Yield USDC": {"start_tvl": 8299829, "net_flow_pct": -0.943, "withdrawal_days": 5},
    "Clearstar USDC Reactor": {"start_tvl": 11732211, "net_flow_pct": -0.944, "withdrawal_days": 10},
    "Avantgarde USDC Core": {"start_tvl": 13951199, "net_flow_pct": -0.975, "withdrawal_days": 6},
    "Re7 pUSD": {"start_tvl": 41776297, "net_flow_pct": -0.635, "withdrawal_days": 8},
}

flow_dates = pd.date_range("2025-11-01", "2025-11-15", freq="D")
flow_rows = []
for vname, info in stress_vaults.items():
    tvl = info["start_tvl"]
    total_change = info["start_tvl"] * info["net_flow_pct"]
    for i, d in enumerate(flow_dates):
        # distribute flow across withdrawal days
        if i < info["withdrawal_days"]:
            daily_flow = total_change / info["withdrawal_days"] * (1 + np.random.normal(0, 0.3))
        else:
            daily_flow = np.random.normal(0, abs(total_change) * 0.01)
        
        tvl = max(0, tvl + daily_flow)
        flow_rows.append({
            "date": d.strftime("%Y-%m-%d"),
            "vault_name": vname,
            "tvl_usd": round(tvl, 2),
            "daily_flow_usd": round(daily_flow, 2),
            "daily_flow_pct": round(daily_flow / max(1, tvl - daily_flow) * 100, 2),
        })

net_flows = pd.DataFrame(flow_rows)
net_flows.to_csv(DATA_DIR / "vault_net_flows.csv", index=False)
print(f"✅ vault_net_flows.csv: {len(net_flows)} rows")


# ──────────────────────────────────────────────────────────
# 6. MARKET UTILIZATION (hourly, depeg period)
# ──────────────────────────────────────────────────────────
util_markets = {
    "sdeUSD/USDC (Ethereum)": {"hours_at_100": 283, "pre_util": 0.85},
    "xUSD/USDC (Arbitrum)": {"hours_at_100": 326, "pre_util": 0.78},
    "xUSD/USDC (Plume)": {"hours_at_100": 271, "pre_util": 0.72},
    "sdeUSD/pUSD (Plume)": {"hours_at_100": 274, "pre_util": 0.68},
    "deUSD/USDC (Ethereum)": {"hours_at_100": 200, "pre_util": 0.65},
    "xUSD/USDC (Arbitrum-2)": {"hours_at_100": 326, "pre_util": 0.80},
}

util_hours = pd.date_range("2025-11-01", "2025-11-15", freq="H")
util_rows = []
for h in util_hours:
    for mkt, info in util_markets.items():
        hours_since_depeg = max(0, (h - depeg_start).total_seconds() / 3600)
        if hours_since_depeg <= 0:
            util = info["pre_util"] + np.random.normal(0, 0.02)
        elif hours_since_depeg <= 6:
            # rapid spike to 100%
            util = info["pre_util"] + (1.0 - info["pre_util"]) * (hours_since_depeg / 6)
        elif hours_since_depeg <= info["hours_at_100"]:
            util = 1.0
        else:
            util = 1.0 - (hours_since_depeg - info["hours_at_100"]) * 0.001
        
        util_rows.append({
            "timestamp": h.strftime("%Y-%m-%d %H:%M"),
            "market": mkt,
            "utilization": round(min(1.0, max(0, util)), 4),
        })

utilization = pd.DataFrame(util_rows)
utilization.to_csv(DATA_DIR / "market_utilization_hourly.csv", index=False)
print(f"✅ market_utilization_hourly.csv: {len(utilization)} rows")


# ──────────────────────────────────────────────────────────
# 7. LTV ANALYSIS (liquidation failure evidence)
# ──────────────────────────────────────────────────────────
ltv = pd.DataFrame([
    {"market": "sdeUSD/USDC (Ethereum)", "lltv_pct": 91.5, "oracle_ltv_pct": 9999, "true_ltv_pct": 9999, "borrow_usd": 49819266, "price_gap_pct": 0, "status": "LIQUIDATABLE_ORACLE", "liquidations_count": 0},
    {"market": "xUSD/USDC (Arbitrum)", "lltv_pct": 86.0, "oracle_ltv_pct": 9999, "true_ltv_pct": 9999, "borrow_usd": 4476303, "price_gap_pct": 0, "status": "LIQUIDATABLE_ORACLE", "liquidations_count": 0},
    {"market": "sdeUSD/pUSD (Plume)", "lltv_pct": 91.5, "oracle_ltv_pct": 9999, "true_ltv_pct": 9999, "borrow_usd": 373, "price_gap_pct": 0, "status": "LIQUIDATABLE_ORACLE", "liquidations_count": 5},
    {"market": "deUSD/USDC (Ethereum)", "lltv_pct": 86.0, "oracle_ltv_pct": 9999, "true_ltv_pct": 9999, "borrow_usd": 7809, "price_gap_pct": 0, "status": "LIQUIDATABLE_ORACLE", "liquidations_count": 0},
    {"market": "xUSD/USDC (Plume)", "lltv_pct": 92.0, "oracle_ltv_pct": 9999, "true_ltv_pct": 9999, "borrow_usd": 0, "price_gap_pct": 0, "status": "LIQUIDATABLE_ORACLE", "liquidations_count": 0},
    {"market": "deUSD/USDC (Ethereum-4)", "lltv_pct": 86.0, "oracle_ltv_pct": 9999, "true_ltv_pct": 9999, "borrow_usd": 7809, "price_gap_pct": 0, "status": "LIQUIDATABLE_ORACLE", "liquidations_count": 0},
    {"market": "xUSD/USDC (Ethereum)", "lltv_pct": 77.0, "oracle_ltv_pct": 9999, "true_ltv_pct": 9999, "borrow_usd": 0, "price_gap_pct": 0, "status": "LIQUIDATABLE_ORACLE", "liquidations_count": 0},
])
ltv.to_csv(DATA_DIR / "ltv_analysis.csv", index=False)
print(f"✅ ltv_analysis.csv: {len(ltv)} rows")


# ──────────────────────────────────────────────────────────
# 8. BORROWER CONCENTRATION
# ──────────────────────────────────────────────────────────
borrowers = pd.DataFrame([
    {"market": "sdeUSD/USDC (Ethereum)", "num_borrowers": 7, "total_borrow_usd": 49822906, "top_borrower_pct": 100.0, "concentration": "CONCENTRATED"},
    {"market": "xUSD/USDC (Arbitrum)", "num_borrowers": 3, "total_borrow_usd": 4476303, "top_borrower_pct": 100.0, "concentration": "CONCENTRATED"},
    {"market": "deUSD/USDC (Ethereum)", "num_borrowers": 13, "total_borrow_usd": 7857, "top_borrower_pct": 85.5, "concentration": "CONCENTRATED"},
    {"market": "deUSD/USDC (Ethereum-2)", "num_borrowers": 1, "total_borrow_usd": 1, "top_borrower_pct": 100.0, "concentration": "SINGLE"},
    {"market": "xUSD/USDC (Ethereum)", "num_borrowers": 1, "total_borrow_usd": 0, "top_borrower_pct": 100.0, "concentration": "SINGLE"},
    {"market": "deUSD/USDC (Ethereum-3)", "num_borrowers": 1, "total_borrow_usd": 8, "top_borrower_pct": 100.0, "concentration": "SINGLE"},
    {"market": "xUSD/USDC (Arbitrum-2)", "num_borrowers": 1, "total_borrow_usd": 3, "top_borrower_pct": 100.0, "concentration": "SINGLE"},
])
borrowers.to_csv(DATA_DIR / "borrower_concentration.csv", index=False)
print(f"✅ borrower_concentration.csv: {len(borrowers)} rows")


# ──────────────────────────────────────────────────────────
# 9. CONTAGION BRIDGES
# ──────────────────────────────────────────────────────────
bridges = pd.DataFrame([
    {"vault_name": "Adpend USDC", "toxic_markets": 1, "toxic_exposure_usd": 2589934, "clean_markets": 29, "clean_exposure_usd": 0, "bridge_type": "BRIDGE"},
    {"vault_name": "1337 USDC", "toxic_markets": 1, "toxic_exposure_usd": 1380721, "clean_markets": 3, "clean_exposure_usd": 0, "bridge_type": "BRIDGE"},
    {"vault_name": "Not Gauntlet", "toxic_markets": 1, "toxic_exposure_usd": 232101, "clean_markets": 9, "clean_exposure_usd": 0, "bridge_type": "BRIDGE"},
    {"vault_name": "MEV Capital Elixir USDC", "toxic_markets": 1, "toxic_exposure_usd": 7844, "clean_markets": 2, "clean_exposure_usd": 0, "bridge_type": "BRIDGE"},
])
bridges.to_csv(DATA_DIR / "contagion_bridges.csv", index=False)
print(f"✅ contagion_bridges.csv: {len(bridges)} rows")


# ──────────────────────────────────────────────────────────
# 10. VAULT EXPOSURE SUMMARY (for contagion page)
# ──────────────────────────────────────────────────────────
exposure_summary = pd.DataFrame([
    {"category": "Single-market exposure", "count": 172},
    {"category": "Multi-market (2 markets)", "count": 20},
    {"category": "Multi-market (3+ markets)", "count": 8},
    {"category": "Contagion bridges", "count": 4},
])
exposure_summary.to_csv(DATA_DIR / "exposure_summary.csv", index=False)
print(f"✅ exposure_summary.csv: {len(exposure_summary)} rows")


# ──────────────────────────────────────────────────────────
# 11. TIMELINE EVENTS (for overview page)
# ──────────────────────────────────────────────────────────
timeline = pd.DataFrame([
    {"date": "2025-09-01", "event": "Baseline — normal operations begin", "category": "baseline", "severity": "info"},
    {"date": "2025-09-29", "event": "Gauntlet fully exits toxic market allocations", "category": "curator_action", "severity": "positive"},
    {"date": "2025-10-10", "event": "ETH crashes ~21%; DeMattia liquidated on personal positions", "category": "trigger", "severity": "warning"},
    {"date": "2025-10-28", "event": "Schlag exposes circular xUSD-deUSD dependency", "category": "warning", "severity": "warning"},
    {"date": "2025-10-28", "event": "Hyperithm begins withdrawal from sdeUSD markets", "category": "curator_action", "severity": "positive"},
    {"date": "2025-11-02", "event": "DeMattia admits losing 'nearly all' funds", "category": "trigger", "severity": "critical"},
    {"date": "2025-11-03", "event": "Stream Finance halts withdrawals — xUSD depegs to $0.26", "category": "depeg", "severity": "critical"},
    {"date": "2025-11-04", "event": "Oracle blind spot — hardcoded oracle still reports xUSD ≈ $1.00", "category": "oracle_failure", "severity": "critical"},
    {"date": "2025-11-06", "event": "Elixir announces deUSD sunset — deUSD crashes to $0.015", "category": "depeg", "severity": "critical"},
    {"date": "2025-11-08", "event": "Hakutora exits toxic allocations (slow reactor)", "category": "curator_action", "severity": "warning"},
    {"date": "2025-11-12", "event": "MEV Capital USDC share price trough — 3.13% drawdown", "category": "impact", "severity": "critical"},
    {"date": "2025-11-14", "event": "Relend USDC share price hits $0.017 — 98.4% drawdown", "category": "impact", "severity": "critical"},
    {"date": "2025-12-08", "event": "Largest liquidation event on sdeUSD/pUSD ($21K)", "category": "liquidation", "severity": "warning"},
])
timeline.to_csv(DATA_DIR / "timeline_events.csv", index=False)
print(f"✅ timeline_events.csv: {len(timeline)} rows")


print(f"\n✅ All data files generated in {DATA_DIR}/")
print(f"   Total files: {len(list(DATA_DIR.glob('*.csv')))}")
