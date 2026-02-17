import os
import sys
import requests
import pandas as pd
import time
from pathlib import Path
from dotenv import load_dotenv
from typing import List, Dict, Set, Tuple
from datetime import datetime

# Load .env from project root
# Script lives at: 03-queries/block1-exposure/graphsql/script.py → 4 levels to /app/
PROJECT_ROOT = Path(__file__).parent.parent
env_path = PROJECT_ROOT / '.env'
load_dotenv(dotenv_path=env_path)

GRAPHQL_URL = "https://blue-api.morpho.org/graphql"

# Chain IDs for Morpho GraphQL API
CHAIN_IDS = {
    'ethereum': 1,
    'base': 8453,
    'arbitrum': 42161,
    'optimism': 10,
    'plume': 98866,
    'unichain': 130,
    'polygon': 137,
    'hyperevm': 999,
}

# Reverse map for chain name lookup
CHAIN_NAMES = {v: k for k, v in CHAIN_IDS.items()}

# Toxic collateral — filter by symbol (primary) and exclude false positives
TOXIC_COLLATERAL = ['xUSD', 'XUSD', 'deUSD', 'sdeUSD', 'deusd']
FALSE_POSITIVES = [
    'AA_FalconXUSDC', 'stakedao-crvfrxUSD', 'crvfrxUSD', 'sfrxUSD', 'fxUSD',
]

# Date range for analysis
START_DATE = "2025-09-01"
END_DATE = "2025-11-10"
DEPEG_START = "2025-11-04"
PRE_DEPEG_START = "2025-10-28"

# Rate limiting
API_DELAY = 0.3  # seconds between API calls


def query_graphql(query: str) -> dict:
    """Execute GraphQL query against Morpho API"""
    headers = {"Content-Type": "application/json"}
    response = requests.post(GRAPHQL_URL, json={"query": query}, headers=headers)
    response.raise_for_status()
    return response.json()


def safe_float(val, default=0.0):
    """Safely convert a value to float"""
    if val is None:
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def safe_int(val, default=0):
    """Safely convert a value to int"""
    if val is None:
        return default
    try:
        return int(val)
    except (ValueError, TypeError):
        return default


# ══════════════════════════════════════════════════════════════════════
#  VAULT GRAPHQL FRAGMENT — shared across all queries
# ══════════════════════════════════════════════════════════════════════

VAULT_FIELDS = """
    address
    name
    symbol
    listed
    featured
    creationTimestamp
    creatorAddress
    factory {
      address
    }
    asset {
      address
      symbol
      decimals
    }
    chain {
      id
      network
    }
    state {
      timestamp
      blockNumber
      totalAssets
      totalAssetsUsd
      totalSupply
      sharePriceNumber
      sharePriceUsd
      apy
      netApy
      fee
      timelock
      curator
      guardian
      owner
      feeRecipient
      curators {
        id
        name
        image
        verified
      }
      curatorMetadata(first: 10) {
        items {
          type
          metadata {
            ... on SafeAddressMetadata {
              owners
              threshold
            }
          }
        }
      }
      allocation {
        market {
          uniqueKey
          loanAsset {
            symbol
            address
          }
          collateralAsset {
            symbol
            address
          }
          lltv
        }
        supplyAssets
        supplyAssetsUsd
        supplyShares
        supplyCap
        supplyCapUsd
        supplyQueueIndex
        withdrawQueueIndex
        enabled
        removableAt
        pendingSupplyCap
        pendingSupplyCapValidAt
        pendingSupplyCapUsd
      }
    }
    warnings {
      type
      level
    }
    publicAllocatorConfig {
      fee
      accruedFee
      admin
    }
"""


# ══════════════════════════════════════════════════════════════════════
#  PHASE 0: Discover toxic market uniqueKeys
# ══════════════════════════════════════════════════════════════════════

def discover_toxic_markets() -> Dict[str, Dict]:
    """
    Load toxic market uniqueKeys from block1_markets_graphql.csv,
    or discover them via inline GraphQL query if CSV not available.
    Returns dict: { uniqueKey: { chain_id, chain, collateral_symbol, loan_symbol } }
    """
    markets_csv = PROJECT_ROOT / "data" / "block1_markets_graphql.csv"

    if markets_csv.exists():
        df = pd.read_csv(markets_csv)
        print(f"\n📂 Loaded {len(df)} toxic markets from {markets_csv.name}")

        # Handle column name variations
        chain_col = "blockchain" if "blockchain" in df.columns else "chain"
        chain_id_col = "chain_id"
        market_id_col = "market_id"
        collat_col = "collateral_symbol"
        loan_col = "loan_symbol"

        toxic_keys = {}
        for _, row in df.iterrows():
            key = row[market_id_col]
            toxic_keys[key] = {
                "chain_id": int(row[chain_id_col]),
                "chain": row[chain_col],
                "collateral_symbol": row[collat_col],
                "loan_symbol": row[loan_col],
            }
        return toxic_keys

    # Fallback: discover inline
    print("\n⚠️  Markets CSV not found — discovering toxic markets inline...")
    toxic_keys = {}
    for chain_name, chain_id in CHAIN_IDS.items():
        query = f"""
        {{
          markets(first: 500, where: {{ chainId_in: [{chain_id}] }}) {{
            items {{
              uniqueKey
              collateralAsset {{ symbol }}
              loanAsset {{ symbol }}
            }}
          }}
        }}
        """
        try:
            result = query_graphql(query)
            items = result.get("data", {}).get("markets", {}).get("items", [])
            for m in items:
                collat = (m.get("collateralAsset") or {}).get("symbol", "")
                if collat in TOXIC_COLLATERAL and collat not in FALSE_POSITIVES:
                    toxic_keys[m["uniqueKey"]] = {
                        "chain_id": chain_id,
                        "chain": chain_name,
                        "collateral_symbol": collat,
                        "loan_symbol": (m.get("loanAsset") or {}).get("symbol", ""),
                    }
            time.sleep(API_DELAY)
        except Exception as e:
            print(f"   ❌ Error querying {chain_name}: {e}")

    print(f"   Found {len(toxic_keys)} toxic markets inline")
    return toxic_keys


# ══════════════════════════════════════════════════════════════════════
#  PHASE 1: Vault discovery via marketUniqueKey_in filter
# ══════════════════════════════════════════════════════════════════════

def phase1_vaults_by_market_keys(toxic_keys: Dict[str, Dict]) -> Dict[Tuple[str, int], Dict]:
    """
    Query vaults(where: { marketUniqueKey_in: [...] }) per chain.
    Returns dict: { (address_lower, chain_id): vault_data }
    """
    print(f"\n{'─' * 60}")
    print(f"🔍 PHASE 1: Vaults with current positions in toxic markets")
    print(f"{'─' * 60}")

    # Group toxic keys by chain_id
    keys_by_chain: Dict[int, List[str]] = {}
    for key, info in toxic_keys.items():
        cid = info["chain_id"]
        if cid not in keys_by_chain:
            keys_by_chain[cid] = []
        keys_by_chain[cid].append(key)

    found_vaults: Dict[Tuple[str, int], Dict] = {}

    for chain_id, market_keys in keys_by_chain.items():
        chain_name = CHAIN_NAMES.get(chain_id, str(chain_id))
        # Format keys as GraphQL string array
        keys_str = ", ".join(f'"{k}"' for k in market_keys)

        print(f"\n   {chain_name} ({len(market_keys)} toxic markets)...")

        skip = 0
        page_size = 100
        page = 1

        while True:
            query = f"""
            {{
              vaults(
                first: {page_size}
                skip: {skip}
                where: {{
                  chainId_in: [{chain_id}]
                  marketUniqueKey_in: [{keys_str}]
                }}
              ) {{
                items {{
                  {VAULT_FIELDS}
                }}
                pageInfo {{
                  count
                  countTotal
                  limit
                  skip
                }}
              }}
            }}
            """
            try:
                result = query_graphql(query)
                time.sleep(API_DELAY)

                if "errors" in result:
                    print(f"   ❌ GraphQL Error: {result['errors'][0].get('message', '')}")
                    break

                items = result.get("data", {}).get("vaults", {}).get("items", [])
                page_info = result.get("data", {}).get("vaults", {}).get("pageInfo", {})
                count_total = page_info.get("countTotal", 0)

                for v in items:
                    addr = v.get("address", "").lower()
                    cid = (v.get("chain") or {}).get("id", chain_id)
                    found_vaults[(addr, cid)] = v

                if not items or skip + len(items) >= count_total:
                    total = len([k for k in found_vaults if k[1] == chain_id])
                    print(f"   ✅ {total} vaults with current positions (total matching: {count_total})")
                    break

                skip += page_size
                page += 1

            except Exception as e:
                print(f"   ❌ Exception: {e}")
                break

    print(f"\n   Phase 1 total: {len(found_vaults)} unique vaults")
    return found_vaults


# ══════════════════════════════════════════════════════════════════════
#  PHASE 2: Historical discovery via vaultReallocates
# ══════════════════════════════════════════════════════════════════════

def phase2_reallocate_discovery(toxic_keys: Dict[str, Dict]) -> Tuple[Set[Tuple[str, int]], Dict[Tuple[str, int], Set[str]]]:
    """
    Query vaultReallocates(where: { marketUniqueKey_in: [...] }) to find
    ALL vault addresses that EVER interacted with toxic markets.
    Returns:
      - set of (address_lower, chain_id) tuples
      - dict mapping (address_lower, chain_id) → set of market uniqueKeys
    """
    print(f"\n{'─' * 60}")
    print(f"🔍 PHASE 2: Historical vault discovery via reallocations")
    print(f"{'─' * 60}")

    # Group toxic keys by chain_id
    keys_by_chain: Dict[int, List[str]] = {}
    for key, info in toxic_keys.items():
        cid = info["chain_id"]
        if cid not in keys_by_chain:
            keys_by_chain[cid] = []
        keys_by_chain[cid].append(key)

    discovered_addrs: Set[Tuple[str, int]] = set()
    addr_to_markets: Dict[Tuple[str, int], Set[str]] = {}

    for chain_id, market_keys in keys_by_chain.items():
        chain_name = CHAIN_NAMES.get(chain_id, str(chain_id))
        keys_str = ", ".join(f'"{k}"' for k in market_keys)

        print(f"\n   {chain_name}...")

        skip = 0
        page_size = 500
        total_events = 0

        while True:
            query = f"""
            {{
              vaultReallocates(
                first: {page_size}
                skip: {skip}
                orderBy: Timestamp
                orderDirection: Desc
                where: {{
                  marketUniqueKey_in: [{keys_str}]
                  chainId_in: [{chain_id}]
                }}
              ) {{
                items {{
                  vault {{
                    address
                  }}
                  market {{
                    uniqueKey
                  }}
                  type
                  timestamp
                  assets
                }}
                pageInfo {{
                  count
                  countTotal
                }}
              }}
            }}
            """
            try:
                result = query_graphql(query)
                time.sleep(API_DELAY)

                if "errors" in result:
                    print(f"   ❌ GraphQL Error: {result['errors'][0].get('message', '')}")
                    break

                items = result.get("data", {}).get("vaultReallocates", {}).get("items", [])
                page_info = result.get("data", {}).get("vaultReallocates", {}).get("pageInfo", {})
                count_total = page_info.get("countTotal", 0)

                for r in items:
                    vault_addr = (r.get("vault") or {}).get("address", "").lower()
                    market_key = (r.get("market") or {}).get("uniqueKey", "")
                    if vault_addr:
                        addr_key = (vault_addr, chain_id)
                        discovered_addrs.add(addr_key)
                        if addr_key not in addr_to_markets:
                            addr_to_markets[addr_key] = set()
                        if market_key:
                            addr_to_markets[addr_key].add(market_key)

                total_events += len(items)

                if not items or skip + len(items) >= count_total:
                    chain_addrs = len([a for a in discovered_addrs if a[1] == chain_id])
                    print(f"   ✅ {chain_addrs} unique vault addresses from {count_total} reallocation events")
                    break

                skip += page_size

            except Exception as e:
                print(f"   ❌ Exception: {e}")
                break

    print(f"\n   Phase 2 total: {len(discovered_addrs)} unique vault addresses from reallocations")
    return discovered_addrs, addr_to_markets


# ══════════════════════════════════════════════════════════════════════
#  PHASE 3: Fetch missing vaults individually
# ══════════════════════════════════════════════════════════════════════

def phase3_fetch_missing_vaults(
    missing_addrs: Set[Tuple[str, int]],
) -> Dict[Tuple[str, int], Dict]:
    """
    For vault addresses discovered in Phase 2 but not Phase 1,
    query vaultByAddress individually to get full data.
    """
    print(f"\n{'─' * 60}")
    print(f"🔍 PHASE 3: Fetching {len(missing_addrs)} missing vaults individually")
    print(f"{'─' * 60}")

    new_vaults: Dict[Tuple[str, int], Dict] = {}
    sorted_addrs = sorted(missing_addrs)

    for idx, (addr, chain_id) in enumerate(sorted_addrs):
        chain_name = CHAIN_NAMES.get(chain_id, str(chain_id))

        query = f"""
        {{
          vaultByAddress(
            address: "{addr}"
            chainId: {chain_id}
          ) {{
            {VAULT_FIELDS}
          }}
        }}
        """
        try:
            result = query_graphql(query)
            time.sleep(API_DELAY)

            if "errors" in result:
                print(f"   ❌ {addr[:10]}... ({chain_name}): {result['errors'][0].get('message', '')}")
                continue

            vault_data = result.get("data", {}).get("vaultByAddress")
            if vault_data:
                vault_name = vault_data.get("name", "Unknown")
                new_vaults[(addr, chain_id)] = vault_data
                print(f"   [{idx+1}/{len(missing_addrs)}] ✅ {vault_name} ({chain_name}) "
                      f"TVL=${safe_float((vault_data.get('state') or {}).get('totalAssetsUsd')):,.0f}")
            else:
                print(f"   [{idx+1}/{len(missing_addrs)}] ⚠️  No data for {addr[:10]}... ({chain_name})")

        except Exception as e:
            print(f"   [{idx+1}/{len(missing_addrs)}] ❌ {addr[:10]}... ({chain_name}): {e}")

    print(f"\n   Phase 3: fetched {len(new_vaults)} additional vaults")
    return new_vaults


# ══════════════════════════════════════════════════════════════════════
#  Filter & parse vault-market pairs
# ══════════════════════════════════════════════════════════════════════

def filter_vaults_with_toxic_exposure(
    vaults: Dict[Tuple[str, int], Dict],
    toxic_keys: Dict[str, Dict],
    historical_addrs: Set[Tuple[str, int]],
    addr_to_markets: Dict[Tuple[str, int], Set[str]] = None,
) -> List[Dict]:
    """
    Build vault-market pairs from vault data.

    For vaults with CURRENT toxic allocations: extract normally.
    For vaults found ONLY via reallocations (no current allocation): create synthetic entry
    using the actual market key(s) from the reallocation events.
    """
    if addr_to_markets is None:
        addr_to_markets = {}

    toxic_key_set = set(toxic_keys.keys())
    vault_market_pairs = []
    found_by_allocation: Set[Tuple[str, int]] = set()

    for (addr, chain_id), vault in vaults.items():
        state = vault.get("state")
        if not state:
            continue

        allocations = state.get("allocation") or []

        for alloc in allocations:
            market = alloc.get("market")
            if not market:
                continue

            market_key = market.get("uniqueKey", "")
            collateral = market.get("collateralAsset") or {}
            symbol = collateral.get("symbol", "")

            # Match by market uniqueKey (primary) OR collateral symbol (fallback)
            is_toxic_by_key = market_key in toxic_key_set
            is_toxic_by_symbol = (symbol in TOXIC_COLLATERAL and symbol not in FALSE_POSITIVES)

            if is_toxic_by_key or is_toxic_by_symbol:
                vault_market_pairs.append({
                    "vault": vault,
                    "allocation": alloc,
                    "market": market,
                    "discovery_method": "current_allocation",
                })
                found_by_allocation.add((addr, chain_id))

    # ── Synthetic entries for historical-only vaults ──
    historical_only = historical_addrs - found_by_allocation
    print(f"\n   Current allocation matches: {len(found_by_allocation)} vaults")
    print(f"   Historical-only (no current toxic allocation): {len(historical_only)} vaults")

    for (addr, chain_id) in historical_only:
        vault = vaults.get((addr, chain_id))
        if not vault:
            continue

        # Use actual market keys from reallocation events (Phase 2 data)
        known_markets = addr_to_markets.get((addr, chain_id), set())

        if known_markets:
            # Create one entry per known market (vault may have interacted with multiple)
            for market_key in known_markets:
                if market_key not in toxic_keys:
                    continue
                toxic_info = toxic_keys[market_key]

                synthetic_alloc = {
                    "supplyAssets": "0",
                    "supplyAssetsUsd": 0,
                    "supplyShares": "0",
                    "supplyCap": "0",
                    "supplyCapUsd": 0,
                    "supplyQueueIndex": None,
                    "withdrawQueueIndex": None,
                    "enabled": False,
                    "removableAt": None,
                    "pendingSupplyCap": None,
                    "pendingSupplyCapValidAt": None,
                    "pendingSupplyCapUsd": None,
                    "market": {
                        "uniqueKey": market_key,
                        "collateralAsset": {
                            "symbol": toxic_info["collateral_symbol"],
                            "address": None,
                        },
                        "loanAsset": {
                            "symbol": toxic_info["loan_symbol"],
                            "address": None,
                        },
                        "lltv": None,
                    },
                }

                vault_market_pairs.append({
                    "vault": vault,
                    "allocation": synthetic_alloc,
                    "market": synthetic_alloc["market"],
                    "discovery_method": "historical_reallocation",
                })
        else:
            # Fallback: no known markets from Phase 2, assign all same-chain toxic markets
            matching_toxic = [
                (key, info) for key, info in toxic_keys.items()
                if info["chain_id"] == chain_id
            ]

            if not matching_toxic:
                continue

            # Use first match as fallback (less accurate)
            toxic_key, toxic_info = matching_toxic[0]
            print(f"   ⚠️  {vault.get('name', addr[:10])}: no market key from reallocations, "
                  f"using fallback ({toxic_info['collateral_symbol']}/{toxic_info['loan_symbol']})")

            synthetic_alloc = {
                "supplyAssets": "0",
                "supplyAssetsUsd": 0,
                "supplyShares": "0",
                "supplyCap": "0",
                "supplyCapUsd": 0,
                "supplyQueueIndex": None,
                "withdrawQueueIndex": None,
                "enabled": False,
                "removableAt": None,
                "pendingSupplyCap": None,
                "pendingSupplyCapValidAt": None,
                "pendingSupplyCapUsd": None,
                "market": {
                    "uniqueKey": toxic_key,
                    "collateralAsset": {
                        "symbol": toxic_info["collateral_symbol"],
                        "address": None,
                    },
                    "loanAsset": {
                        "symbol": toxic_info["loan_symbol"],
                        "address": None,
                    },
                    "lltv": None,
                },
            }

            vault_market_pairs.append({
                "vault": vault,
                "allocation": synthetic_alloc,
                "market": synthetic_alloc["market"],
                "discovery_method": "historical_reallocation",
            })

    return vault_market_pairs


def determine_exposure_status(allocation: Dict, discovery_method: str = "current_allocation") -> str:
    """
    Determine the exposure status of a vault-market allocation.

    Categories:
    - ACTIVE_DEPEG:            Still has supply to toxic market
    - FULLY_EXITED:            Allocation exists but supplyAssets = 0
    - WITHDREW_DURING_DEPEG:   removableAt timestamp >= Nov 4 (depeg)
    - WITHDREW_PRE_DEPEG:      removableAt timestamp >= Oct 28 but < Nov 4
    - STOPPED_SUPPLYING:       supplyCap = 0 or disabled
    - HISTORICALLY_EXPOSED:    Found via reallocations only — fully removed from queue
    """
    if discovery_method == "historical_reallocation":
        return "HISTORICALLY_EXPOSED"

    supply_assets = safe_int(allocation.get("supplyAssets"))
    supply_cap = safe_int(allocation.get("supplyCap"))
    removable_at = allocation.get("removableAt")

    if supply_cap == 0:
        if removable_at:
            try:
                removable_ts = int(removable_at)
                depeg_ts = int(datetime.fromisoformat(DEPEG_START + 'T00:00:00+00:00').timestamp())
                pre_depeg_ts = int(datetime.fromisoformat(PRE_DEPEG_START + 'T00:00:00+00:00').timestamp())

                if removable_ts >= depeg_ts:
                    return "WITHDREW_DURING_DEPEG"
                elif removable_ts >= pre_depeg_ts:
                    return "WITHDREW_PRE_DEPEG"
            except (ValueError, TypeError):
                pass
        return "STOPPED_SUPPLYING"

    if supply_assets == 0:
        return "FULLY_EXITED"

    return "ACTIVE_DEPEG"


def parse_vault_market_data(vault_market: Dict) -> Dict:
    """Parse vault-market pair data into flat dictionary — all fields for Blocks 2-6"""
    vault = vault_market["vault"]
    allocation = vault_market["allocation"]
    market = vault_market["market"]
    discovery_method = vault_market.get("discovery_method", "current_allocation")

    state = vault.get("state") or {}
    chain = vault.get("chain") or {}
    deposit_asset = vault.get("asset") or {}
    factory = vault.get("factory") or {}

    # --- Curator resolution ---
    curator_address = state.get("curator")
    curators_list = state.get("curators") or []

    curator_name = None
    curator_verified = None
    if curators_list:
        curator_name = curators_list[0].get("name")
        curator_verified = curators_list[0].get("verified")
    if not curator_name:
        curator_name = curator_address

    all_curator_names = "|".join([c.get("name", "") for c in curators_list]) if curators_list else None

    # --- Curator metadata (Safe multisig info) ---
    curator_metadata = state.get("curatorMetadata") or {}
    meta_items = curator_metadata.get("items") or []
    curator_is_multisig = False
    curator_multisig_threshold = None
    curator_multisig_owners = None
    for meta_item in meta_items:
        if meta_item.get("type") == "safe":
            meta = meta_item.get("metadata") or {}
            curator_is_multisig = True
            curator_multisig_threshold = meta.get("threshold")
            owners = meta.get("owners") or []
            curator_multisig_owners = len(owners)

    # --- Market info ---
    collateral = market.get("collateralAsset") or {}
    loan = market.get("loanAsset") or {}

    # --- Allocation data ---
    supply_assets = safe_int(allocation.get("supplyAssets"))
    supply_assets_usd = safe_float(allocation.get("supplyAssetsUsd"))
    supply_cap = safe_int(allocation.get("supplyCap"))
    supply_cap_usd = safe_float(allocation.get("supplyCapUsd"))

    # --- Vault totals ---
    vault_total_assets_usd = safe_float(state.get("totalAssetsUsd"))
    vault_total_assets_raw = safe_int(state.get("totalAssets"))

    # --- Exposure as % of vault ---
    exposure_pct = (supply_assets_usd / vault_total_assets_usd) if vault_total_assets_usd > 0 else 0

    # --- Pending cap changes ---
    pending_supply_cap = allocation.get("pendingSupplyCap")
    pending_supply_cap_valid_at = allocation.get("pendingSupplyCapValidAt")
    pending_supply_cap_usd = safe_float(allocation.get("pendingSupplyCapUsd"))

    # --- Warnings ---
    warnings = vault.get("warnings") or []
    warning_types = [w.get("type") for w in warnings] if warnings else []
    warning_levels = [w.get("level") for w in warnings] if warnings else []

    # --- Public allocator ---
    pa_config = vault.get("publicAllocatorConfig")
    has_public_allocator = pa_config is not None
    pa_admin = pa_config.get("admin") if pa_config else None

    # --- Exposure status ---
    exposure_status = determine_exposure_status(allocation, discovery_method)

    return {
        # --- Vault identity ---
        "vault_name": vault.get("name"),
        "vault_address": vault.get("address"),
        "vault_symbol": vault.get("symbol"),
        "vault_listed": vault.get("listed"),
        "vault_featured": vault.get("featured"),
        "vault_creation_timestamp": vault.get("creationTimestamp"),
        "vault_creator_address": vault.get("creatorAddress"),
        "factory_address": factory.get("address"),
        "deposit_asset_symbol": deposit_asset.get("symbol"),
        "deposit_asset_address": deposit_asset.get("address"),
        "deposit_asset_decimals": safe_int(deposit_asset.get("decimals")),

        # --- Chain ---
        "blockchain": chain.get("network"),
        "chain_id": chain.get("id"),

        # --- Curator (Block 3.4) ---
        "curator_address": curator_address,
        "curator_name": curator_name,
        "curator_verified": curator_verified,
        "all_curator_names": all_curator_names,
        "curator_is_multisig": curator_is_multisig,
        "curator_multisig_threshold": curator_multisig_threshold,
        "curator_multisig_owners": curator_multisig_owners,

        # --- Governance (Block 3.2) ---
        "owner": state.get("owner"),
        "guardian": state.get("guardian"),
        "fee_recipient": state.get("feeRecipient"),
        "timelock": safe_int(state.get("timelock")),

        # --- Market info ---
        "market_id": market.get("uniqueKey"),
        "market_id_short": f"{market.get('uniqueKey', '')[:6]}...{market.get('uniqueKey', '')[-4:]}" if market.get('uniqueKey') else None,
        "collateral_symbol": collateral.get("symbol"),
        "collateral_address": collateral.get("address"),
        "loan_symbol": loan.get("symbol"),
        "loan_address": loan.get("address"),
        "lltv": float(market.get("lltv", 0)) / 1e18 if market.get("lltv") else 0,

        # --- Allocation (current snapshot) ---
        "supply_assets": supply_assets,
        "supply_assets_usd": supply_assets_usd,
        "supply_shares": safe_int(allocation.get("supplyShares")),
        "supply_cap": supply_cap,
        "supply_cap_usd": supply_cap_usd,
        "supply_queue_index": allocation.get("supplyQueueIndex"),
        "withdraw_queue_index": allocation.get("withdrawQueueIndex"),
        "enabled": allocation.get("enabled"),
        "removable_at": allocation.get("removableAt"),

        # --- Pending cap changes (Block 3.2) ---
        "pending_supply_cap": pending_supply_cap,
        "pending_supply_cap_valid_at": pending_supply_cap_valid_at,
        "pending_supply_cap_usd": pending_supply_cap_usd,

        # --- Vault-level state ---
        "vault_total_assets": vault_total_assets_raw,
        "vault_total_assets_usd": vault_total_assets_usd,
        "vault_total_supply_shares": safe_int(state.get("totalSupply")),
        "vault_share_price": safe_float(state.get("sharePriceNumber")),
        "vault_share_price_usd": safe_float(state.get("sharePriceUsd")),
        "vault_apy": safe_float(state.get("apy")),
        "vault_net_apy": safe_float(state.get("netApy")),
        "vault_fee": safe_float(state.get("fee")),

        # --- Computed: exposure % ---
        "exposure_pct": exposure_pct,
        "exposure_status": exposure_status,
        "discovery_method": discovery_method,

        # --- Public allocator (Block 6.2) ---
        "has_public_allocator": has_public_allocator,
        "public_allocator_admin": pa_admin,

        # --- Warnings ---
        "warning_types": "|".join(warning_types) if warning_types else None,
        "warning_levels": "|".join(warning_levels) if warning_levels else None,

        # --- Metadata ---
        "state_timestamp": state.get("timestamp"),
        "state_block_number": state.get("blockNumber"),
    }


# ══════════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════════

def main():
    print("=" * 80)
    print("Block 1.2 (GraphQL): Vault exposure — 3-PHASE DISCOVERY")
    print("=" * 80)
    print(f"Analysis period: {START_DATE} to {END_DATE}")
    print(f"Depeg start:     {DEPEG_START}")
    print(f"Pre-depeg week:  {PRE_DEPEG_START}")
    print("=" * 80)

    # ── Phase 0: Get toxic market uniqueKeys ──
    toxic_keys = discover_toxic_markets()
    if not toxic_keys:
        print("❌ No toxic markets found — cannot discover vaults")
        return

    # Group by chain for display
    by_chain = {}
    for key, info in toxic_keys.items():
        c = info["chain"]
        if c not in by_chain:
            by_chain[c] = []
        by_chain[c].append(f"{info['collateral_symbol']}/{info['loan_symbol']}")

    for chain, markets in by_chain.items():
        print(f"   {chain}: {', '.join(markets)}")
    print(f"   Total: {len(toxic_keys)} toxic markets across {len(by_chain)} chains")

    # ── Phase 1: Vaults with current positions ──
    phase1_vaults = phase1_vaults_by_market_keys(toxic_keys)

    # ── Phase 2: Historical discovery via reallocations ──
    phase2_addrs, addr_to_markets = phase2_reallocate_discovery(toxic_keys)

    # ── Find addresses in Phase 2 but not Phase 1 ──
    phase1_addrs = set(phase1_vaults.keys())
    missing_addrs = phase2_addrs - phase1_addrs

    print(f"\n{'─' * 60}")
    print(f"📊 DISCOVERY SUMMARY")
    print(f"{'─' * 60}")
    print(f"   Phase 1 (current positions):     {len(phase1_addrs)} vaults")
    print(f"   Phase 2 (reallocation history):   {len(phase2_addrs)} vault addresses")
    print(f"   New from Phase 2 (not in P1):     {len(missing_addrs)} vaults to fetch")

    # ── Phase 3: Fetch missing vaults individually ──
    phase3_vaults: Dict[Tuple[str, int], Dict] = {}
    if missing_addrs:
        phase3_vaults = phase3_fetch_missing_vaults(missing_addrs)

    # ── Merge all vaults ──
    all_vaults = {**phase1_vaults, **phase3_vaults}
    print(f"\n   TOTAL unique vaults: {len(all_vaults)}")

    # ── Filter & build vault-market pairs ──
    print(f"\n{'─' * 60}")
    print(f"🔍 Building vault-market pairs...")
    print(f"{'─' * 60}")

    vault_market_pairs = filter_vaults_with_toxic_exposure(
        all_vaults, toxic_keys, phase2_addrs, addr_to_markets
    )

    if not vault_market_pairs:
        print("\n⚠️  No vaults found with toxic collateral exposure")
        return

    print(f"\n✅ Found {len(vault_market_pairs)} vault-market pairs with toxic exposure")

    # Parse vault-market data
    print(f"\n📊 Parsing vault allocation data...")
    parsed_data = [parse_vault_market_data(vm) for vm in vault_market_pairs]
    df = pd.DataFrame(parsed_data)

    # Deduplicate: same (vault_address, market_id) should appear only once
    # Keep current_allocation over historical_reallocation if both exist
    pre_dedup = len(df)
    df = df.sort_values("discovery_method", ascending=True)  # current_allocation < historical_reallocation
    df = df.drop_duplicates(subset=["vault_address", "market_id"], keep="first")
    if len(df) < pre_dedup:
        print(f"   ℹ️  Deduplicated: {pre_dedup} → {len(df)} rows (removed {pre_dedup - len(df)} exact duplicates)")

    # Sort by supply assets USD descending, then by vault_total_assets_usd
    df = df.sort_values(['supply_assets_usd', 'vault_total_assets_usd'], ascending=[False, False])

    # Save output
    output_dir = PROJECT_ROOT / "data"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "block1_vaults_graphql.csv"

    df.to_csv(output_path, index=False)

    print(f"\n✅ Found {len(df)} vault positions with toxic collateral")
    print(f"✅ Saved to {output_path}")

    # ══════════════════════════════════════════════════════════════════
    #  Summary Statistics
    # ══════════════════════════════════════════════════════════════════
    if len(df) > 0:
        print(f"\n{'─' * 60}")
        print(f"📊 SUMMARY")
        print(f"{'─' * 60}")
        print(f"   Vault-market pairs:    {len(df)}")
        print(f"   Unique vaults:         {df['vault_address'].nunique()}")
        print(f"   Unique curators:       {df['curator_name'].nunique()}")
        print(f"   Chains:                {df['blockchain'].unique().tolist()}")
        print(f"   Collateral types:      {df['collateral_symbol'].unique().tolist()}")
        print(f"   Total exposure (USD):  ${df['supply_assets_usd'].sum():,.2f}")
        print(f"   Listed vaults:         {df['vault_listed'].sum()} / {df['vault_address'].nunique()}")

        # Discovery method breakdown
        print(f"\n{'─' * 60}")
        print(f"🔍 DISCOVERY METHOD BREAKDOWN")
        print(f"{'─' * 60}")
        method_counts = df['discovery_method'].value_counts()
        for method, count in method_counts.items():
            method_total = df[df['discovery_method'] == method]['vault_total_assets_usd'].sum()
            print(f"   {method:30s}: {count:3d} positions  TVL: ${method_total:>15,.2f}")

        # Exposure by status
        print(f"\n{'─' * 60}")
        print(f"🏷️  EXPOSURE BY STATUS")
        print(f"{'─' * 60}")
        status_counts = df['exposure_status'].value_counts()
        for status, count in status_counts.items():
            status_total = df[df['exposure_status'] == status]['supply_assets_usd'].sum()
            print(f"   {status:25s}: {count:3d} positions  ${status_total:>15,.2f}")

        # Curators breakdown
        print(f"\n{'─' * 60}")
        print(f"👤 CURATORS")
        print(f"{'─' * 60}")
        curator_summary = df.groupby('curator_name').agg(
            vaults=('vault_address', 'nunique'),
            positions=('vault_address', 'count'),
            total_exposure_usd=('supply_assets_usd', 'sum'),
            total_tvl_usd=('vault_total_assets_usd', 'first'),
            chains=('blockchain', lambda x: list(x.unique())),
        ).sort_values('total_tvl_usd', ascending=False)
        for curator, row in curator_summary.iterrows():
            verified_tag = ""
            curator_rows = df[df['curator_name'] == curator]
            if curator_rows['curator_verified'].any():
                verified_tag = " ✓"
            multisig_tag = ""
            if curator_rows['curator_is_multisig'].any():
                t = curator_rows['curator_multisig_threshold'].iloc[0]
                o = curator_rows['curator_multisig_owners'].iloc[0]
                multisig_tag = f" [Safe {t}/{o}]"
            print(f"   {curator}{verified_tag}{multisig_tag}")
            print(f"     Vaults: {row['vaults']}  Positions: {row['positions']}  "
                  f"Exposure: ${row['total_exposure_usd']:,.2f}  "
                  f"TVL: ${row['total_tvl_usd']:,.2f}  Chains: {row['chains']}")

        # Timelock analysis
        print(f"\n{'─' * 60}")
        print(f"⏱️  TIMELOCKS (Block 3.2)")
        print(f"{'─' * 60}")

        # Factory / Vault Version analysis
        if 'factory_address' in df.columns:
            print(f"\n{'─' * 60}")
            print(f"🏭 VAULT FACTORIES (Version Identification)")
            print(f"{'─' * 60}")
            factory_summary = df[['vault_name', 'factory_address', 'vault_creation_timestamp', 'blockchain']].drop_duplicates(subset='vault_name')
            factory_counts = factory_summary['factory_address'].value_counts()
            for factory_addr, count in factory_counts.items():
                if pd.notna(factory_addr):
                    names = factory_summary[factory_summary['factory_address'] == factory_addr]['vault_name'].tolist()
                    print(f"   Factory {factory_addr}:")
                    print(f"     Vaults ({count}): {', '.join(names)}")
                else:
                    print(f"   Factory UNKNOWN: {count} vaults")

        timelocks = df[['vault_name', 'curator_name', 'timelock']].drop_duplicates(subset='vault_name')
        for _, row in timelocks.iterrows():
            tl_seconds = row['timelock']
            if tl_seconds > 0:
                tl_hours = tl_seconds / 3600
                tl_str = f"{tl_hours:.1f}h" if tl_hours < 24 else f"{tl_hours/24:.1f}d"
            else:
                tl_str = "0 (instant)"
            print(f"   {row['vault_name']}: {tl_str} ({row['curator_name']})")

        # Public allocator
        pa_vaults = df[df['has_public_allocator'] == True]
        if len(pa_vaults) > 0:
            print(f"\n{'─' * 60}")
            print(f"🔄 PUBLIC ALLOCATOR (Block 6.2)")
            print(f"{'─' * 60}")
            pa_unique = pa_vaults[['vault_name', 'public_allocator_admin']].drop_duplicates()
            for _, row in pa_unique.iterrows():
                print(f"   {row['vault_name']}: admin={row['public_allocator_admin']}")

        # Pending cap changes
        pending = df[df['pending_supply_cap'].notna()]
        if len(pending) > 0:
            print(f"\n{'─' * 60}")
            print(f"⏳ PENDING CAP CHANGES")
            print(f"{'─' * 60}")
            for _, row in pending.iterrows():
                print(f"   {row['vault_name']} → {row['collateral_symbol']}/{row['loan_symbol']}: "
                      f"pending cap={row['pending_supply_cap']}  valid at={row['pending_supply_cap_valid_at']}")

        # Warnings
        vaults_with_warnings = df[df['warning_types'].notna()]
        if len(vaults_with_warnings) > 0:
            print(f"\n⚠️  Vaults with warnings: {len(vaults_with_warnings)}")
            for _, row in vaults_with_warnings.head(10).iterrows():
                print(f"   {row['vault_name']} ({row['blockchain']}): {row['warning_types']}")

        # Top exposures table
        print(f"\n{'─' * 60}")
        print(f"🔝 TOP 20 EXPOSURES BY VAULT TVL")
        print(f"{'─' * 60}")
        cols = ['vault_name', 'curator_name', 'collateral_symbol', 'loan_symbol',
                'supply_assets_usd', 'vault_total_assets_usd', 'exposure_pct',
                'exposure_status', 'discovery_method']
        display = df.sort_values('vault_total_assets_usd', ascending=False).head(20)[cols].copy()
        display['exposure_pct'] = display['exposure_pct'].apply(lambda x: f"{x*100:.1f}%")
        display['supply_assets_usd'] = display['supply_assets_usd'].apply(lambda x: f"${x:,.0f}")
        display['vault_total_assets_usd'] = display['vault_total_assets_usd'].apply(lambda x: f"${x:,.0f}")
        print(display.to_string(index=False))

        # Active exposure during depeg
        print(f"\n{'─' * 60}")
        print(f"🔴 ACTIVE EXPOSURE DURING DEPEG")
        print(f"{'─' * 60}")
        active = df[df['exposure_status'] == 'ACTIVE_DEPEG']
        if len(active) > 0:
            total_active = active['supply_assets_usd'].sum()
            print(f"   {len(active)} positions with ${total_active:,.2f} active exposure")
            cols_a = ['vault_name', 'curator_name', 'collateral_symbol', 'supply_assets_usd',
                       'exposure_pct', 'vault_share_price']
            display_a = active[cols_a].copy()
            display_a['exposure_pct'] = display_a['exposure_pct'].apply(lambda x: f"{x*100:.1f}%")
            print(display_a.to_string(index=False))
        else:
            print("   None found")

        # Historically exposed (NEW section)
        print(f"\n{'─' * 60}")
        print(f"🕐 HISTORICALLY EXPOSED (fully exited — found via reallocations)")
        print(f"{'─' * 60}")
        historical = df[df['exposure_status'] == 'HISTORICALLY_EXPOSED']
        if len(historical) > 0:
            print(f"   {len(historical)} vaults that fully removed toxic markets from queue")
            cols_h = ['vault_name', 'curator_name', 'vault_total_assets_usd',
                      'vault_share_price', 'blockchain']
            display_h = historical[cols_h].copy()
            display_h['vault_total_assets_usd'] = display_h['vault_total_assets_usd'].apply(lambda x: f"${x:,.0f}")
            print(display_h.to_string(index=False))
        else:
            print("   None found")

        # Exited / stopped
        print(f"\n{'─' * 60}")
        print(f"✅ FULLY EXITED / STOPPED SUPPLYING")
        print(f"{'─' * 60}")
        exited = df[df['exposure_status'].isin(['FULLY_EXITED', 'STOPPED_SUPPLYING',
                                                 'WITHDREW_PRE_DEPEG', 'WITHDREW_DURING_DEPEG'])]
        if len(exited) > 0:
            print(f"   {len(exited)} positions")
            cols_e = ['vault_name', 'curator_name', 'exposure_status', 'removable_at', 'timelock']
            print(exited[cols_e].head(15).to_string(index=False))
        else:
            print("   None found")

    print(f"\n{'=' * 80}")


if __name__ == "__main__":
    main()
