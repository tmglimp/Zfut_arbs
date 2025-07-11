import logging
import sys
from pathlib import Path
from enum import Enum
from typing import List, Tuple
import pandas as pd
import requests
import urllib3
import cfg

# ─── user credentials & endpoints ─────────────────────────────────────────────────────
IB_USERNAME = cfg.username
IB_PASSWORD = cfg.password
IB_API_KEY  = cfg.api_key
IB_BASE     = cfg.base
EXCHANGE    = cfg.exchange

# ─── logging (file + console) ───────────────────────────────────────────
logging.basicConfig(
    level=cfg.LOG_LEVEL,
    format=cfg.LOG_FORMAT,
    handlers=[
        logging.FileHandler(cfg.LOG_FILE),
        logging.StreamHandler(sys.stdout)
    ]
)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ─── helpers ────────────────────────────────────────────────────
class MarketDataField(Enum):
    con_id = 6008

FIELDS = str(MarketDataField.con_id.value)
INDEX_DIR = Path(getattr(cfg, "DATA_DIR", "."))
FUT_INDEX_PATH = INDEX_DIR / "FUTURES.index"

# ─── IronBeam auth, discovery, populate FUTURES ─────────────────────────────────────────

def authenticate() -> str:
    """Return a session token or raise."""
    url  = f"{IB_BASE}/auth"
    resp = requests.post(
        url,
        json={
            "username": IB_USERNAME,
            "password": IB_PASSWORD,
            "apikey":   IB_API_KEY
        },
        headers={"Content-Type": "application/json"},
    )
    resp.raise_for_status()
    token = resp.json().get("token")
    if not token:
        raise RuntimeError("Failed to authenticate to Ironbeam")
    return token

def fetch_market_groups(_: str | None = None) -> None:
    """Use the list already defined in cfg.market_groups. Raise if it's missing."""
    if not getattr(cfg, "market_groups", []):
        raise ValueError(
            "cfg.market_groups is empty. Define it before calling populate_dictionary()."
        )
    logging.info(f"Using static market groups from cfg: {cfg.market_groups}")

def fetch_current_contracts(token: str) -> None:
    """Fetch *all* contracts for each market group and store in cfg.current_contracts."""
    headers = {"Authorization": f"Bearer {token}"}
    logging.info(f"Fetching futures contracts for market group => {cfg.market_groups}")

    raw_syms: list[str] = []
    for grp in cfg.market_groups:
        url = f"{IB_BASE}/info/symbol/search/futures/{EXCHANGE}/{grp}"
        r   = requests.get(url, headers=headers)
        r.raise_for_status()
        found = [item["symbol"] for item in r.json().get("symbols", [])]
        logging.info(f"  -> {grp}: found {len(found)} contracts")
        raw_syms.extend(found)

    cfg.current_contracts = sorted(set(raw_syms))
    logging.info(f"Retrieved futures contracts: {cfg.current_contracts}")

def fetch_and_populate_definitions(token: str) -> None:
    """Fetch contract definitions and store full DataFrame in cfg.FUTURES."""
    if not cfg.current_contracts:
        logging.warning("No contracts to fetch definitions for.")
        cfg.FUTURES = pd.DataFrame()
        return

    logging.info(
        f"Fetching contract definitions for futures symbols => {cfg.current_contracts}"
    )
    url     = f"{IB_BASE}/info/security/definitions"
    params  = [("symbols", sym) for sym in cfg.current_contracts]
    headers = {"Authorization": f"Bearer {token}"}
    resp    = requests.get(url, headers=headers, params=params)
    resp.raise_for_status()

    records: list[dict] = []
    for entry in resp.json().get("securityDefinitions", []):
        sym = entry.get("exchSym", "").replace(f"{EXCHANGE}:", "").strip()

        # convert epoch‑ms to pandas.Timestamp
        for fld in ("activationTime", "expirationTime", "creationDate"):
            if isinstance(entry.get(fld), (int, float)):
                entry[fld] = pd.to_datetime(entry[fld], unit="ms")

        for side in ("Bid", "Ask"):
            row = entry.copy()
            row["symbol"] = sym
            row["side"]   = side
            records.append(row)

    if not records:
        cfg.FUTURES = pd.DataFrame()
        return

    df = pd.DataFrame(records).reset_index(drop=True)
    today = pd.Timestamp.today()
    df["ytm"]       = (df["expirationTime"] - today) / pd.Timedelta(days=365.25)
    df["days2expy"] = df["expirationTime"] - today
    cfg.FUTURES = df
    logging.info(f"Populated FUTURES with {len(df)} rows")

    # ── Merge conids from FUTURES.index with tighter criteria ──────────────────
    if FUT_INDEX_PATH.exists():
        fut_index_df = pd.read_csv(FUT_INDEX_PATH)

        # harmonise column names
        if "exchSym" in fut_index_df.columns and "symbol" not in fut_index_df.columns:
            fut_index_df = fut_index_df.rename(columns={"exchSym": "symbol"})
        if "ticker" not in fut_index_df.columns and "symbol" in fut_index_df.columns:
            fut_index_df["ticker"] = fut_index_df["symbol"]

        required = {"ticker", "year_to_maturity", "conid"}
        if required.issubset(fut_index_df.columns):
            merge_df = cfg.FUTURES.merge(fut_index_df[["ticker", "year_to_maturity", "conid"]],left_on="marketSymbol",right_on="ticker", how="left",suffixes=("", "_index"))

            tol = 0.05
            merge_df["ytm_diff"] = (merge_df["ytm"] - merge_df["year_to_maturity"]).abs()

            # keep conid only where YTM is within ±0.05
            merge_df.loc[merge_df["ytm_diff"] > tol, "conid"] = pd.NA

            # propagate the existing conid to every row that shares the same marketSymbol
            merge_df["conid"] = (merge_df.groupby("marketSymbol")["conid"].transform(lambda s: s.dropna().iloc[0] if not s.dropna().empty else pd.NA).astype("Int64"))

            cfg.FUTURES = merge_df.drop(columns=["ticker", "year_to_maturity", "ytm_diff"]).copy()
            cfg.FUTURES = cfg.FUTURES.drop_duplicates(subset=["symbol", "side"], keep="first")

            logging.info("Merged conids from FUTURES.index; filled %d of %d rows",merge_df["conid"].notna().sum(),len(cfg.FUTURES))

        else:
            logging.warning("FUTURES.index missing required columns: %s", required - set(fut_index_df.columns))
    else:
        logging.warning("FUTURES.index file not found; conid merge skipped")

def populate_dictionary() -> None:
    logging.info("Authenticating with Iron Beam...")
    cfg.token = authenticate()
    logging.info("Successfully authenticated.")

    fetch_market_groups(cfg.token)
    fetch_current_contracts(cfg.token)
    logging.info("Fetched all futures contracts.")
    fetch_and_populate_definitions(cfg.token)

    logging.info("cfg.FUTURES now populated.")

if __name__ == "__main__":
    populate_dictionary()
