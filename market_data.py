import logging
import sys
import time
import cfg
from pathlib import Path
from datetime import datetime
from enum import Enum
from typing import List, Tuple
import numpy as np
import pandas as pd
import requests
import config
from leaky_bucket import leaky_bucket

# ────────────────────────────────────────────────────────────
# Friendly names ↔ IBKR numeric market‑data fields
# ────────────────────────────────────────────────────────────
class MarketDataField(Enum):
    last_price            = 31
    last_size             = 7059
    last_yield            = 7698
    last_exch             = 7058
    avg_price             = 74
    bid_price             = 84
    bid_size              = 88
    bid_yield             = 7699
    bid_exch              = 7068
    ask_price             = 86
    ask_size              = 85
    ask_yield             = 7720
    ask_exch              = 7057
    volume                = 87
    avg_volume            = 7282
    exchange              = 6004
    con_id                = 6008
    marker                = 6119
    mkt_data_avail        = 6509
    company               = 7051
    contract_description  = 7219
    listing_exchange      = 7221
    shortable_shares      = 7636

FIELDS = ",".join(str(f.value) for f in MarketDataField)

# ────────────────────────────────────────────────────────────
# Logging / basic paths
# ────────────────────────────────────────────────────────────
logging.basicConfig(
    level=config.LOG_LEVEL,
    format=config.LOG_FORMAT,
    handlers=[
        logging.FileHandler(config.LOG_FILE),
        logging.StreamHandler(sys.stdout),
    ],
)

INDEX_DIR = Path(getattr(config, "DATA_DIR", "."))
UST_PATH  = INDEX_DIR / "UST.index.csv"
FUT_PATH  = INDEX_DIR / "FUTURES.index"
ZER_PATH  = INDEX_DIR / "zeroes.index.csv"  # Principal‑STRIP index created by scanner.py

# ────────────────────────────────────────────────────────────
# Helpers – read static index tables & gather conids
# ────────────────────────────────────────────────────────────

def _load_index(path: Path, id_col: str) -> Tuple[List[int], pd.DataFrame]:
    """Return a list of numeric ids and the DataFrame (empty if path missing)."""
    if not path.exists():
        return [], pd.DataFrame()
    df = pd.read_csv(path, dtype=str)
    if id_col not in df.columns:
        return [], df
    ids = (
        pd.to_numeric(df[id_col], errors="coerce")
        .dropna()
        .astype(int)
        .tolist()
    )
    return ids, df

# ── Load the various static sources ─────────────────────────
UST_CONIDS,  ust_static_df    = _load_index(UST_PATH,  "conid")

CORP_CONIDS, corpus_static_df = _load_index(UST_PATH,  "corpusCusip_conid")
ZER_CONIDS,  zer_static_df    = _load_index(ZER_PATH, "conid")  # STRIPs


# ────────────────────────────────────────────────────────────
# Utility: normalise a DataFrame to guarantee a single Int64 `conid` column
# ────────────────────────────────────────────────────────────

def _ensure_conid_column(df: pd.DataFrame) -> pd.DataFrame:
    alt_cols = [c for c in df.columns if c.lower() in {"conidex", "conid_ex"}]
    if "conid" not in df.columns and alt_cols:
        df = df.rename(columns={alt_cols[0]: "conid"})
    elif "conid" in df.columns and alt_cols:
        df["conid"] = df["conid"].fillna(df[alt_cols[0]])

    if "conid" in df.columns:
        df["conid"] = pd.to_numeric(df["conid"], errors="coerce").astype("Int64")

    return df.drop(columns=alt_cols, errors="ignore")

# Apply the normaliser to every static table we just loaded
ust_static_df   = _ensure_conid_column(ust_static_df)
print(f'ust static df as', ust_static_df)
zer_static_df   = _ensure_conid_column(zer_static_df)
corpus_static_df = corpus_static_df.copy()  # Might be empty but keep type

# ── Stitch in the Principal‑STRIP universe ─────────────────
if not zer_static_df.empty:
    zer_static_df = zer_static_df.rename(
        columns={"conid": "corpusCusip_conid", "cusip": "corpusCusip"})
    corpus_static_df = (
        pd.concat([corpus_static_df, zer_static_df], ignore_index=True)
        .drop_duplicates(subset=["corpusCusip_conid"], keep="first"))
    CORP_CONIDS.extend([cid for cid in ZER_CONIDS if cid not in CORP_CONIDS])

UST_CONIDS  = sorted(set(UST_CONIDS))
CORP_CONIDS = sorted(set(CORP_CONIDS))

config.ust_conids    = UST_CONIDS
config.corpus_conids = CORP_CONIDS

# ────────────────────────────────────────────────────────────
# IBKR REST helpers
# ────────────────────────────────────────────────────────────
QUOTES_URL = f"{config.IBKR_BASE_URL}/v1/api/iserver/marketdata/snapshot"
MAX_BATCH  = 500

def _batched(seq: List[int], n: int):
    for i in range(0, len(seq), n):
        yield seq[i : i + n]

def fetch_snapshot(conids: List[int]) -> List[dict]:
    if not conids:
        return []
    rows: List[dict] = []
    for chunk in _batched(conids, MAX_BATCH):
        leaky_bucket.wait_for_token()
        try:
            resp = requests.get(
                QUOTES_URL,
                params={"conids": ",".join(map(str, chunk)), "fields": FIELDS},
                timeout=20,
                verify=False,
            )
            resp.raise_for_status()
            rows.extend(resp.json())
        except Exception as exc:
            logging.warning("snapshot chunk failed %s…: %s", chunk[:3], exc)
        time.sleep(0.05)
    return rows

# ────────────────────────────────────────────────────────────
# Snapshot post‑processing helpers
# ────────────────────────────────────────────────────────────
_num_field_map = {str(f.value): f.name for f in MarketDataField}

def _rename_fields(df: pd.DataFrame) -> pd.DataFrame:
    return df.rename(columns={c: _num_field_map.get(c, c) for c in df.columns})

def _mark_remove_closed(df: pd.DataFrame) -> pd.DataFrame:
    """Filter out rows where the contract is closed *or* has no bid/ask prices."""
    price_cols = [c for c in ("ask_price", "bid_price", "last_price") if c in df.columns]
    if not price_cols:
        return df.copy()

    # 1) Closed quotes begin with "C" or "c"
    closed_mask = df[price_cols].apply(lambda s: s.astype(str).str.startswith(("C", "c"))).any(axis=1)

    # 2) Missing/blank bid or ask price
    missing_cols = [c for c in ("ask_price", "bid_price") if c in df.columns]
    missing_mask = df[missing_cols].replace("", np.nan).isna().any(axis=1) if missing_cols else False

    return df.loc[~(closed_mask | missing_mask)].copy()

def _process_rows(rows: List[dict]) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df = _ensure_conid_column(df)
    df = _rename_fields(df)
    return df

# ────────────────────────────────────────────────────────────
# Main public entry
# ────────────────────────────────────────────────────────────

def refresh_market_data() -> Tuple[pd.DataFrame, pd.DataFrame]:
    logging.info("[MarketData] grabbing snapshot for UST_CONIDS (%d) …", len(UST_CONIDS))
    ust_rows = fetch_snapshot(UST_CONIDS)
    logging.debug("UST rows: %s", ust_rows)

    logging.info("[MarketData] grabbing snapshot for CORP_CONIDS (%d) …", len(CORP_CONIDS))
    corp_rows = fetch_snapshot(CORP_CONIDS)
    logging.debug("CORP rows: %s", corp_rows)

    if len(ust_rows) == 0 and len(corp_rows) == 0:
        logging.warning("[MarketData] all snapshot calls returned empty — keeping old data")
        return config.USTs, config.ZEROES

    ust_snap = _mark_remove_closed(_process_rows(ust_rows))
    ust_snap.to_csv('ust_snap.csv')
    corpus_snap = _mark_remove_closed(_process_rows(corp_rows))

    # Ensure numeric conids in static DataFrames
    for d in (ust_static_df,):
        if "conid" in d.columns:
            d["conid"] = pd.to_numeric(d["conid"], errors="coerce").astype("Int64")
    if "corpusCusip_conid" in corpus_static_df.columns:
        corpus_static_df["corpusCusip_conid"] = pd.to_numeric(
            corpus_static_df["corpusCusip_conid"], errors="coerce"
        ).astype("Int64")

    # Join static tables with fresh snapshots
    ust_final = (
        ust_static_df.join(ust_snap.set_index("conid"), on="conid", how="left", rsuffix="_md")
        if "conid" in ust_static_df.columns else ust_static_df.copy()
    )

    corpus_final = corpus_static_df.join(
        corpus_snap.set_index("conid").rename(columns={"conid": "corpusCusip_conid"}),
        on="corpusCusip_conid",
        how="left",
        rsuffix="_md",
    )

    # Optional filters
    if "original_maturity" in ust_final.columns:
        ust_final = ust_final[pd.to_numeric(ust_final["original_maturity"], errors="coerce") <= 10.05]

    snapshot_cols = [c for c in corpus_snap.columns if c != "conid"]
    existing_cols = [c for c in snapshot_cols if c in corpus_final.columns]
    if existing_cols:
        corpus_final = corpus_final.dropna(subset=existing_cols, how="all")

    # Re‑order columns
    ust_order = ["conid", "cusip"] + [c for c in ust_final.columns if c not in ("conid", "cusip")]
    corpus_order = ["corpusCusip_conid", "corpusCusip"] + [
        c for c in corpus_final.columns if c not in ("corpusCusip_conid", "corpusCusip")
    ]

    if not ust_final.empty:
        available_cols = [col for col in ust_order if col in ust_final.columns]
        ust_final = ust_final.reindex(columns=available_cols)
    corpus_final = corpus_final[[col for col in corpus_order if col in corpus_final.columns]]

    # Persist ZERO‑curve history (rolling window)
    now_ns = time.time_ns()
    corpus_final["timestamp_dt"] = now_ns

    zero_curve_path = INDEX_DIR / "zero_curve.csv"
    if zero_curve_path.exists():
        zero_curve_df = pd.read_csv(zero_curve_path)

        # Drop legacy Unnamed index columns if present
        zero_curve_df = zero_curve_df.loc[:, ~zero_curve_df.columns.str.contains("^Unnamed")]
    else:
        zero_curve_df = pd.DataFrame()

    # Concatenate with new data (reset index)
    zero_curve_df = pd.concat([zero_curve_df, corpus_final], ignore_index=True)

    # Clean yield columns (convert percent strings to floats)
    pct_cols = ["ask_yield", "bid_yield"]
    for col in pct_cols:
        zero_curve_df[col] = (
            zero_curve_df[col]
            .astype(str)
            .str.replace("%", "", regex=False)
            .replace("", np.nan)
            .astype(float)
        )

    # Trim to latest 10,000 rows
    if len(zero_curve_df) > 10000:
        zero_curve_df = zero_curve_df.iloc[-10000:]

    # Write to CSV with index suppressed
    zero_curve_df.to_csv("zero_curve.csv", index=False)
    logging.info("Zero curve length: %d", len(zero_curve_df))

    # Store back to config
    config.USTs = ust_final
    config.USTs.to_csv("config.USTs.csv")
    config.ZEROES = zero_curve_df

    logging.info(
        "[MarketData] %d UST, %d corpus rows @ %s (zero_curve rows: %d)",
        len(ust_final), len(corpus_final), datetime.now(), len(zero_curve_df)
    )

    return config.USTs, config.ZEROES

if __name__ == "__main__":
    refresh_market_data()
