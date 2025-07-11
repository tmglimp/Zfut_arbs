'''
mktdta.py > Live depth > weighted quotes > rolling snapshot
────────────────────────────────────────────────────────
* Computes size‑weighted bid/ask prices.
* Drops rows where price is missing, size < 2, or mid_price is missing.
* Maintains cfg.row_pool as a rolling buffer of the most recent 1 000 rows.
'''

import logging
import sys
import time
import threading
import pandas as pd
import requests
import cfg
import config
from fixed_income_calc import P2Y

LOCK = threading.Lock()

# ─────────────────────────────────────────
# logging setup
# ─────────────────────────────────────────
logging.basicConfig(
    level=cfg.LOG_LEVEL,
    format=cfg.LOG_FORMAT,
    handlers=[logging.FileHandler(cfg.LOG_FILE), logging.StreamHandler(sys.stdout)],
)

# ─────────────────────────────────────────
# helpers
# ─────────────────────────────────────────

def get_market_depth(token: str):
    depth_url = f"{cfg.base}/market/depth"
    params = [("symbols", f"XCBT:{sym}") for sym in cfg.current_contracts]
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(depth_url, headers=headers, params=params, timeout=30.0)
    resp.raise_for_status()
    time.sleep(0.000_001)
    return resp.json()["depths"]

def _wap(levels: list[dict]) -> tuple[float | None, int]:
    if not levels:
        return None, 0
    tot_sz = sum(l["sz"] for l in levels)
    if tot_sz == 0:
        return None, 0
    return sum(l["p"] * l["sz"] for l in levels) / tot_sz, tot_sz

# ─────────────────────────────────────────
# main entry
# ─────────────────────────────────────────

def _update_snapshot(df_target: pd.DataFrame, quote_df: pd.DataFrame, label: str):
    """Merge quote_df into df_target in‑place, matching by symbol + side."""
    if not isinstance(df_target, pd.DataFrame):
        logging.warning("cfg.%s is not a DataFrame; initialising placeholder", label)
        df_target = pd.DataFrame()

    snapshot = df_target.copy()

    # Ensure 'symbol' column is present in both DataFrames
    for df in (snapshot, quote_df):
        if "symbol" not in df.columns and "exchSym" in df.columns:
            df.rename(columns={"exchSym": "symbol"}, inplace=True)

    # Initial build: explode into Bid/Ask rows if not yet structured
    if "side" not in snapshot.columns:
        snapshot = pd.merge(
            snapshot.assign(side="Bid"),
            pd.concat([quote_df[quote_df.side == "Bid"], quote_df[quote_df.side == "Ask"]]),
            on=["symbol", "side"],
            how="left",
            validate="many_to_one",
        )
    else:
        snapshot["side"] = snapshot["side"].str.title()
        snapshot = snapshot.drop(columns=["price", "size", "mid_price"], errors="ignore")
        snapshot = snapshot.merge(quote_df, on=["symbol", "side"], how="left", validate="many_to_one")

    df_target[["price", "size", "mid_price"]] = snapshot[["price", "size", "mid_price"]].values
    return df_target, snapshot


def refresh_dta(token: str):
    """Fetch depth → update cfg.FUTURES.  After the update, *inherit* ZQ rows
    from cfg.FUTURES into cfg.zeroes (no separate merge) and keep the rolling
    non‑ZQ snapshot in cfg.row_pool.
    Returns (row_pool, zeroes).
    """
    logging.info("Fetching market data for futures contracts…")
    depths = get_market_depth(token)
    now_ns = time.time_ns()

    frames: list[dict] = []  # ALL symbols, ZQ included

    for row in depths:
        symbol = row["s"].split(":")[-1]

        bid_px, bid_sz = _wap(row["b"])
        ask_px, ask_sz = _wap(row["a"])
        mid_px = (bid_px + ask_px) / 2 if bid_px is not None and ask_px is not None else None

        frames.extend(
            [
                dict(symbol=symbol, side="Bid", price=bid_px, size=bid_sz, mid_price=mid_px, timestamp_dt=now_ns),
                dict(symbol=symbol, side="Ask", price=ask_px, size=ask_sz, mid_price=mid_px, timestamp_dt=now_ns),
            ]
        )

    if not frames:
        logging.warning("Depth API returned no usable rows.")
        cfg.FUTURES = pd.DataFrame()
        cfg.zeroes = pd.DataFrame()
        return pd.DataFrame(), pd.DataFrame()

    # ---------------- all contracts ----------------
    quote_df = pd.DataFrame(frames)
    quote_df["side"] = quote_df["side"].str.title()
    quote_df = quote_df[quote_df["price"].notnull() &quote_df["mid_price"].notnull() &quote_df["size"].notnull() &(quote_df["size"] >= 2)]

    # ensure cfg containers exist
    if not hasattr(cfg, "FUTURES"):
        cfg.FUTURES = pd.DataFrame()
    if not hasattr(cfg, "zeroes"):
        cfg.zeroes = pd.DataFrame()

    # merge/overwrite snapshots for ALL symbols
    cfg.FUTURES, fut_snapshot = _update_snapshot(cfg.FUTURES, quote_df, "FUTURES")
    cfg.FUTURES.to_csv('cfg.FUTURES.csv')

    with LOCK:
        # Initialize row_pool if missing or invalid
        if not hasattr(cfg, "row_pool") or not isinstance(cfg.row_pool, pd.DataFrame):
            cfg.row_pool = pd.DataFrame()

        # Append from cfg.FUTURES
        if not cfg.FUTURES.empty:
            cfg.row_pool = pd.concat([cfg.row_pool, cfg.FUTURES.copy()], ignore_index=True)

        # Filter for valid rows
        cfg.row_pool = cfg.row_pool[
            cfg.row_pool["price"].notna()
            & cfg.row_pool["mid_price"].notna()
            & (cfg.row_pool["size"] >= 10)
            ]

        # Trim to last 10,000 rows
        if len(cfg.row_pool) > 200:
            cfg.row_pool = cfg.row_pool.iloc[-200:]

        # Reorder columns to move 'conid' to front if it exists
        if "conid" in cfg.row_pool.columns:
            cols = cfg.row_pool.columns.tolist()
            cols.insert(0, cols.pop(cols.index("conid")))
            cfg.row_pool = cfg.row_pool[cols]

        # Save without index (clean format)
        cfg.row_pool.to_csv("row_pool.csv", index=False)

    logging.info("Market data updated.")
    time.sleep(0.0001)

