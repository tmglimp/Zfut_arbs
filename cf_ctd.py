import logging
import cfg
from cfg import row_pool
import config
import pandas as pd
import numpy as np
from datetime import datetime
from market_data import refresh_market_data
from mktdta import refresh_dta
from curve import boot_curve
from fixed_income_calc import BPrice

# ---------------- Market Data Import ----------------
def refresh_data():
    refresh_market_data()
    refresh_dta(cfg.token)
    return

# ---------------- Helper Function ----------------
def safe_datetime(val):
    try:
        if pd.isna(val):
            return None
        dt = pd.to_datetime(val, errors='coerce')
        return None if pd.isna(dt) else dt
    except Exception:
        return None

# ---------- IRR-based Fair Value Derivation for CTD Baskets ----------------
def fair_value_derivation():
    implied = config.USTs.__deepcopy__()

    implied['years_to_maturity'] = pd.to_numeric(implied['years_to_maturity'], errors='coerce')
    implied['coupon'] = pd.to_numeric(implied['coupon'], errors='coerce')
    implied['Bspln_yld_crv'] = pd.to_numeric(implied['Bspln_yld_crv'], errors='coerce')
    implied['conversion_factor'] = pd.to_numeric(implied['conversion_factor'], errors='coerce')
    implied['mid_price'] = pd.to_numeric(implied.get('mid_price'), errors='coerce')

    implied['prev_coupon'] = implied['prev_coupon'].apply(safe_datetime)
    implied['maturity_date'] = implied['maturity_date'].apply(safe_datetime)
    implied['next_coupon'] = implied['next_coupon'].apply(safe_datetime)

    required_cols = ['coupon', 'Bspln_yld_crv', 'conversion_factor', 'prev_coupon', 'maturity_date', 'next_coupon']
    for col in required_cols:
        print(f"-> {implied[col].isna().sum()} missing in {col}")

    implied = implied.dropna(subset=required_cols)
    print(f"\nRemaining after dropna: {len(implied)} rows")

    implied['prev_coupon'] = implied['prev_coupon'].apply(lambda x: x.strftime('%Y%m%d') if not pd.isna(x) else None)
    implied['maturity_date'] = implied['maturity_date'].apply(lambda x: x.strftime('%Y%m%d') if not pd.isna(x) else None)
    implied['next_coupon'] = implied['next_coupon'].apply(lambda x: x.strftime('%Y%m%d') if not pd.isna(x) else None)
    settle_date = datetime.today().strftime('%Y%m%d')

    implied['BPrice'] = implied.apply(lambda row: BPrice(
        cpn=row['coupon'],
        term=row['years_to_maturity'],
        yield_=row['Bspln_yld_crv'],
        period=2,
        begin=row['prev_coupon'],
        settle=settle_date,
        next_coupon=row['next_coupon'],
        day_count=1
    ), axis=1)

    implied['price'] = implied['mid_price']
    implied.to_csv('implied.csv')

    return implied

# ---------- CTD Pairing ----------------
def ctd_pairing(HEDGES, implied):
    print("Starting CTD pairing")
    HEDGES = cfg.row_pool.copy()

    for idx, fut in HEDGES.iterrows():
        symbol = fut.get("symbol", "")[:2]
        sym_full = fut.get("symbol", "")
        expiry = fut.get("ytm", np.nan)
        fut_price = fut.get("price", np.nan)

        print(f"\n-> Processing row {idx}: symbol={sym_full}, expiry={expiry}, Fwd Mkt Price={fut_price}")

        if pd.isna(expiry) or pd.isna(fut_price):
            continue

        # Determine deliverable window and original maturity cap
        if symbol == "ZQ":
            lower = expiry
            upper = lower + (31 / 360)
            max_origin = np.inf
        elif symbol == "ZT":
            lower = expiry + 1.74
            upper = expiry + 2.03
            max_origin = 5.25
        elif symbol == "Z3":
            lower = expiry + 2.74
            upper = expiry + 3.03
            max_origin = 7.0
        elif symbol == "ZF":
            lower = expiry + 4.1677
            upper = expiry + 5.28
            max_origin = 5.25
        elif symbol == "ZN":
            lower = expiry + 6.5
            upper = expiry + 8.03
            max_origin = 10.0
        elif symbol == "TN":
            lower = expiry + 9.5
            upper = expiry + 10.03
            max_origin = 10.0
        else:
            print(f"Unknown prefix for symbol '{sym_full}' — skipping")
            continue

        print(f"-> Deliverable range: {lower:.2f} to {upper:.2f}, max_origin: {max_origin}")
        (implied["original_maturity"].astype(float) <= max_origin)
        candidates = implied[(implied["years_to_maturity"] >= lower) &
                             (implied["years_to_maturity"] <= upper) &
                             (pd.to_numeric(implied["original_maturity"], errors="coerce") <= max_origin)
                             ].copy()

        print(f"-> {len(candidates)} candidates after filter for {symbol}")

        if candidates.empty or "BPrice" not in candidates or candidates["BPrice"].isna().all():
            print("Candidates missing price column or all prices are NaN")
            continue

        # Redefine IRR as: (Futures Price * CF) - BPrice
        candidates["IRR"] = fut_price * candidates["conversion_factor"] - candidates["BPrice"]
        print(f"IRR calculated for {len(candidates)} candidates")
        candidates["YTM"] = candidates["years_to_maturity"]

        #ctd = candidates.sort_values("IRR", ascending=False).iloc[0]
        ctd_back = candidates.sort_values("IRR", ascending=True).iloc[0]
        print(f"{sym_full} CTDB conid: {ctd_back['conid']}, IRR: {ctd_back['IRR']:.6f}")

        for col in [
                "Bspln_yld_crv", "bid_price", "ask_price", "bid_yield", "ask_yield", "coupon", "YTM","maturity_date",
                "conid", "cusip", "prev_coupon", "next_coupon", "conversion_factor", "BPrice", "IRR",]:
                HEDGES.loc[idx, f"CTD_{col.upper()}"] = ctd_back.get(col, np.nan)

    print("CTD pairing complete")
    HEDGES.to_csv("HEDGES.csv", index=False)
    config.HEDGES = HEDGES
    return HEDGES

# ---------------- Main ----------------
def cf_ctd_main():
    logging.info("Starting cf_ctd processing script.")
    refresh_data()
    boot_curve()
    implied = fair_value_derivation()
    HEDGES = ctd_pairing(row_pool, implied)
    return HEDGES

if __name__ == "__main__":
    cf_ctd_main()
