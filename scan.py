import warnings
from datetime import datetime, timedelta
import re
import pandas as pd
import requests
import urllib3
from dateutil.relativedelta import relativedelta

# --------------------------------------------------------------------------- #
# Config – edit as needed
# --------------------------------------------------------------------------- #
BASE_URL = "https://127.0.0.1:5000"
SCAN_URL = f"{BASE_URL}/v1/api/iserver/scanner/run"

MAX_ROWS_PER_SCAN = 200        # Trim very large scans for manageability
VERIFY_SSL = False             # Set True if you have valid certs

# --------------------------------------------------------------------------- #
# One-off: silence “InsecureRequestWarning” if VERIFY_SSL=False
# --------------------------------------------------------------------------- #
if not VERIFY_SSL:
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    warnings.filterwarnings("ignore", category=urllib3.exceptions.InsecureRequestWarning)

# --------------------------------------------------------------------------- #
# Helper to build the JSON payload IBKR expects
# --------------------------------------------------------------------------- #
def build_scan_body(start_dt: datetime, end_dt: datetime) -> dict:
    """Return scanner body with the supplied maturity‑date bounds (inclusive)."""
    return {
        "instrument": "BOND.GOVT",
        "location": "BOND.GOVT.US",
        "type": "BOND_CUSIP_AZ",
        "filter": [
            {"code": "maturityDateAbove", "value": start_dt.strftime("%Y%m%d")},
            {"code": "maturityDateBelow", "value": end_dt.strftime("%Y%m%d")},
            {"code": "excludeTypes", "value": "STRIPS_TIPS_INTERESTS,STRIPS_INTERESTS,BOND,NOTE"},
            {
                "code": "includeTypes",
                "value": "STRIPS_PRINCIPALS,NOTES_STRIPS_PRINCIPALS,STRIPS_TIPS_PRINCIPALS,BILLS,BILL",
            },
        ],
    }

# --------------------------------------------------------------------------- #
# Execute one scan and post‑process results
# --------------------------------------------------------------------------- #
def run_one_scan(body: dict, tag: str) -> pd.DataFrame:
    """Run a single scanner query and return a normalised DataFrame."""
    try:
        resp = requests.post(SCAN_URL, json=body, verify=VERIFY_SSL, timeout=10)
        resp.raise_for_status()
    except requests.RequestException as exc:
        print(f"[Scanner] request failed: {exc}")
        return pd.DataFrame()

    raw_contracts = resp.json().get("contracts", [])

    # keep only principal STRIPs
    filtered = [c for c in raw_contracts if "Principals" in c.get("contract_description_2", "")]
    filtered = filtered[:MAX_ROWS_PER_SCAN]
    if not filtered:
        return pd.DataFrame()

    df = pd.json_normalize(filtered)

    # conid may arrive as either `con_id` or `conidex`
    if "conidex" in df.columns and "con_id" not in df.columns:
        df = df.rename(columns={"conidex": "con_id"})

    df = df[["con_id", "contract_description_2"]]
    df = df.rename(columns={"con_id": "conid"})
    df["window_tag"] = tag
    return df

# --------------------------------------------------------------------------- #
# Run the 5 maturity buckets and combine
# --------------------------------------------------------------------------- #
def fetch_principal_strips() -> pd.DataFrame:
    """Query IBKR five times (1‑2y, 2‑4y, 4‑6y, 6‑8y, 8‑11y) and return a deduped DataFrame."""
    today = datetime.today()
    windows = [
        (today + relativedelta(months=+9), today + relativedelta(years=+2)),
        (today + relativedelta(years=+2), today + relativedelta(years=+4)),
        (today + relativedelta(years=+4), today + relativedelta(years=+6)),
        (today + relativedelta(years=+6), today + relativedelta(years=+8)),
        (today + relativedelta(years=+8), today + relativedelta(years=+11)),
    ]

    all_scans: list[pd.DataFrame] = []
    for start, end in windows:
        tag = f"{round((start - today).days / 365, 2)}-{round((end - today).days / 365, 2)}y"
        df = run_one_scan(build_scan_body(start, end), tag)
        print(f"[Window {tag}] {len(df)} rows")
        all_scans.append(df)

    combined = pd.concat(all_scans, ignore_index=True).drop_duplicates(subset=["conid"])
    return combined

# --------------------------------------------------------------------------- #
# Regex helpers to enrich the final dataframe
# --------------------------------------------------------------------------- #
def extract_cusip_and_date(df: pd.DataFrame) -> pd.DataFrame:
    """Add `cusip`, `maturity_date`, and `years_to_maturity` columns derived from `contract_description_2`."""
    def _cusip(text: str):
        m = re.search(r"\b(912[0-9A-Z]{6,7})\b", text)
        return m.group(1) if m else None

    def _mat_date(text: str):
        m = re.search(r"([A-Z][a-z]{2})(\d{2})'(\d{2})", text)
        if not m:
            return None
        mon, day, yr = m.groups()
        try:
            return datetime.strptime(f"{mon} {day} 20{yr}", "%b %d %Y")
        except ValueError:
            return None

    df["cusip"] = df["contract_description_2"].apply(_cusip)
    df["maturity_date"] = df["contract_description_2"].apply(_mat_date)
    df["maturity_date"] = pd.to_datetime(df["maturity_date"], errors="coerce")

    today = datetime.today()
    df["years_to_maturity"] = df["maturity_date"].apply(
        lambda dt: round((dt - today).days / 365.25, 2) if pd.notnull(dt) else None
    )
    df["maturity_date"] = df["maturity_date"].dt.strftime("%Y-%m-%d")

    return df

# --------------------------------------------------------------------------- #
# Public API: pop_zeroes()
# --------------------------------------------------------------------------- #
def pop_zeroes(save_path: str | None = None, verbose: bool = True) -> pd.DataFrame:
    """Return a dataframe of Principal STRIPs and optionally save to CSV.

    Parameters
    ----------
    save_path : str | None
        If provided, the dataframe is written to this CSV path (e.g. "zeroes.index.csv").
    verbose : bool
        When *True* print progress and a preview to stdout.
    """
    if verbose:
        pd.set_option("display.max_rows", None)

    result_df = fetch_principal_strips()
    if result_df.empty:
        if verbose:
            print("\nNo eligible Principal STRIPs found in any window.")
        return pd.DataFrame()

    zero_list = extract_cusip_and_date(result_df)

    if save_path:
        zero_list.to_csv(save_path, index=False)

        if verbose:
            print(f"\nSaved {len(zero_list)} rows to {save_path}")

    if verbose:
        print("\nzero_list preview:")
        print(zero_list[["conid", "contract_description_2", "cusip", "maturity_date", "years_to_maturity", "window_tag"]].to_string(index=False))

    return zero_list

# --------------------------------------------------------------------------- #
# Stand‑alone invocation
# --------------------------------------------------------------------------- #
if __name__ == "__main__":
    pop_zeroes(save_path="zeroes.index.csv")