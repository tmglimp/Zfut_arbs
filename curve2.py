import logging
import numpy as np
import pandas as pd
from patsy import dmatrix, build_design_matrices
from scipy.optimize import minimize_scalar
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import config
import os
import threading

# ------------------- Design matrix grid config -------------------
knot_min = 4
knot_max = 11

# 16 explicitly defined parameter combinations
base_params = [
    (.001, .999, 0, 1), (.005, .995, 0, 1), (.01, .99, 0, 1), (.05, .95, 0, 1), (.1, .9, 0, 1), (.2, .8, 0, 1),
    (.001, .995, 0, 1), (.001, .99, 0, 1), (.001, .95, 0, 1), (.001, .9, 0, 1), (.001, .8, 0, 1),
    (.005, .999, 0, 1), (.01, .999, 0, 1), (.05, .999, 0, 1), (.1, .999, 0, 1), (.2, .999, 0, 1),
]

# Create all (k, qt_l, qt_u, b_l, b_u) combinations
param_combos = [
    (k, qt_l, qt_u, b_l, b_u)
    for k in range(knot_min, knot_max + 1)
    for (qt_l, qt_u, b_l, b_u) in base_params
]

results_lock = threading.Lock()
all_results = []

# ------------------- Spline helpers -------------------
def bspline_design_matrix(x, knots, degree=3):
    return dmatrix(
        f"bs(x, knots={knots.tolist()}, degree={degree}, include_intercept=False)",
        {"x": x},
        return_type='dataframe'
    )

def fit_penalized_spline(x, y, knots, lam, degree=3):
    X = bspline_design_matrix(x, knots, degree)
    X_np = X.to_numpy()
    D = np.eye(X_np.shape[1])
    XtX = X_np.T @ X_np
    XtY = X_np.T @ y
    beta_hat = np.linalg.solve(XtX + lam * D, XtY)
    y_hat = X_np @ beta_hat
    hat_matrix = X_np @ np.linalg.inv(XtX + lam * D) @ X_np.T
    trace_H = np.trace(hat_matrix)
    rss = np.sum((y - y_hat) ** 2)
    denom = (len(y) - trace_H)
    gcv = rss / denom**2 if denom > 1e-6 else np.inf
    return gcv, y_hat, beta_hat, X.columns, X.design_info

def gcv_objective(log_lambda, x, y, knots, degree=3):
    lam = np.exp(log_lambda)
    gcv, *_ = fit_penalized_spline(x, y, knots, lam, degree)
    return gcv

def is_too_jagged(x_sorted, y_hat_sorted, max_slope_change=0.00001):
    dx = np.diff(x_sorted)
    dy = np.diff(y_hat_sorted)
    with np.errstate(divide='ignore', invalid='ignore'):
        slopes = np.where(dx != 0, dy / dx, 0)
        slope_changes = np.abs(np.diff(slopes))
    return np.any(slope_changes > max_slope_change)

# ------------------- Thread worker -------------------
def thread_worker(k, qt_l, qt_u, b_l, b_u):
    try:
        df = config.ZEROES.copy().dropna(subset=['bid_yield', 'years_to_maturity'])
        df['years_to_maturity'] = pd.to_numeric(df['years_to_maturity'], errors='coerce')
        df = df.dropna(subset=['years_to_maturity'])

        x = df['years_to_maturity'].values
        y = df['bid_yield'].values
        knots = np.quantile(x, np.linspace(qt_l, qt_u, k))

        opt_result = minimize_scalar(
            gcv_objective,
            args=(x, y, knots),
            method='bounded', bounds=(b_l, b_u)
        )

        lam = np.exp(opt_result.x)
        gcv_score, y_hat, coefs, colnames, design_info = fit_penalized_spline(x, y, knots, lam)
        sorted_idx = np.argsort(x)
        x_sorted = x[sorted_idx]
        y_hat_sorted = y_hat[sorted_idx]

        with results_lock:
            all_results.append((gcv_score, lam, y_hat, coefs, colnames, design_info, knots, x_sorted, y_hat_sorted))
    except Exception as e:
        print(f"Thread failed: knots={k}, qt=({qt_l}, {qt_u}), bounds=({b_l}, {b_u})\n{e}")

# ------------------- Bootstrapping main -------------------
def boot_curve():
    threads = []
    for k, qt_l, qt_u, b_l, b_u in param_combos:
        t = threading.Thread(target=thread_worker, args=(k, qt_l, qt_u, b_l, b_u))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    if not all_results:
        print("No successful spline fits were completed.")
        return

    filtered = [r for r in all_results if not is_too_jagged(r[7], r[8])]
    best_result = min(filtered, key=lambda r: r[0]) if filtered else min(all_results, key=lambda r: r[0])

    best_gcv, best_lam, best_yhat, best_coef, best_colnames, best_design_info, best_knots, *_ = best_result

    ust_df = config.USTs
    ust_x_series = pd.to_numeric(ust_df['years_to_maturity'], errors='coerce')
    valid_rows = ~ust_x_series.isna()
    ust_x_series = ust_x_series[valid_rows]
    ust_df = ust_df[valid_rows]

    X_new = build_design_matrices([best_design_info], data={"x": ust_x_series})[0]
    ust_df['Bspln_yld_crv'] = (np.asarray(X_new) @ best_coef) / 100
    config.USTs = ust_df

    print(f"Best GCV Score: {best_gcv:.6f} with λ = {best_lam:.6f} and {len(best_knots)} knots.")
    print("config.USTs populated with imputed yields.")

if __name__ == "__main__":
    boot_curve()
