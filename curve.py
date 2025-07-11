import logging
import numpy as np
import pandas as pd
from patsy import dmatrix, build_design_matrices
from scipy.optimize import minimize_scalar
import matplotlib
matplotlib.use("Agg")  # Non-GUI backend for thread-safe PDF generation
import matplotlib.pyplot as plt
import config
import os

### ### ### spline fitting design matrix ### ### ###
knot_min = 4 # sets min eigenknot count
knot_max = 12 # sets max eigenknot count
qt_lower = .001 # GCV min quantile fit param
qt_upper = .999 # GCV max quanitle fit param
bounds_lower = 0 # lower alpha penalty param
bounds_upper = 1 # upper alpha penalty param
### ### ### ### ### ### ### ### ### ### ### ### ###



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

def boot_curve():
    df = config.ZEROES.copy()  # <-- Start with a true copy of the original
    df = df.dropna(subset=['bid_yield', 'years_to_maturity']).copy()
    df['years_to_maturity'] = pd.to_numeric(df['years_to_maturity'], errors='coerce')
    df = df.dropna(subset=['years_to_maturity']).copy()
    x = df['years_to_maturity'].values
    y = df['bid_yield'].values


    # Create output dir for plots
    plot_dir = "spline_plots"
    os.makedirs(plot_dir, exist_ok=True)

    # Try multiple knot counts
    results = []
    for k in range(knot_min, knot_max):
        knots = np.quantile(x, np.linspace(qt_lower, qt_upper, k))
        opt_result = minimize_scalar(
            gcv_objective,
            args=(x, y, knots),
            method='bounded', bounds=(bounds_lower, bounds_upper)
        )

        lam = np.exp(opt_result.x)
        gcv_score, y_hat, coefs, colnames, design_info = fit_penalized_spline(x, y, knots, lam)

        sorted_idx = np.argsort(x)
        x_sorted = x[sorted_idx]
        y_hat_sorted = y_hat[sorted_idx]

        results.append((gcv_score, lam, y_hat, coefs, colnames, design_info, knots, x_sorted, y_hat_sorted))

        # Save plot to PDF
        plt.figure(figsize=(10, 6))

        # Jitter and scatter points
        jitter = np.random.normal(0, 0.03, size=x.shape)
        x_jittered = x + jitter
        plt.scatter(
            x_jittered,
            y,
            color='gray',
            edgecolors='black',
            linewidths=0.25,
            s=10,
            alpha=0.9,
            label='Observed'
        )

        # Plot the fitted spline
        plt.plot(x_sorted, y_hat_sorted, color='blue', linewidth=1.5, label=f'Knots={k}, λ={lam:.2f}')
        plt.title(f'Spline Fit with {k} Knots\nGCV={gcv_score:.2e}, λ={lam:.2f}')
        plt.xlabel("Years to Maturity")
        plt.ylabel("Yield")
        plt.legend()
        plt.tight_layout()
        fname = os.path.join(plot_dir, f"spline_fit_knots_{k}.pdf")
        plt.savefig(fname)
        plt.close()
        print(f"Saved: {fname}")

    # Filter out jagged fits
    filtered_results = [r for r in results if not is_too_jagged(r[7], r[8])]
    if filtered_results:
        best_result = min(filtered_results, key=lambda r: r[0])
    else:
        best_result = min(results, key=lambda r: r[0])  # fallback

    best_gcv, best_lam, best_yhat, best_coef, best_colnames, best_design_info, best_knots, *_ = best_result

    # Evaluate spline with lowest GCV score at UST maturities using build_design_matrices
    ust_df = config.USTs
    ust_x_series = pd.to_numeric(ust_df['years_to_maturity'], errors='coerce')
    valid_rows = ~ust_x_series.isna()
    ust_x_series = ust_x_series[valid_rows]
    ust_df = ust_df[valid_rows]
    X_new = build_design_matrices([best_design_info], data={"x": ust_x_series})[0]
    X_new_np = np.asarray(X_new)
    ust_df['Bspln_yld_crv'] = (X_new_np @ best_coef)
    ust_df['Bspln_yld_crv'] = ust_df['Bspln_yld_crv']/100
    config.USTs = ust_df
    print(f'config.USTs with imputed yields', config.USTs)

    # Curve results
    print(f"Best GCV Score: {best_gcv:.6f} with λ = {best_lam:.6f} and {len(best_knots)} knots.")
    print(f'config.USTs populated with imputed yields.')

if __name__ == "__main__":
    boot_curve()