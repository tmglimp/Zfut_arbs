import time
import config
from cf_ctd import cf_ctd_main
from KPIs2_Orders import calculate_quantities
from ctd_fut_kpis import run_fixed_income_calculation
from leaky_bucket import leaky_bucket
from bl import refresh_dta
import cfg
from orders import orderRequest

def business_logic_function():
    """
    Continuously executes the business logic as a separate process all the way to the order placement.
    This function runs in a loop and executes every 3 seconds.
    """
    while True:
        if config.USTs is not None and config.FUTURES is not None and not config.USTs.empty and not config.FUTURES.empty:
            print("*** Business logic started: Obtaining data, balancing hedges, managing order queue. ***")
            leaky_bucket.wait_for_token()

            refresh_dta(cfg.token)
            HEDGES = cf_ctd_main()

            HEDGES_Combos = run_fixed_income_calculation(HEDGES)

            print("Populated HEDGES_Combos:")
            print(HEDGES_Combos)

            config.updated_ORDERS = calculate_quantities(HEDGES_Combos, config.SMA)
            print("Updated ORDERS:")
            print(config.updated_ORDERS)

            # Wait until config.ZEROES is under 1000 rows before placing orders
            if config.ZEROES is not None:
                zeroes_len = len(config.ZEROES)
                print(f"Length of config.ZEROES is {zeroes_len}")
                if zeroes_len > 2000:
                    orderRequest(config.updated_ORDERS)
                else:
                    print("Skipping orderRequest — config.ZEROES too small.")
            else:
                print("Skipping orderRequest — config.ZEROES is None.")

        time.sleep(0.000001)
