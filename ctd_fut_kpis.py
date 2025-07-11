"""
CTD and FUT KPIs
"""
import time
import itertools
import pandas as pd
import config
from config import HEDGES
from fixed_income_calc import MDur, MacDur, DV01, Cvx

# Generate the CTD DataFrame (HEDGES) using the function

def display_hedges_info():
    print("Displaying first 5 rows of HEDGES dataframe:")
    print(HEDGES.head())

def run_fixed_income_calculation(HEDGES):
    # Ensure the HEDGES dataframe has uppercase column names.
    HEDGES.columns = HEDGES.columns.str.upper()

    # Define constant parameters
    period = 2
    day_count = 1
    settle_date = time.strftime('%Y%m%d')

    # Compute CTD KPIs using the uppercase column names.

    HEDGES['CTD_MDUR'] = HEDGES.apply(
        lambda row: MDur(cpn=row['CTD_COUPON'],
                         term=row['CTD_YTM'],
                         yield_=row['CTD_BSPLN_YLD_CRV'],
                         period=period,
                         begin=row['CTD_PREV_COUPON'],
                         settle=settle_date,
                         next_coupon=row['CTD_NEXT_COUPON'],
                         day_count=day_count), axis=1)

    HEDGES['CTD_CVX'] = HEDGES.apply(
        lambda row: Cvx(cpn=row['CTD_COUPON'],
                           term=row['CTD_YTM'],
                           yield_=row['CTD_BSPLN_YLD_CRV'],
                           period=period,
                           begin=row['CTD_PREV_COUPON'],
                           settle=settle_date,
                           next_coupon=row['CTD_NEXT_COUPON'],
                           day_count=day_count), axis=1)

    HEDGES['CTD_MACDUR'] = HEDGES.apply(
        lambda row: MacDur(cpn=row['CTD_COUPON'],
                           term=row['CTD_YTM'],
                           yield_=row['CTD_BSPLN_YLD_CRV'],
                           period=period,
                           begin=row['CTD_PREV_COUPON'],
                           settle=settle_date,
                           next_coupon=row['CTD_NEXT_COUPON'],
                           day_count=day_count), axis=1)

    HEDGES['CTD_DV01'] = HEDGES.apply(
        lambda row: DV01(cpn=row['CTD_COUPON'],
                         term=row['CTD_YTM'],
                         yield_=row['CTD_BSPLN_YLD_CRV'],
                         period=period,
                         begin=row['CTD_PREV_COUPON'],
                         settle=settle_date,
                         next_coupon=row['CTD_NEXT_COUPON'],
                         day_count=day_count), axis=1)

    # Now compute the FUT KPIs by dividing the CTD metrics by the conversion factor.
    HEDGES['FUT_MDUR'] = HEDGES['CTD_MDUR'] / HEDGES['CTD_CONVERSION_FACTOR']
    HEDGES['FUT_MACDUR'] = HEDGES['CTD_MACDUR'] / HEDGES['CTD_CONVERSION_FACTOR']
    HEDGES['FUT_DV01'] = HEDGES['CTD_DV01'] / HEDGES['CTD_CONVERSION_FACTOR']
    HEDGES['FUT_CVX'] = HEDGES['CTD_CVX'] / HEDGES['CTD_CONVERSION_FACTOR']

    combinations = [(row1, row2) for row1, row2 in itertools.product(HEDGES.iterrows(), repeat=2)
                    if row1[1]['CTD_CONID'] != row2[1]['CTD_CONID']]

    combos_data = []
    for combo in combinations:
        row1, row2 = combo
        # Prefix the headers of row1 with 'A_' and row2 with 'B_'
        row1_data = {f'A_{key}': value for key, value in row1[1].to_dict().items()}
        row2_data = {f'B_{key}': value for key, value in row2[1].to_dict().items()}
        combined_row = {**row1_data, **row2_data}
        combos_data.append(combined_row)
    HEDGES_Combos = pd.DataFrame(combos_data)

    # Trim to latest 10,000 rows
    if len(HEDGES_Combos) > 750:
        HEDGES_Combos = HEDGES_Combos.iloc[-750:]

    config.HEDGES_Combos
    return HEDGES_Combos
    time.sleep(.1)
if __name__ == "__main__":
    display_hedges_info()
    combos = run_fixed_income_calculation(HEDGES)
    print("Fixed income calculation completed. CTD-FUT combinations shape:", combos.shape)
