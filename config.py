# Configuration settings for bond trading application
# has new global control scalars for optimization
import pandas as pd

from cfg import row_pool

#IBKR Client Portal Web API
IBKR_BASE_URL = "https://localhost:5000"
IBKR_ACCT_ID = ""  # populate with IBKR Acct ID.

# Logging Settings
LOG_FORMAT = "'%(asctime)s - %(name)s - %(levelname)s - %(message)s'"
LOG_LEVEL = "INFO"
LOG_FILE = "application.log"

### SCALARS ###
risk_reducer = .45 # scales net rev after fees to remain w/in CME circuit band
PRICE_EXP = 1 # expands instantaneous spread as scalar
VS = 15 #denominator coefficient for logarithmic volume scalar penalty fn (larger no. = less weight to vol var in RENTD).
SPREAD_POP = .25 # of accumulated rows to pass for determining spread width
PEND_CLEAR = 3 # max/s to let position sit in queue before DEL operation.

### MARGIN ###
MARGIN_CUSHION = .05 # Margin cushion (volatility control measure)
UNDER = 1 - MARGIN_CUSHION #KPIs2Orders Reciprocal % of acct value to use determining nominal size
INIT_MARG_THRESH = pd.DataFrame()
INITIAL_MARGIN = pd.DataFrame()

### DICTIONARY OBJECTS ###
FUT_SYMBOLS = ["ZT","ZF","Z3N"]
curve_years = 6 # adjust with symbols array
row_pool = pd.DataFrame()
USTs = pd.DataFrame()  # DF to be populated during runtime
FUTURES = pd.DataFrame()  # DF to be populated during runtime
ZEROES = pd.DataFrame()
HEDGES = pd.DataFrame()
HEDGES_Combos = pd.DataFrame()
zero_list = pd.DataFrame()
X = 0

### IB datas###
row_pool = pd.DataFrame()

### ORDERS ###
ACTIVE_ORDERS_LIMIT = 5  # Limit for number of active orders per time
VOLUME = 1
ORDERS = pd.DataFrame()  # to be populated during runtime
updated_ORDERS = pd.DataFrame() # to be populated during runtime
SUPPRESSED_IDS = "o163,o451,o354,o383"  # Applicable message ids to suppress
placed_orders_runtime = pd.DataFrame()
TOTAL_OVERLAY = pd.DataFrame()
SMA = pd.DataFrame()

### HISTORICAL DATA FOR RISK METRICS ###
FUTURES_VARIANCE = pd.DataFrame()


