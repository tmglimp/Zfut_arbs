import pandas as pd

# IronBeam API Details
base = "https://demo.ironbeamapi.com/v2"
username = ""
password = ""
api_key = ""
token = ""  # populated during runtime

# Symbols to Query
exchange = 'XCBT'  # primary exchange
market_groups = ["ZT","Z3N","ZF"]
current_contracts = []  # populated during runtime

# Mkt Data
market_data_thread_started = None
row_pool = pd.DataFrame()  # DF to be populated during runtime for bid/ask mean pricing
SPREAD_POP = .25  # Temporal duration of MA used for deriving price mean value in market_data.py

FUTURES = pd.DataFrame()

# Logging Settings
LOG_FORMAT = "'%(asctime)s - %(name)s - %(levelname)s - %(message)s'"
LOG_LEVEL = "INFO"
LOG_FILE = "application.log"
