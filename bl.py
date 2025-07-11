import logging
import sys
import cfg
from mktdta import refresh_dta

# Configure logging
logging.basicConfig(
    level=cfg.LOG_LEVEL,
    format=cfg.LOG_FORMAT,
    handlers=[
        logging.FileHandler(cfg.LOG_FILE),
        logging.StreamHandler(sys.stdout)
    ]
)

def business_logic_function():
    """"
    This function performs the initial setup for the core trading logic by:
    1. Logging a startup message to indicate that trading operations are beginning.
    2. Delegating the initialization of concurrent symbol-specific trading threads to `start_symbol_threads()`.
   """
    logging.info("*** Business logic started: Obtaining data, balancing hedges, managing order queue. ***")

    refresh_dta(cfg.token)
    return cfg.row_pool