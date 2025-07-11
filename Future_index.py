import logging
import sys
from datetime import datetime
import pandas as pd
import requests
import urllib3
import config
from enums.ContractField import ContractField
from contract import Contract
from enums.MarketDataField import MarketDataField
from fixed_income_calc import compute_settlement_date, calculate_term
from leaky_bucket import leaky_bucket

# Configure logging
logging.basicConfig(level=config.LOG_LEVEL, format=config.LOG_FORMAT, handlers=[
    logging.FileHandler(config.LOG_FILE),
    logging.StreamHandler(sys.stdout)
])

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def scan(symbol):
    url = config.IBKR_BASE_URL + f"/v1/api/trsrv/futures?symbols={symbol}"
    leaky_bucket.wait_for_token()
    logging.info(f'Requesting futures contracts from {url}...')
    try:
        response = requests.get(url=url, verify=False)
        if response.status_code != 200:
            logging.error(f'Failed to fetch futures for {symbol}. Status: {response.status_code}')
            return {}
        logging.info(f'Successfully retrieved futures for {symbol}.')
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f'Exception during IBKR futures scan for {symbol}: {e}')
        return {}

def extract_futures_contracts(futures):
    if futures is not None:
        return [contract for contracts in futures.values() for contract in contracts]
    return []

def filter_futures_by_expiry(futures, year_to_maturity=11):
    trade_settle_date = compute_settlement_date(datetime.today().strftime('%Y%m%d'))
    return [f for f in futures if calculate_term(str(trade_settle_date), str(f['expirationDate'])) < year_to_maturity]

def extract_contract_fields(contract_details):
    contracts = []
    logging.info("Extracting contract data fields: Started...")
    for contract in contract_details:
        if contract.get("error"):
            continue
        contract_info = {
            ContractField.con_id.value: contract.get(ContractField.con_id.value),
            ContractField.currency.name: contract.get(ContractField.currency.value),
            ContractField.ticker.name: contract.get(ContractField.ticker.value),
            ContractField.full_name.name: contract.get(ContractField.full_name.value),
            ContractField.all_exchanges.name: contract.get(ContractField.all_exchanges.value),
            ContractField.listing_exchanges.name: contract.get(ContractField.listing_exchanges.value),
            ContractField.asset_class.name: contract.get(ContractField.asset_class.value),
            ContractField.expiry.name: contract.get(ContractField.expiry.value),
            ContractField.last_trading_day.name: contract.get(ContractField.last_trading_day.value),
            ContractField.strike.name: contract.get(ContractField.strike.value),
            ContractField.underlying_conid.name: contract.get(ContractField.underlying_conid.value),
            ContractField.underlying_exchange.name: contract.get(ContractField.underlying_exchange.value),
            ContractField.multiplier.name: contract.get(ContractField.multiplier.value),
            ContractField.increment.name: next(iter(contract.get('incrementRules', {})), {}).get(
                ContractField.increment.value),
            ContractField.increment_lower_edge.name: next(iter(contract.get('incrementRules', {})), {}).get(
                ContractField.increment_lower_edge.value)
        }

        trade_settle_date = compute_settlement_date(datetime.today().strftime('%Y%m%d'))
        contract_info[ContractField.year_to_maturity.name] = (
            calculate_term(str(trade_settle_date), contract_info[ContractField.expiry.name]))

        contracts.append(contract_info)
    logging.info("Extracting contract data fields: Completed.")
    return contracts

def convert_price_to_decimal(df):
    df["ask_price"] = pd.to_numeric(df["ask_price"], errors='coerce')
    df["bid_price"] = pd.to_numeric(df["bid_price"], errors='coerce')
    df["price"] = df[["ask_price", "bid_price"]].mean(axis=1)
    return df

def main():
    all_contracts = []
    for symbol in config.FUT_SYMBOLS:
        logging.info(f"Scanning symbol: {symbol}")
        futures = scan(symbol)
        contracts = extract_futures_contracts(futures)
        all_contracts.extend(contracts)

    logging.info(f"Discovered a total of {len(all_contracts)} Treasury futures.")
    security_definitions = Contract.get_security_definition(all_contracts)
    if not security_definitions:
        logging.error("Unable to fetch contract security definitions.")
        return

    structured_contracts = extract_contract_fields(security_definitions)
    config.FUTURES = pd.DataFrame(structured_contracts)
    config.FUTURES.to_csv("FUTURES.index", index=False)

if __name__ == "__main__":
    main()
