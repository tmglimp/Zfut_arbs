from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.ticktype import *
from ibapi.common import *
import threading
import time


class IBApp(EClient, EWrapper):
    def __init__(self):
        EClient.__init__(self, self)

    def nextValidId(self, orderId: int):
        self.reqIds(-1)
        self.request_depth()

    def request_depth(self):
        print("Requesting market depth...")
        contract = Contract()
        contract.secType = "BOND"
        contract.symbol = ""            # leave blank
        contract.exchange = "SMART"
        contract.currency = "USD"
        contract.cusip = "91282CJL6"    # Example 2y Treasury
        contract.conId = 0              # leave 0, CUSIP resolves it

        self.reqMktDepth(7001, contract, 5, False, [])  # requestId, contract, numRows

    def updateMktDepth(self, reqId, position, operation, side, price, size):
        print(f"Depth [{reqId}] | Pos: {position}, Side: {side}, Op: {operation}, Price: {price}, Size: {size}")

    def updateMktDepthL2(self, reqId, position, marketMaker, operation, side, price, size, isSmartDepth):
        print(f"L2 Depth [{reqId}] | MM: {marketMaker}, Pos: {position}, Side: {side}, Price: {price}, Size: {size}")

    def error(self, reqId, errorCode, errorString):
        print(f"Error [{reqId}] {errorCode}: {errorString}")


def run_loop(app):
    app.run()


if __name__ == "__main__":
    app = IBApp()
    app.connect("127.0.0.1", 4002, clientId=1)  # Make sure TWS is running on this port

    thread = threading.Thread(target=run_loop, args=(app,))
    thread.start()

    time.sleep(3)  # Keep script alive to receive depth data
    app.disconnect()
