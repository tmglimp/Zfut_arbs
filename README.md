##### Zfut_arbs.py #####

This futures arbitrage algorithm has the right parts to satisfy Volcker quant's risk-mitigating hedging rules with {ZT,Z3N,ZF,ZN,TN} spread pairs.* CME/CBOT recognizes orders that emerge from the algo as legged spreads or covered UDS recursions.

**Dependencies**
Execution--IBKR account with futures trade privileges enabled. 
CBOT UST Futures Market Data--Ironbeam account with market data enabled.
UST CTD basket and 0 coupon index--Treasury's Fiscal Services API account with platinum service level. You could take a shortcut and only use IBKR's bond data, but you'll come up short about ~50-60 zero coupon CUSIPs for the 10 year yield curve depending on auction schedules.
Python IDE--statsmodels, pandas, scipy, numpy, patsy, matplotlib, json, urllib3, requests, math, datetime, and itertools.


Many rights are reserved by Mr. Thomas Madison Glimp and Mr. Victor Irechukwu. Others are waived for your use with attribution under the MIT license. 

Big thanks to the U. IL at Chicago's MSc-Fin staff and especially Mr. John Miller. 
7.11.2025.

*This opinion is premised upon conveyances from a Goldman Sachs FIS trade desk alum. It is not intended to constitute legal advice, nor should it be relied upon as such.
