# Zfut_arbs

This futures arbitrage algorithm for the IBKR FCM suite has the right parts to satisfy the Volcker risk-mitigation rules with {ZT,Z3N,ZF,ZN,TN} spread pairs. 
CME/CBOT recognize emergent orders as legged spreads and covered UDS recursions in the exchange API's spreads panel.

Recovering corpusCusips to boot the curve from zeros requires credentials for the Treasury's Fiscal Services API in addition to an IBKR account. 
I found it generates more results than IBKR's scanner for contract discovery which leads to a more accurate curve fit, but you could probably take a shortcut and do just the IBKR way if you want.
https://api-community.fiscal.treasury.gov/s/communityapi/a01Qo00000pDeK7IAK/enterprise-apisustreasurymarketablesecuritiesexperienceapi?tabset-83a38=2

Dependencies include statsmodels, pandas, scipy, numpy, patsy, matplotlib, json, urllib3, requests, math, datetime, and itertools.

Many rights are reserved by Mr. Thomas Madison Glimp and Mr. Victor Irechukwu.  
Others are waived for your use with attribution under the MIT license. 

Big thanks to the U. IL at Chicago's MSc-Fin staff.
6.23.2025.
