# Zfut_arbs

This algorithm for the IBKR FCM suite has the right parts to satisfy Volcker's risk-mitigation rules with zero-basis {ZT,ZF,Z3N,ZN} diagonal calendar spread pairs.
See: https://www.sec.gov/files/rules/final/2020/bhca-9.pdf#page=4
See  also: https://www.cftc.gov/sites/default/files/idc/groups/public/@newsroom/documents/file/volckerrule_factsheet_final.pdf#page=2

CME/CBOT recognize the method as a UDS recursion in the exchange API's spreads panel. Recovering corpusCusips to boot the curve from zeros requires credentials for the Treasury's API in addition to an IBKR account. I have found that it is much more reliable than IBKR's scanner for contract discovery.  
https://api-community.fiscal.treasury.gov/s/communityapi/a01Qo00000pDeK7IAK/enterprise-apisustreasurymarketablesecuritiesexperienceapi?tabset-83a38=2

As set out in the MVP.txt file, all rights are reserved by T. Madison Glimp with fair credit attributed to Victor Irechukwu.
Use with attribution permitted permitted on request. Commercial licenses available.

Big thanks to the U. IL at Chicago's MSc-Fin staff for .
5.11.2025.
