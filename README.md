# Zfut_arbs

This algorithm for the IBKR FCM suite has the right parts to satisfy Volcker's risk-mitigation rules for insurance companies with zero-basis {ZT,ZF,Z3N,ZN} diagonal calendar spread pairs. CME/CBOT recognize the method as a UDS recursion in the exchange API's spreads panel. 

Recovering corpusCusips to boot the curve from zeros requires credentials for the Treasury's API in addition to an IBKR account. I have found that it is much more reliable than IBKR's scanner for contract discovery, but you could probably try the IBKR way if you wanted.  
https://api-community.fiscal.treasury.gov/s/communityapi/a01Qo00000pDeK7IAK/enterprise-apisustreasurymarketablesecuritiesexperienceapi?tabset-83a38=2

Requires as an additional dependency entailing Mark Fisher's yield curve package for Mathematica/Wolfram Engine:
http://markfisher.net/~mefisher/mma/mathematica.html

All rights are reserved by Mr. Thomas Madison Glimp with fair credit attributed to Mr. Victor Irechukwu.
Use with attribution permitted permitted on request. Commercial licenses available.

Big thanks to the U. IL at Chicago's MSc-Fin staff.
6.23.2025.
