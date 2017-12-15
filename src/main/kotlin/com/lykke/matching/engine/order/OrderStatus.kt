package com.lykke.matching.engine.order

enum class OrderStatus {
    //Init status, limit order in order book
    InOrderBook
    //Partially matched
    ,Processing
    //Fully matched
    ,Matched
    //Not enough funds on account
    ,NotEnoughFunds
    //Reserved volume greater than balance
    ,ReservedVolumeGreaterThanBalance
    //No liquidity
    ,NoLiquidity
    //Unknown asset
    ,UnknownAsset
    //Cancelled
    ,Cancelled
    //Lead to negative spread
    ,LeadToNegativeSpread
    //Price is outside threshold
    ,PriceIsOutsideThreshold
}