package com.lykke.matching.engine.order

enum class OrderStatus {
    //Init status, limit order in order book
    InOrderBook
    //Partially matched
    ,Processing
    //Stop limit order pending
    ,Pending
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
    //Disabled asset
    ,DisabledAsset
    //Cancelled
    ,Cancelled
    //Lead to negative spread
    ,LeadToNegativeSpread
    //Invalid fee
    ,InvalidFee
    //Too small volume
    ,TooSmallVolume
    //Invalid price
    ,InvalidPrice
    // Previous order is not found (by oldUid)
    ,NotFoundPrevious
    // Replaced
    ,Replaced
    ,InvalidPriceAccuracy
    ,InvalidVolumeAccuracy
    ,InvalidVolume
    ,InvalidValue
}