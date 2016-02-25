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
    //No liquidity
    ,NoLiquidity
}