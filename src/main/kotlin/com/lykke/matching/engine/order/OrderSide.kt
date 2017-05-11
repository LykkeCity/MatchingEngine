package com.lykke.matching.engine.order

enum class OrderSide {
    Buy,
    Sell;

    fun oppositeSide(): OrderSide {
        when (this) {
            Buy -> return Sell
            Sell -> return Buy
        }
    }
}