package com.lykke.matching.engine.order

import java.util.HashMap

enum class OrderSide (val side: Int) {
    Buy(1),
    Sell(2);

    fun oppositeSide(): OrderSide {
        when (this) {
            Buy -> return Sell
            Sell -> return Buy
        }
    }

    companion object {
        val sidesMap = HashMap<Int, OrderSide>()

        init {
            values().forEach {
                sidesMap.put(it.side, it)
            }
        }

        fun valueOf(type: Int): OrderSide? {
            return sidesMap[type]
        }
    }
}