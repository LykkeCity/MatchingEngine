package com.lykke.matching.engine.outgoing.messages.v2

enum class OrderSide(val id: Int) {
    UNKNOWN_ORDER_SIDE(0),
    BUY(1),
    SELL(2)
}