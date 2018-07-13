package com.lykke.matching.engine.outgoing.messages.v2.enums

enum class OrderType(val id: Int) {
    UNKNOWN_ORDER_TYPE(0),
    MARKET(1),
    LIMIT(2),
    STOP_LIMIT(3)
}