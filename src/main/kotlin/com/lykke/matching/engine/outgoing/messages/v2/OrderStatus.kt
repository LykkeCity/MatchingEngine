package com.lykke.matching.engine.outgoing.messages.v2

enum class OrderStatus(val id: Int) {
    UNKNOWN_STATUS(0),
    PLACED(1),
    PARTIALLY_MATCHED(2),
    MATCHED(3),
    PENDING(4),
    CANCELLED(5),
    REPLACED(6),
    REJECTED(7)
}