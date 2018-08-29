package com.lykke.matching.engine.outgoing.messages.v2.enums

enum class OrderTimeInForce(val id: Int) {
    UNKNOWN_ORDER_TIME_IN_FORCE(0),
    GTC(1),
    GTD(2),
    IOC(3),
    FOK(4)
}