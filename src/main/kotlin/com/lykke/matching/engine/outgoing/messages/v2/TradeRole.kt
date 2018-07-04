package com.lykke.matching.engine.outgoing.messages.v2

enum class TradeRole(val id: Int) {
    UNKNOWN_ROLE(0),
    MAKER(1),
    TAKER(2)
}