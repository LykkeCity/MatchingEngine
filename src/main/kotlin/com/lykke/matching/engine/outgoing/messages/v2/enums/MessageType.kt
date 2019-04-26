package com.lykke.matching.engine.outgoing.messages.v2.enums

enum class MessageType(val id: Int) {
    UNKNOWN_MESSAGE_TYPE(0),
    CASH_IN(1),
    CASH_OUT(2),
    CASH_TRANSFER(3),
    ORDER(4),
    RESERVED_BALANCE_UPDATE(5)
}