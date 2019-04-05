package com.lykke.matching.engine.logging

import com.lykke.matching.engine.daos.Message
import com.lykke.matching.engine.outgoing.messages.*
import com.lykke.matching.engine.outgoing.messages.v2.events.Event
import java.util.Date

fun toLogMessage(message: Any, stringRepresentation: String): Message {
        val type = message::class.java.simpleName
        return when (message) {
            is BalanceUpdate -> Message(null, message.messageId, message.id, type, message.timestamp, stringRepresentation)
            is CashOperation -> Message(null, message.messageId, message.id, type, message.dateTime, stringRepresentation)
            is CashTransferOperation -> Message(null, message.messageId, message.id, type, message.dateTime, stringRepresentation)
            is LimitOrdersReport -> Message(null, message.messageId, message.messageId, type, Date(), stringRepresentation)
            is MarketOrderWithTrades ->
                Message(null, message.messageId, message.order.id, type, Date(), stringRepresentation)
            is ReservedCashOperation -> Message(null, message.messageId, message.id, type, message.dateTime, stringRepresentation)
            is Event<*> -> {
                val header = message.header
                Message(header.sequenceNumber, header.messageId, header.requestId, header.eventType, header.timestamp, stringRepresentation)
            }
            else -> {
                throw IllegalArgumentException("Unknown message type: ${message::class.java.name}")
            }
        }
    }
