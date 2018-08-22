package com.lykke.matching.engine.logging

import com.google.gson.Gson
import com.lykke.matching.engine.daos.Message
import com.lykke.matching.engine.outgoing.messages.*
import com.lykke.matching.engine.outgoing.messages.v2.events.Event
import org.springframework.stereotype.Component
import java.util.*

@Component
class JsonMessageTransformer(private val gson: Gson): MessageTransformer {
    override fun transform(message: Any): Message {
        val type = message::class.java.simpleName
        return when (message) {
            is BalanceUpdate -> Message(null, message.messageId, message.id, type, message.timestamp, getStringValue(message))
            is CashOperation -> Message(null, message.messageId, message.id, type, message.dateTime, getStringValue(message))
            is CashSwapOperation -> Message(null, message.messageId, message.id, type, message.dateTime, getStringValue(message))
            is CashTransferOperation -> Message(null, message.messageId, message.id, type, message.dateTime, getStringValue(message))
            is LimitOrdersReport -> Message(null, message.messageId, UUID.randomUUID().toString(), type, Date(), getStringValue(message))
            is MarketOrderWithTrades ->
                Message(null, message.messageId, message.order.id, type, Date(), getStringValue(message))
            is ReservedCashOperation -> Message(null, message.messageId, message.id, type, message.dateTime, getStringValue(message))
            is Event<*> -> {
                val header = message.header
                Message(header.sequenceNumber, header.messageId, header.requestId, header.eventType, header.timestamp, getStringValue(message))
            }
            else -> {
                throw IllegalArgumentException("Unknown message type: ${message::class.java.name}")
            }
        }
    }

    private fun getStringValue(message: Any): String {
        return gson.toJson(message)
    }
}