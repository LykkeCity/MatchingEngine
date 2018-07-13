package com.lykke.matching.engine.logging

import com.lykke.matching.engine.daos.Message
import com.lykke.matching.engine.database.MessageLogDatabaseAccessor
import com.lykke.matching.engine.outgoing.messages.BalanceUpdate
import com.lykke.matching.engine.outgoing.messages.CashOperation
import com.lykke.matching.engine.outgoing.messages.CashSwapOperation
import com.lykke.matching.engine.outgoing.messages.CashTransferOperation
import com.lykke.matching.engine.outgoing.messages.LimitOrdersReport
import com.lykke.matching.engine.outgoing.messages.MarketOrderWithTrades
import com.lykke.matching.engine.outgoing.messages.ReservedCashOperation
import com.lykke.matching.engine.outgoing.messages.v2.events.Event
import java.util.Date
import java.util.UUID

class MessageDatabaseLogger(dbAccessor: MessageLogDatabaseAccessor<Message>) : DatabaseLogger<MessageWrapper, Message>(dbAccessor) {

    override fun transformMessage(message: MessageWrapper): Message {
        val baseMessage = message.message
        val type = baseMessage::class.java.simpleName
        return when (baseMessage) {
            is BalanceUpdate -> Message(null, baseMessage.messageId, baseMessage.id, type, baseMessage.timestamp, message.stringValue)
            is CashOperation -> Message(null, baseMessage.messageId, baseMessage.id, type, baseMessage.dateTime, message.stringValue)
            is CashSwapOperation -> Message(null, baseMessage.messageId, baseMessage.id, type, baseMessage.dateTime, message.stringValue)
            is CashTransferOperation -> Message(null, baseMessage.messageId, baseMessage.id, type, baseMessage.dateTime, message.stringValue)
            is LimitOrdersReport -> Message(null, baseMessage.messageId, UUID.randomUUID().toString(), type, Date(), message.stringValue)
            is MarketOrderWithTrades ->
                Message(null, baseMessage.messageId, baseMessage.order.id, type, Date(), message.stringValue)
            is ReservedCashOperation -> Message(null, baseMessage.messageId, baseMessage.id, type, baseMessage.dateTime, message.stringValue)
            is Event<*> -> {
                val header = baseMessage.header
                Message(header.sequenceNumber, header.messageId, header.requestId, header.eventType, header.timestamp, message.stringValue)
            }
            else -> {
                throw IllegalArgumentException("Unknown message type: ${message::class.java.name}")
            }
        }
    }
}