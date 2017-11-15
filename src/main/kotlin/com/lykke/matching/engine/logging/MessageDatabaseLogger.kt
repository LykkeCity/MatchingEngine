package com.lykke.matching.engine.logging

import com.lykke.matching.engine.daos.Message
import com.lykke.matching.engine.database.MessageLogDatabaseAccessor
import com.lykke.matching.engine.outgoing.messages.BalanceUpdate
import com.lykke.matching.engine.outgoing.messages.CashOperation
import com.lykke.matching.engine.outgoing.messages.CashSwapOperation
import com.lykke.matching.engine.outgoing.messages.CashTransferOperation
import com.lykke.matching.engine.outgoing.messages.JsonSerializable
import com.lykke.matching.engine.outgoing.messages.LimitOrdersReport
import com.lykke.matching.engine.outgoing.messages.MarketOrderWithTrades
import java.util.Date
import java.util.UUID

class MessageDatabaseLogger(dbAccessor: MessageLogDatabaseAccessor<Message>): DatabaseLogger<JsonSerializable, Message>(dbAccessor) {

    override fun transformMessage(message: JsonSerializable): Message = when (message) {
        is BalanceUpdate -> Message(message.id, message.timestamp, message.toJson())
        is CashOperation -> Message(message.id, message.dateTime, message.toJson())
        is CashSwapOperation -> Message(message.id, message.dateTime, message.toJson())
        is CashTransferOperation -> Message(message.id, message.dateTime, message.toJson())
        is LimitOrdersReport -> Message(UUID.randomUUID().toString(), Date(), message.toJson())
        is MarketOrderWithTrades ->
            Message(message.order.id, Date(), message.toJson())
        else -> {
            throw IllegalArgumentException("Unknown message type: ${this::class.java.name}")
        }
    }
}