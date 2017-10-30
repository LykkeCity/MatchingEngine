package com.lykke.matching.engine.logging

import com.lykke.matching.engine.daos.azure.AzureMessage
import com.lykke.matching.engine.database.azure.AzureMessageLogDatabaseAccessor
import com.lykke.matching.engine.outgoing.messages.BalanceUpdate
import com.lykke.matching.engine.outgoing.messages.CashOperation
import com.lykke.matching.engine.outgoing.messages.CashSwapOperation
import com.lykke.matching.engine.outgoing.messages.CashTransferOperation
import com.lykke.matching.engine.outgoing.messages.JsonSerializable
import com.lykke.matching.engine.outgoing.messages.LimitOrdersReport
import com.lykke.matching.engine.outgoing.messages.MarketOrderWithTrades
import com.lykke.utils.MAX_AZURE_FIELD_LENGTH
import com.lykke.utils.string.parts
import org.apache.log4j.Logger
import java.util.UUID

class MessageDatabaseLogger(dbAccessor: AzureMessageLogDatabaseAccessor): DatabaseLogger<AzureMessage>(dbAccessor) {

    companion object {
        private val LOGGER = Logger.getLogger(MessageDatabaseLogger::class.java.name)
    }

    override fun transformMessage(message: JsonSerializable): AzureMessage? {
        return when (message) {
            is BalanceUpdate -> AzureMessage(message.id, message.type, parts(message.toJson()))
            is CashOperation -> AzureMessage(message.id, "CashOperation", parts(message.toJson()))
            is CashSwapOperation -> AzureMessage(message.id, "CashSwapOperation", parts(message.toJson()))
            is CashTransferOperation -> AzureMessage(message.id, "CashTransferOperation", parts(message.toJson()))
            is LimitOrdersReport -> AzureMessage(UUID.randomUUID().toString(), "LimitOrdersReport", parts(message.toJson()))
            is MarketOrderWithTrades ->
                AzureMessage(message.order.id, "MarketOrderWithTrades", parts(message.toJson()))
            else -> {
                LOGGER.error("Unknown message type: ${this::class.java.name}")
                null
            }
        }
    }

    private fun parts(value: String): Array<String> {
        return value.parts(MAX_AZURE_FIELD_LENGTH, 6)
    }
}