package com.lykke.matching.engine.order.transaction

import com.lykke.matching.engine.balance.WalletOperationsProcessor
import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.LkkTrade
import com.lykke.matching.engine.deduplication.ProcessedMessage
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.outgoing.messages.LimitOrderWithTrades
import com.lykke.matching.engine.outgoing.messages.MarketOrderWithTrades
import com.lykke.matching.engine.services.validators.impl.OrderValidationResult
import org.apache.log4j.Logger
import java.util.Date

class ExecutionContext(val messageId: String,
                       val requestId: String,
                       val messageType: MessageType,
                       val processedMessage: ProcessedMessage?,
                       val assetPairsById: Map<String, AssetPair>,
                       val assetsById: Map<String, Asset>,
                       val preProcessorValidationResultsByOrderId: Map<String, OrderValidationResult>,
                       val walletOperationsProcessor: WalletOperationsProcessor,
                       val orderBooksHolder: CurrentTransactionOrderBooksHolder,
                       val stopOrderBooksHolder: CurrentTransactionStopOrderBooksHolder,
                       val date: Date,
                       val logger: Logger) {

    var tradeIndex: Long = 0

    private val clientLimitOrdersWithTradesByInternalId = LinkedHashMap<String, LimitOrderWithTrades>()
    private val trustedClientLimitOrdersWithTradesByInternalId = LinkedHashMap<String, LimitOrderWithTrades>()
    var marketOrderWithTrades: MarketOrderWithTrades? = null

    val lkkTrades = mutableListOf<LkkTrade>()

    fun addClientLimitOrderWithTrades(limitOrderWithTrades: LimitOrderWithTrades) {
        addToReport(clientLimitOrdersWithTradesByInternalId, limitOrderWithTrades)
        trustedClientLimitOrdersWithTradesByInternalId.remove(limitOrderWithTrades.order.id)
    }

    fun addTrustedClientLimitOrderWithTrades(limitOrderWithTrades: LimitOrderWithTrades) {
        addToReport(trustedClientLimitOrdersWithTradesByInternalId, limitOrderWithTrades)
    }

    fun addClientsLimitOrdersWithTrades(limitOrdersWithTrades: Collection<LimitOrderWithTrades>) {
        limitOrdersWithTrades.forEach {
            addClientLimitOrderWithTrades(it)
        }
    }

    fun addTrustedClientsLimitOrdersWithTrades(limitOrdersWithTrades: Collection<LimitOrderWithTrades>) {
        limitOrdersWithTrades.forEach {
            addTrustedClientLimitOrderWithTrades(it)
        }
    }

    private fun addToReport(limitOrdersWithTradesByInternalId: MutableMap<String, LimitOrderWithTrades>, limitOrderWithTrades: LimitOrderWithTrades) {
        val limitOrderWithAllTrades = if (limitOrdersWithTradesByInternalId.containsKey(limitOrderWithTrades.order.id)) {
            val allTrades = limitOrdersWithTradesByInternalId[limitOrderWithTrades.order.id]!!.trades
            allTrades.addAll(limitOrderWithTrades.trades)
            LimitOrderWithTrades(limitOrderWithTrades.order, allTrades)
        } else {
            limitOrderWithTrades
        }
        limitOrdersWithTradesByInternalId[limitOrderWithTrades.order.id] = limitOrderWithAllTrades
    }

    fun getClientsLimitOrdersWithTrades() = clientLimitOrdersWithTradesByInternalId.values
    fun getTrustedClientsLimitOrdersWithTrades() = trustedClientLimitOrdersWithTradesByInternalId.values

    fun debug(message: String) {
        logger.debug(getLogMessage(message))
    }

    fun info(message: String) {
        logger.info(getLogMessage(message))
    }

    fun error(message: String) {
        logger.error(getLogMessage(message))
    }

    fun error(message: String, t: Throwable) {
        logger.error(getLogMessage(message), t)
    }

    private fun getLogMessage(message: String) = "[$messageId] $message"

    fun apply() {
        walletOperationsProcessor.apply()
        orderBooksHolder.apply(date)
        stopOrderBooksHolder.apply(date)
    }
}