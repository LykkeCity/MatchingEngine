package com.lykke.matching.engine.order.cancel

import com.lykke.matching.engine.daos.MarketOrder
import com.lykke.matching.engine.deduplication.ProcessedMessage
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.holders.MessageSequenceNumberHolder
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.outgoing.messages.LimitOrderWithTrades
import com.lykke.matching.engine.outgoing.messages.LimitOrdersReport
import com.lykke.matching.engine.outgoing.messages.MarketOrderWithTrades
import com.lykke.matching.engine.outgoing.messages.v2.builders.EventFactory
import com.lykke.matching.engine.services.MessageSender
import java.util.Date
import java.util.concurrent.BlockingQueue

object CancelOrdersUtils {

    @JvmStatic
    fun persistAndSendNotifications(canceller: GenericLimitOrdersCanceller,
                                    operationId: String,
                                    messageId: String,
                                    processedMessage: ProcessedMessage?,
                                    messageType: MessageType,
                                    validateBalances: Boolean,
                                    balancesHolder: BalancesHolder,
                                    messageSequenceNumberHolder: MessageSequenceNumberHolder,
                                    clientLimitOrdersQueue: BlockingQueue<LimitOrdersReport>,
                                    trustedClientsLimitOrdersQueue: BlockingQueue<LimitOrdersReport>,
                                    messageSender: MessageSender,
                                    date: Date) =
            persistAndSendNotifications(canceller,
                    operationId,
                    messageId,
                    processedMessage,
                    messageType,
                    validateBalances,
                    balancesHolder,
                    messageSequenceNumberHolder,
                    clientLimitOrdersQueue,
                    trustedClientsLimitOrdersQueue,
                    messageSender,
                    date,
                    null,
                    null)

    @JvmStatic
    fun persistAndSendNotifications(canceller: GenericLimitOrdersCanceller,
                                    operationId: String,
                                    messageId: String,
                                    processedMessage: ProcessedMessage?,
                                    messageType: MessageType,
                                    validateBalances: Boolean,
                                    balancesHolder: BalancesHolder,
                                    messageSequenceNumberHolder: MessageSequenceNumberHolder,
                                    clientLimitOrdersQueue: BlockingQueue<LimitOrdersReport>,
                                    trustedClientsLimitOrdersQueue: BlockingQueue<LimitOrdersReport>,
                                    messageSender: MessageSender,
                                    date: Date,
                                    marketOrder: MarketOrder?,
                                    rabbitSwapQueue: BlockingQueue<MarketOrderWithTrades>?): Boolean {
        val limitOrdersCancelResult = canceller.processLimitOrders()
        val stopLimitOrdersResult = canceller.processStopLimitOrders()

        val walletProcessor = balancesHolder.createWalletProcessor(null, validateBalances)
        walletProcessor.preProcess(limitOrdersCancelResult.walletOperations)
        walletProcessor.preProcess(stopLimitOrdersResult.walletOperations)

        val limitOrdersWithTrades = mutableListOf<LimitOrderWithTrades>()
        limitOrdersWithTrades.addAll(stopLimitOrdersResult.clientsOrdersWithTrades)
        limitOrdersWithTrades.addAll(limitOrdersCancelResult.clientsOrdersWithTrades)

        val trustedClientsLimitOrdersWithTrades = mutableListOf<LimitOrderWithTrades>()
        trustedClientsLimitOrdersWithTrades.addAll(stopLimitOrdersResult.trustedClientsOrdersWithTrades)
        trustedClientsLimitOrdersWithTrades.addAll(limitOrdersCancelResult.trustedClientsOrdersWithTrades)

        val isTrustedClientEvent = trustedClientsLimitOrdersWithTrades.isNotEmpty()
        val isClientEvent = limitOrdersWithTrades.isNotEmpty() || marketOrder != null

        var sequenceNumber: Long? = null
        var clientsSequenceNumber: Long? = null
        var trustedClientsSequenceNumber: Long? = null
        if (isTrustedClientEvent) {
            trustedClientsSequenceNumber = messageSequenceNumberHolder.getNewValue()
            sequenceNumber = trustedClientsSequenceNumber
        }
        if (isClientEvent) {
            clientsSequenceNumber = messageSequenceNumberHolder.getNewValue()
            sequenceNumber = clientsSequenceNumber
        }

        val persistenceData = canceller.getPersistenceData()
        val updated = walletProcessor.persistBalances(processedMessage,
                persistenceData.limitOrderBooksPersistenceData,
                persistenceData.stopLimitOrderBooksPersistenceData,
                sequenceNumber)
        if (!updated) {
            return false
        }

        walletProcessor.apply().sendNotification(operationId, messageType.name, messageId)
        canceller.apply(messageId, processedMessage, stopLimitOrdersResult, limitOrdersCancelResult)

        if (isTrustedClientEvent) {
            trustedClientsLimitOrdersQueue.put(LimitOrdersReport(messageId, trustedClientsLimitOrdersWithTrades))
            messageSender.sendTrustedClientsMessage(EventFactory.createTrustedClientsExecutionEvent(trustedClientsSequenceNumber!!,
                    messageId,
                    operationId,
                    date,
                    messageType,
                    trustedClientsLimitOrdersWithTrades))
        }

        if (isClientEvent) {
            if (limitOrdersWithTrades.isNotEmpty()) {
                clientLimitOrdersQueue.put(LimitOrdersReport(messageId, limitOrdersWithTrades))
            }
            val marketOrderWithTrades = if (marketOrder != null) MarketOrderWithTrades(messageId, marketOrder) else null
            marketOrderWithTrades?.let { rabbitSwapQueue!!.put(it) }
            messageSender.sendMessage(EventFactory.createExecutionEvent(clientsSequenceNumber!!,
                    messageId,
                    operationId,
                    date,
                    messageType,
                    walletProcessor.getClientBalanceUpdates(),
                    limitOrdersWithTrades,
                    marketOrderWithTrades))
        }

        canceller.checkAndProcessStopOrders(messageId)

        return true
    }
}

