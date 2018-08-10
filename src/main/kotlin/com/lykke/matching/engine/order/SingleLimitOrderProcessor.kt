package com.lykke.matching.engine.order

import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.context.SingleLimitOrderContext
import com.lykke.matching.engine.matching.MatchingEngine
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.order.process.LimitOrdersProcessorFactory
import com.lykke.matching.engine.outgoing.messages.LimitOrderWithTrades
import com.lykke.matching.engine.services.GenericLimitOrderService
import com.lykke.matching.engine.utils.order.MessageStatusUtils
import org.apache.log4j.Logger
import java.math.BigDecimal
import java.util.*

class SingleLimitOrderProcessor(private val limitOrderService: GenericLimitOrderService,
                                private val limitOrdersProcessorFactory: LimitOrdersProcessorFactory,
                                private val matchingEngine: MatchingEngine,
                                private val LOGGER: Logger) {

    fun processLimitOrder(singleLimitContext: SingleLimitOrderContext,
                          now: Date,
                          payBackReserved: BigDecimal? = null,
                          messageWrapper: MessageWrapper? = null) {
        val order = singleLimitContext.limitOrder
        val assetPair = singleLimitContext.assetPair
        val limitAsset = if (order.isBuySide()) assetPair.quotingAssetId else assetPair.baseAssetId
        val orderBook = limitOrderService.getOrderBook(order.assetPairId).copy()
        val clientsLimitOrdersWithTrades = mutableListOf<LimitOrderWithTrades>()
        var buySideOrderBookChanged = false
        var sellSideOrderBookChanged = false
        var cancelVolume = BigDecimal.ZERO
        val ordersToCancel = mutableListOf<LimitOrder>()

        if (singleLimitContext.isCancelOrders) {
            limitOrderService.searchOrders(order.clientId, order.assetPairId, order.isBuySide()).forEach { orderToCancel ->
                ordersToCancel.add(orderToCancel)
                orderBook.removeOrder(orderToCancel)
                clientsLimitOrdersWithTrades.add(LimitOrderWithTrades(orderToCancel))
                val cancelVol = if (orderToCancel.isBuySide()) orderToCancel.remainingVolume * orderToCancel.price else orderToCancel.getAbsRemainingVolume()
                cancelVolume = cancelVolume.plus(cancelVol)
                buySideOrderBookChanged = buySideOrderBookChanged || order.isBuySide()
                sellSideOrderBookChanged = sellSideOrderBookChanged || !order.isBuySide()
            }
        }
        val totalPayBackReserved = cancelVolume + (payBackReserved ?: BigDecimal.ZERO)

        val processor = limitOrdersProcessorFactory.create(matchingEngine,
                now,
                singleLimitContext.isTrustedClient,
                order.clientId,
                assetPair,
                singleLimitContext.baseAsset,
                singleLimitContext.quotingAsset,
                orderBook,
                if (limitAsset == assetPair.baseAssetId) totalPayBackReserved else BigDecimal.ZERO,
                if (limitAsset == assetPair.quotingAssetId) totalPayBackReserved else BigDecimal.ZERO,
                ordersToCancel,
                clientsLimitOrdersWithTrades,
                emptyList(),
                LOGGER)

        matchingEngine.initTransaction()
        val result = processor.preProcess(singleLimitContext.messageId, listOf(order))
                .apply(singleLimitContext.messageId, singleLimitContext.processedMessage, order.externalId, MessageType.LIMIT_ORDER, buySideOrderBookChanged, sellSideOrderBookChanged)
        messageWrapper?.processedMessagePersisted = true
        if (!result.success) {
            val message = "Unable to save result data"
            LOGGER.error("$message (order external id: ${order.externalId})")
            writeResponse(messageWrapper, order, MessageStatus.RUNTIME, message)
            return
        }

        if (result.orders.size != 1) {
            throw Exception("Error during limit order process (id: ${order.externalId}): result has invalid orders count: ${result.orders.size}")
        }
        val processedOrder = result.orders.first()
        if (processedOrder.accepted) {
            writeResponse(messageWrapper, order, MessageStatus.OK)
        } else {
            writeResponse(messageWrapper, processedOrder.order, MessageStatusUtils.toMessageStatus(processedOrder.order.status), processedOrder.reason)
        }
    }

    private fun writeResponse(messageWrapper: MessageWrapper?, order: LimitOrder, status: MessageStatus, reason: String? = null) {
        if (messageWrapper == null) {
            return
        }
        if (messageWrapper.type == MessageType.OLD_LIMIT_ORDER.type) {
            messageWrapper.writeResponse(ProtocolMessages.Response.newBuilder().setUid(order.externalId.toLong()))
        } else {
            if (reason == null) {
                messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder().setMatchingEngineId(order.id).setStatus(status.type))
            } else {
                messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder().setMatchingEngineId(order.id).setStatus(status.type).setStatusReason(reason))
            }
        }
    }
}