package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.context.MultilimitOrderContext
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.utils.order.MessageStatusUtils
import com.lykke.matching.engine.order.transaction.ExecutionContextFactory
import com.lykke.matching.engine.order.process.GenericLimitOrdersProcessor
import com.lykke.matching.engine.order.process.StopOrderBookProcessor
import com.lykke.matching.engine.order.ExecutionDataApplyService
import com.lykke.matching.engine.order.process.PreviousLimitOrdersProcessor
import com.lykke.matching.engine.services.utils.MultiOrderFilter
import org.apache.log4j.Logger
import org.springframework.stereotype.Service
import java.util.Date

@Service
class MultiLimitOrderService(private val executionContextFactory: ExecutionContextFactory,
                             private val genericLimitOrdersProcessor: GenericLimitOrdersProcessor,
                             private val stopOrderBookProcessor: StopOrderBookProcessor,
                             private val executionDataApplyService: ExecutionDataApplyService,
                             private val previousLimitOrdersProcessor: PreviousLimitOrdersProcessor,
                             private val balancesHolder: BalancesHolder) : AbstractService {

    companion object {
        private val LOGGER = Logger.getLogger(MultiLimitOrderService::class.java.name)
    }

    override fun processMessage(messageWrapper: MessageWrapper) {
        if (messageWrapper.parsedMessage == null) {
            parseMessage(messageWrapper)
        }
        processMultiOrder(messageWrapper)
    }

    private fun processMultiOrder(messageWrapper: MessageWrapper) {
        val context = messageWrapper.context as MultilimitOrderContext

        val now = Date()
        val ordersToProcess = getOrdersToProcess(context, context.assetPair!!, now)

        val multiLimitOrder = context.multiLimitOrder

        val executionContext = executionContextFactory.create(messageWrapper.messageId!!,
                messageWrapper.id!!,
                MessageType.MULTI_LIMIT_ORDER,
                messageWrapper.processedMessage,
                mapOf(Pair(context.assetPair.assetPairId, context.assetPair)),
                now,
                LOGGER,
                mapOf(Pair(context.baseAsset!!.assetId, context.baseAsset),
                Pair(context.quotingAsset!!.assetId, context.quotingAsset)),
                context.inputValidationResultByOrderId ?: emptyMap())

        previousLimitOrdersProcessor.cancelAndReplaceOrders(multiLimitOrder.clientId,
                multiLimitOrder.assetPairId,
                multiLimitOrder.cancelAllPreviousLimitOrders,
                multiLimitOrder.cancelBuySide,
                multiLimitOrder.cancelSellSide,
                multiLimitOrder.buyReplacements,
                multiLimitOrder.sellReplacements,
                executionContext)

        val processedOrders = genericLimitOrdersProcessor.processOrders(ordersToProcess, executionContext)
        stopOrderBookProcessor.checkAndExecuteStopLimitOrders(executionContext)
        val persisted = executionDataApplyService.persistAndSendEvents(messageWrapper, executionContext)

        val responseBuilder = ProtocolMessages.MultiLimitOrderResponse.newBuilder()
        if (!persisted) {
            val errorMessage = "Unable to save result data"
            LOGGER.error("$errorMessage (multi limit order id ${multiLimitOrder.messageUid})")

            messageWrapper.writeMultiLimitOrderResponse(responseBuilder
                    .setStatus(MessageStatus.RUNTIME.type)
                    .setAssetPairId(multiLimitOrder.assetPairId)
                    .setStatusReason(errorMessage))

            return
        }

        responseBuilder.setId(multiLimitOrder.messageUid)
                .setStatus(MessageStatus.OK.type).assetPairId = multiLimitOrder.assetPairId

        processedOrders.forEach { processedOrder ->
            val order = processedOrder.order
            val statusBuilder = ProtocolMessages.MultiLimitOrderResponse.OrderStatus.newBuilder()
                    .setId(order.externalId)
                    .setMatchingEngineId(order.id)
                    .setStatus(MessageStatusUtils.toMessageStatus(order.status).type)
                    .setVolume(order.volume.toDouble())
                    .setPrice(order.price.toDouble())
            processedOrder.reason?.let { statusBuilder.statusReason = processedOrder.reason }
            responseBuilder.addStatuses(statusBuilder)
        }
        writeResponse(messageWrapper, responseBuilder)

    }

    fun getOrdersToProcess(context: MultilimitOrderContext, assetPair: AssetPair, now: Date): List<LimitOrder> {
        val baseAssetAvailableBalance = balancesHolder.getAvailableBalance(context.clientId, context.assetPair!!.baseAssetId)
        val quotingAssetAvailableBalance = balancesHolder.getAvailableBalance(context.clientId, assetPair.quotingAssetId)

        val filter = MultiOrderFilter(context.isTrustedClient,
                baseAssetAvailableBalance,
                quotingAssetAvailableBalance,
                context.quotingAsset!!.accuracy,
                now,
                context.multiLimitOrder.orders.size,
                LOGGER)

        for (order in context.multiLimitOrder.orders) {
            filter.checkAndAdd(order)
        }

        return filter.getResult()
    }


    override fun parseMessage(messageWrapper: MessageWrapper) {
        //nothing to do
    }

    fun writeResponse(messageWrapper: MessageWrapper, responseBuilder: ProtocolMessages.MultiLimitOrderResponse.Builder) {
        messageWrapper.writeMultiLimitOrderResponse(responseBuilder)
    }

    override fun writeResponse(messageWrapper: MessageWrapper, status: MessageStatus) {
        val assetPairId = (messageWrapper.parsedMessage as ProtocolMessages.MultiLimitOrder).assetPairId
        messageWrapper.writeMultiLimitOrderResponse(ProtocolMessages.MultiLimitOrderResponse.newBuilder()
                .setStatus(status.type).setAssetPairId(assetPairId))
    }
}