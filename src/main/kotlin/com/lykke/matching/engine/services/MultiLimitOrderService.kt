package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.MultiLimitOrder
import com.lykke.matching.engine.daos.order.OrderTimeInForce
import com.lykke.matching.engine.daos.order.LimitOrderType
import com.lykke.matching.engine.fee.listOfLimitOrderFee
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.daos.context.MultilimitOrderContext
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.utils.order.MessageStatusUtils
import com.lykke.matching.engine.daos.v2.LimitOrderFeeInstruction
import com.lykke.matching.engine.deduplication.ProcessedMessage
import com.lykke.matching.engine.holders.PriceDeviationThresholdHolder
import com.lykke.matching.engine.holders.MessageProcessingStatusHolder
import com.lykke.matching.engine.holders.ApplicationSettingsHolder
import com.lykke.matching.engine.holders.MidPriceHolder
import com.lykke.matching.engine.holders.UUIDHolder
import com.lykke.matching.engine.order.transaction.ExecutionContextFactory
import com.lykke.matching.engine.order.process.GenericLimitOrdersProcessor
import com.lykke.matching.engine.order.process.StopOrderBookProcessor
import com.lykke.matching.engine.order.ExecutionDataApplyService
import com.lykke.matching.engine.order.process.PreviousLimitOrdersProcessor
import com.lykke.matching.engine.order.process.ProcessedOrder
import com.lykke.matching.engine.order.transaction.ExecutionContext
import com.lykke.matching.engine.outgoing.messages.LimitOrderWithTrades
import com.lykke.matching.engine.services.utils.MidPriceUtils
import com.lykke.matching.engine.services.utils.MultiOrderFilter
import com.lykke.matching.engine.services.validators.common.OrderValidationUtils
import org.apache.log4j.Logger
import org.springframework.stereotype.Service
import java.util.Date

@Service
class MultiLimitOrderService(private val executionContextFactory: ExecutionContextFactory,
                             private val genericLimitOrdersProcessor: GenericLimitOrdersProcessor,
                             private val stopOrderBookProcessor: StopOrderBookProcessor,
                             private val executionDataApplyService: ExecutionDataApplyService,
                             private val previousLimitOrdersProcessor: PreviousLimitOrdersProcessor,
                             private val balancesHolder: BalancesHolder,
                             private val midPriceHolder: MidPriceHolder,
                             private val uuidHolder: UUIDHolder,
                             private val priceDeviationThresholdHolder: PriceDeviationThresholdHolder) : AbstractService {

    companion object {
        private val LOGGER = Logger.getLogger(MultiLimitOrderService::class.java.name)
        private val CONTROLS_LOGGER = Logger.getLogger("${MultiLimitOrderService::class.java.name}.controls")
    }


    private class OrdersProcessingResult(val executionContext: ExecutionContext, val processedOrders: List<ProcessedOrder>)

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


        val ordersProcessingResult = processOrders(multiLimitOrder, messageWrapper, assetPair, now)
        val executionContext = ordersProcessingResult.executionContext
        val processedOrders = ordersProcessingResult.processedOrders

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

    fun createExecutionContext(messageWrapper: MessageWrapper, context: MultilimitOrderContext, now: Date): ExecutionContext {
        return executionContextFactory.create(messageWrapper.messageId!!,
                messageWrapper.id!!,
                MessageType.MULTI_LIMIT_ORDER,
                messageWrapper.processedMessage,
                mapOf(Pair(context.assetPair.assetPairId, context.assetPair)),
                now,
                LOGGER,
                mapOf(Pair(context.baseAsset!!.assetId, context.baseAsset),
                        Pair(context.quotingAsset!!.assetId, context.quotingAsset)),
                context.inputValidationResultByOrderId ?: emptyMap())

    }

    private fun processOrders(inputMultiLimitOrder: MultiLimitOrder, messageWrapper: MessageWrapper, assetPair: AssetPair, now: Date): OrdersProcessingResult {
        val processingMultiLimitOrder = inputMultiLimitOrder.copy()
        val executionContext = getExecutionContextWithProcessedPrevOrders(messageWrapper, processingMultiLimitOrder, assetPair, now)

        val (lowerMidPriceBound, upperMidPriceBound) = MidPriceUtils.getMidPricesInterval(priceDeviationThresholdHolder.getMidPriceDeviationThreshold(assetPair.assetPairId, executionContext),
                midPriceHolder.getReferenceMidPrice(assetPair, executionContext))

        if (!isOrderBookMidPriceValidBeforeProcessing(executionContext, lowerMidPriceBound, upperMidPriceBound, assetPair)) {
            processOrderBookMidPriceInvalidBeforeProcessing(executionContext, processingMultiLimitOrder, assetPair, upperMidPriceBound, lowerMidPriceBound)
        }

        val processedOrders = genericLimitOrdersProcessor.processOrders(processingMultiLimitOrder.orders, executionContext)

        val midPriceAfterOrderProcessing =
                if (executionContext.orderBooksHolder.isOrderBookChanged()) {
                    executionContext.orderBooksHolder.getOrderBook(assetPair.assetPairId).getMidPrice()
                } else {
                    null
                }

        val midPriceValid = OrderValidationUtils.isMidPriceValid(midPriceAfterOrderProcessing, lowerMidPriceBound, upperMidPriceBound)
        return if (!midPriceValid) {
            return processMidPriceIsInvalidAfterProcessing(
                    messageWrapper,
                    inputMultiLimitOrder,
                    assetPair,
                    lowerMidPriceBound,
                    upperMidPriceBound,
                    midPriceAfterOrderProcessing,
                    now)
        } else {
            processMidPriceIsValidAfterProcessing(executionContext,
                    inputMultiLimitOrder,
                    lowerMidPriceBound,
                    upperMidPriceBound,
                    midPriceAfterOrderProcessing)
            OrdersProcessingResult(executionContext, processedOrders)
        }
    }

    private fun processMidPriceIsValidAfterProcessing(executionContext: ExecutionContext,
                                                      multiLimitOrder: MultiLimitOrder,
                                                      lowerMidPriceBound: BigDecimal?,
                                                      upperMidPriceBound: BigDecimal?,
                                                      midPrice: BigDecimal?) {
        midPrice?.let {
            executionContext.currentTransactionMidPriceHolder.addMidPrice(executionContext.assetPairsById[multiLimitOrder.assetPairId]!!, midPrice, executionContext)
        }
        if (!applicationSettingsHolder.isTrustedClient(multiLimitOrder.clientId)) {
            executionContext.controlsInfo("Multilimit message uid = ${multiLimitOrder.messageUid}, assetPair = ${multiLimitOrder.assetPairId}, mid price control passed, " +
                    "l = ${NumberUtils.roundForPrint(lowerMidPriceBound)}, u = ${NumberUtils.roundForPrint(upperMidPriceBound)}, " +
                    "m = ${NumberUtils.roundForPrint(midPrice)}")
        }
    }

    private fun processMidPriceIsInvalidAfterProcessing(messageWrapper: MessageWrapper,
                                                        multiLimitOrder: MultiLimitOrder,
                                                        assetPair: AssetPair,
                                                        lowerMidPriceBound: BigDecimal?,
                                                        upperMidPriceBound: BigDecimal?,
                                                        midPriceAfterMatching: BigDecimal?,
                                                        now: Date): OrdersProcessingResult {
        val freshExecutionContext = getExecutionContextWithProcessedPrevOrders(messageWrapper, multiLimitOrder, assetPair, now)

        if (!applicationSettingsHolder.isTrustedClient(multiLimitOrder.clientId)) {
            freshExecutionContext.controlsInfo("Multilimit message uid = ${multiLimitOrder.messageUid}, assetPair = ${assetPair.assetPairId}, mid price control failed, " +
                    "l = ${NumberUtils.roundForPrint(lowerMidPriceBound)}, u = ${NumberUtils.roundForPrint(upperMidPriceBound)}, " +
                    "m = $midPriceAfterMatching")
        }

        //discard old execution context as it contains invalid matching data that should be rejected dues to invalid mid price
        return OrdersProcessingResult(freshExecutionContext, rejectOrders(OrderStatus.TooHighMidPriceDeviation, freshExecutionContext, multiLimitOrder, now))
    }

    private fun isOrderBookMidPriceValidBeforeProcessing(executionContext: ExecutionContext,
                                                         lowerMidPriceBound: BigDecimal?,
                                                         upperMidPriceBound: BigDecimal?,
                                                         assetPair: AssetPair): Boolean {
        val orderBookMidPriceBeforeOrdersMatching = executionContext.orderBooksHolder.getOrderBook(assetPair.assetPairId).getMidPrice()
        return OrderValidationUtils.isMidPriceValid(orderBookMidPriceBeforeOrdersMatching, lowerMidPriceBound, upperMidPriceBound)
    }

    private fun processOrderBookMidPriceInvalidBeforeProcessing(executionContext: ExecutionContext,
                                                                multiLimitOrder: MultiLimitOrder,
                                                                assetPair: AssetPair,
                                                                upperMidPriceBound: BigDecimal?,
                                                                lowerMidPriceBound: BigDecimal?) {
        val orderBookMidPriceBeforeOrdersMatching = executionContext.orderBooksHolder.getOrderBook(assetPair.assetPairId).getMidPrice()


        val message = "Multilimit message uid = ${multiLimitOrder.messageUid}, assetPairId = ${assetPair.assetPairId}, " +
                "order book mid price: ${NumberUtils.roundForPrint(orderBookMidPriceBeforeOrdersMatching)} " +
                "already aut of range lowerBound: ${NumberUtils.roundForPrint(lowerMidPriceBound)}), upperBound: ${NumberUtils.roundForPrint(upperMidPriceBound)}"
        executionContext.error(message)
        executionContext.controlsError(message)
    }

    private fun rejectOrders(status: OrderStatus,
                             executionContext: ExecutionContext,
                             multiLimitOrder: MultiLimitOrder,
                             now: Date): List<ProcessedOrder> {
        multiLimitOrder.orders.forEach {
            if (it.status == OrderStatus.InOrderBook.name) {
                it.updateStatus(status, now)
            }
        }

        multiLimitOrder.orders.forEach { addOrderToReportIfNotTrusted(it, executionContext) }

        return multiLimitOrder.orders.map { ProcessedOrder(it, false) }
    }

    private fun getExecutionContextWithProcessedPrevOrders(messageWrapper: MessageWrapper,
                                                           multiLimitOrder: MultiLimitOrder,
                                                           assetPair: AssetPair,
                                                           now: Date): ExecutionContext {
        val freshContext = createExecutionContext(messageWrapper, assetPair, now)

        previousLimitOrdersProcessor.cancelAndReplaceOrders(multiLimitOrder.clientId,
                multiLimitOrder.assetPairId,
                multiLimitOrder.cancelAllPreviousLimitOrders,
                multiLimitOrder.cancelBuySide,
                multiLimitOrder.cancelSellSide,
                multiLimitOrder.buyReplacements,
                multiLimitOrder.sellReplacements,
                freshContext)


        return freshContext
    }

    private fun addOrderToReportIfNotTrusted(order: LimitOrder, executionContext: ExecutionContext) {
        if (!applicationSettingsHolder.isTrustedClient(order.clientId)) {
            executionContext.addClientLimitOrderWithTrades(LimitOrderWithTrades(order))
        }
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