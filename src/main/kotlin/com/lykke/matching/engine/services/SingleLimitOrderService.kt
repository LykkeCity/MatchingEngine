package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.context.SingleLimitOrderContext
import com.lykke.matching.engine.holders.ApplicationSettingsHolder
import com.lykke.matching.engine.holders.MidPriceHolder
import com.lykke.matching.engine.holders.PriceDeviationThresholdHolder
import com.lykke.matching.engine.order.transaction.ExecutionContextFactory
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.order.process.GenericLimitOrdersProcessor
import com.lykke.matching.engine.order.process.PreviousLimitOrdersProcessor
import com.lykke.matching.engine.order.process.StopOrderBookProcessor
import com.lykke.matching.engine.order.ExecutionDataApplyService
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.order.process.ProcessedOrder
import com.lykke.matching.engine.order.transaction.ExecutionContext
import com.lykke.matching.engine.outgoing.messages.LimitOrderWithTrades
import com.lykke.matching.engine.services.utils.MidPriceUtils
import com.lykke.matching.engine.services.validators.common.OrderValidationUtils
import com.lykke.matching.engine.utils.NumberUtils
import com.lykke.matching.engine.utils.PrintUtils
import com.lykke.matching.engine.utils.order.MessageStatusUtils
import org.apache.log4j.Logger
import org.springframework.stereotype.Service
import java.math.BigDecimal
import java.util.*

@Service
class SingleLimitOrderService(private val executionContextFactory: ExecutionContextFactory,
                              private val genericLimitOrdersProcessor: GenericLimitOrdersProcessor,
                              private val stopOrderBookProcessor: StopOrderBookProcessor,
                              private val executionDataApplyService: ExecutionDataApplyService,
                              private val previousLimitOrdersProcessor: PreviousLimitOrdersProcessor,
                              private val priceDeviationThresholdHolder: PriceDeviationThresholdHolder,
                              private val midPriceHolder: MidPriceHolder,
                              private val applicationSettingsHolder: ApplicationSettingsHolder) : AbstractService {
    companion object {
        private val LOGGER = Logger.getLogger(SingleLimitOrderService::class.java.name)
        private val STATS_LOGGER = Logger.getLogger("${SingleLimitOrderService::class.java.name}.stats")
        private val CONTROLS_LOGGER = Logger.getLogger("${SingleLimitOrderService::class.java.name}.controls")
    }

    private class OrderProcessingResult(val executionContext: ExecutionContext, val processedOrder: ProcessedOrder)

    private var messagesCount: Long = 0

    private var logCount = 100
    private var totalTime: Double = 0.0

    override fun processMessage(messageWrapper: MessageWrapper) {
        val context = messageWrapper.context as SingleLimitOrderContext

        val now = Date()
        LOGGER.info("Got limit order: $context")

        val order = context.limitOrder
        order.register(now)

        val startTime = System.nanoTime()

        val orderProcessingResult = processOrder(context, messageWrapper, now)
        val executionContext = orderProcessingResult.executionContext
        val processedOrder = orderProcessingResult.processedOrder

        stopOrderBookProcessor.checkAndExecuteStopLimitOrders(executionContext)

        val persisted = executionDataApplyService.persistAndSendEvents(messageWrapper, executionContext)

        if (!persisted) {
            val message = "Unable to save result data"
            LOGGER.error("$message (order external id: ${order.externalId})")
            writeResponse(messageWrapper,
                    MessageStatus.RUNTIME,
                    processedOrder.order.id,
                    message)
            return
        }

        if (processedOrder.accepted) {
            writeResponse(messageWrapper, MessageStatus.OK, processedOrder.order.id)
        } else {
            writeResponse(messageWrapper,
                    MessageStatusUtils.toMessageStatus(processedOrder.order.status),
                    processedOrder.order.id,
                    processedOrder.reason)
        }

        val endTime = System.nanoTime()

        messagesCount++
        totalTime += (endTime - startTime).toDouble() / logCount

        if (messagesCount % logCount == 0L) {
            STATS_LOGGER.info("Total: ${PrintUtils.convertToString(totalTime)}. ")
            totalTime = 0.0
        }
    }

    private fun processOrder(context: SingleLimitOrderContext, messageWrapper: MessageWrapper, now: Date): OrderProcessingResult {
        val assetPair = context.assetPair!!

        val processingOrder = context.limitOrder.copy()
        val executionContext = getExecutionContextWithProcessedPrevOrders(context, messageWrapper, now)

        val (lowerMidPriceBound, upperMidPriceBound) = MidPriceUtils.getMidPricesInterval(priceDeviationThresholdHolder.getMidPriceDeviationThreshold(assetPair.assetPairId, executionContext),
                midPriceHolder.getReferenceMidPrice(assetPair, executionContext))

        if (!isOrderBookMidPriceValidBeforeProcessing(executionContext, lowerMidPriceBound, upperMidPriceBound, assetPair)) {
            processOrderBookMidPriceInvalidBeforeProcessing(executionContext,
                    processingOrder,
                    lowerMidPriceBound,
                    upperMidPriceBound,
                    assetPair)
        }

        val processedOrder = genericLimitOrdersProcessor.processOrders(listOf(processingOrder), executionContext).single()

        val midPriceAfterOrderProcessing = if (executionContext.orderBooksHolder.isOrderBookChanged()) {
            executionContext.orderBooksHolder.getChangedCopyOrOriginalOrderBook(assetPair.assetPairId).getMidPrice()
        } else {
            null
        }

        val midPriceValid = OrderValidationUtils.isMidPriceValid(midPriceAfterOrderProcessing, lowerMidPriceBound, upperMidPriceBound)

        return if (!midPriceValid) {
            return processMidPriceIsInvalidAfterProcessing(context,
                    messageWrapper,
                    lowerMidPriceBound,
                    upperMidPriceBound,
                    midPriceAfterOrderProcessing,
                    now)
        } else {
            processMidPriceValidAfterProcessing(executionContext,
                    context.limitOrder,
                    assetPair.assetPairId,
                    lowerMidPriceBound,
                    upperMidPriceBound,
                    midPriceAfterOrderProcessing)
            OrderProcessingResult(executionContext, processedOrder)
        }
    }

    private fun processMidPriceValidAfterProcessing(executionContext: ExecutionContext,
                                                    limitOrder: LimitOrder,
                                                    assetPairId: String,
                                                    lowerMidPriceBound: BigDecimal?,
                                                    upperMidPriceBound: BigDecimal?,
                                                    midPrice: BigDecimal?) {
        midPrice?.let {
            executionContext.currentTransactionMidPriceHolder.addMidPrice(assetPairId, midPrice, executionContext)
        }
        if (!applicationSettingsHolder.isTrustedClient(limitOrder.clientId)) {
            executionContext.controlsInfo("Limit order externalId = ${limitOrder.externalId}, assetPair = ${assetPairId}, mid price control passed, " +
                    "l = ${NumberUtils.roundForPrint(lowerMidPriceBound)}, u = ${NumberUtils.roundForPrint(upperMidPriceBound)}, " +
                    "m = $midPrice")
        }
    }

    private fun processMidPriceIsInvalidAfterProcessing(context: SingleLimitOrderContext,
                                                        messageWrapper: MessageWrapper,
                                                        lowerMidPriceBound: BigDecimal?,
                                                        upperMidPriceBound: BigDecimal?,
                                                        midPrice: BigDecimal?,
                                                        now: Date): OrderProcessingResult {
        val freshExecutionContext = getExecutionContextWithProcessedPrevOrders(context, messageWrapper, now)
        val order = context.limitOrder
        if (!applicationSettingsHolder.isTrustedClient(order.clientId)) {
            freshExecutionContext.controlsInfo("Limit order externalId = ${order.externalId}, assetPair = ${context.assetPair!!.assetPairId}, mid price control failed, " +
                    "l = ${NumberUtils.roundForPrint(lowerMidPriceBound)}, u = ${NumberUtils.roundForPrint(upperMidPriceBound)}, " +
                    "m = $midPrice")
        }

        return OrderProcessingResult(freshExecutionContext, rejectOrder(OrderStatus.TooHighMidPriceDeviation, freshExecutionContext, order, now))
    }

    private fun rejectOrder(status: OrderStatus,
                            executionContext: ExecutionContext,
                            limitOrder: LimitOrder,
                            now: Date): ProcessedOrder {
        if (limitOrder.status == OrderStatus.InOrderBook.name) {
            limitOrder.updateStatus(status, now)
        }

        if (!applicationSettingsHolder.isTrustedClient(limitOrder.clientId)) {
            executionContext.addClientLimitOrderWithTrades(LimitOrderWithTrades(limitOrder))
        }
        return ProcessedOrder(limitOrder, false)
    }

    private fun processOrderBookMidPriceInvalidBeforeProcessing(executionContext: ExecutionContext,
                                                                limitOrder: LimitOrder,
                                                                lowerMidPriceBound: BigDecimal?,
                                                                upperMidPriceBound: BigDecimal?,
                                                                assetPair: AssetPair) {
        val midPrice = executionContext.orderBooksHolder.getChangedCopyOrOriginalOrderBook(assetPair.assetPairId).getMidPrice()
        val message = "Limit order externalId = ${limitOrder.externalId}, assetPairId = ${assetPair.assetPairId}, " +
                "order book mid price: ${NumberUtils.roundForPrint(midPrice)} " +
                "already aut of range lowerBound: ${NumberUtils.roundForPrint(lowerMidPriceBound)}), upperBound: ${NumberUtils.roundForPrint(upperMidPriceBound)}"
        executionContext.error(message)
        executionContext.controlsError(message)
    }

    private fun isOrderBookMidPriceValidBeforeProcessing(executionContext: ExecutionContext,
                                                         lowerMidPriceBound: BigDecimal?,
                                                         upperMidPriceBound: BigDecimal?,
                                                         assetPair: AssetPair): Boolean {
        val midPriceOrderBook = executionContext.orderBooksHolder.getChangedCopyOrOriginalOrderBook(assetPair.assetPairId).getMidPrice()
        return OrderValidationUtils.isMidPriceValid(midPriceOrderBook, lowerMidPriceBound, upperMidPriceBound)
    }

    private fun getExecutionContextWithProcessedPrevOrders(context: SingleLimitOrderContext,
                                                           messageWrapper: MessageWrapper,
                                                           now: Date): ExecutionContext {
        val order = context.limitOrder
        val executionContext = createExecutionContext(context, messageWrapper, context.assetPair!!, now, context.baseAsset!!, context.quotingAsset!!, order)

        previousLimitOrdersProcessor.cancelAndReplaceOrders(order.clientId,
                order.assetPairId,
                context.isCancelOrders,
                order.isBuySide(),
                !order.isBuySide(),
                emptyMap(),
                emptyMap(),
                executionContext)

        return executionContext
    }

    private fun createExecutionContext(context: SingleLimitOrderContext, messageWrapper: MessageWrapper, assetPair: AssetPair, now: Date, baseAsset: Asset, quotingAsset: Asset, order: LimitOrder): ExecutionContext {
        return executionContextFactory.create(context.messageId,
                messageWrapper.id!!,
                MessageType.LIMIT_ORDER,
                messageWrapper.processedMessage,
                mapOf(Pair(context.assetPair!!.assetPairId, assetPair)),
                now,
                LOGGER,
                CONTROLS_LOGGER,
                mapOf(Pair(context.baseAsset!!.assetId, baseAsset),
                        Pair(context.quotingAsset!!.assetId, quotingAsset)),
                context.validationResult?.let { mapOf(Pair(order.id, it)) } ?: emptyMap())
    }

    override fun parseMessage(messageWrapper: MessageWrapper) {
        //do nothing
    }

    override fun writeResponse(messageWrapper: MessageWrapper, status: MessageStatus) {
        writeResponse(messageWrapper, status, null)
    }


    private fun writeResponse(messageWrapper: MessageWrapper,
                              status: MessageStatus,
                              internalOrderId: String?,
                              statusReason: String? = null) {
        val builder = ProtocolMessages.NewResponse.newBuilder().setStatus(status.type)
        internalOrderId?.let { builder.setMatchingEngineId(internalOrderId) }
        statusReason?.let { builder.setStatusReason(it) }
        messageWrapper.writeNewResponse(builder)
    }

}

