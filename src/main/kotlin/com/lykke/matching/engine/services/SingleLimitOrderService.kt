package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.context.SingleLimitOrderContext
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
import com.lykke.matching.engine.services.utils.MidPriceUtils
import com.lykke.matching.engine.utils.PrintUtils
import com.lykke.matching.engine.utils.order.MessageStatusUtils
import org.apache.log4j.Logger
import org.springframework.stereotype.Service
import java.util.*

@Service
class SingleLimitOrderService(private val executionContextFactory: ExecutionContextFactory,
                              private val genericLimitOrdersProcessor: GenericLimitOrdersProcessor,
                              private val stopOrderBookProcessor: StopOrderBookProcessor,
                              private val executionDataApplyService: ExecutionDataApplyService,
                              private val previousLimitOrdersProcessor: PreviousLimitOrdersProcessor,
                              private val priceDeviationThresholdHolder: PriceDeviationThresholdHolder,
                              private val midPriceHolder: MidPriceHolder) : AbstractService {
    companion object {
        private val LOGGER = Logger.getLogger(SingleLimitOrderService::class.java.name)
        private val STATS_LOGGER = Logger.getLogger("${SingleLimitOrderService::class.java.name}.stats")
        private val CONTROLS_LOGGER = Logger.getLogger("${SingleLimitOrderService::class.java.name}.controls")

    }

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
        val assetPair = context.assetPair

        val order = context.limitOrder.copy()
        val executionContext = createExecutionContext(context, messageWrapper, context.assetPair!!, now, context.baseAsset!!, context.quotingAsset!!, order)

        previousLimitOrdersProcessor.cancelAndReplaceOrders(order.clientId,
                order.assetPairId,
                context.isCancelOrders,
                order.isBuySide(),
                !order.isBuySide(),
                emptyMap(),
                emptyMap(),
                executionContext)

        val processedOrder = genericLimitOrdersProcessor.processOrders(listOf(order), executionContext).single()

        val midPriceValid = MidPriceUtils.isMidPriceValid(priceDeviationThresholdHolder.getMidPriceDeviationThreshold(assetPair!!.assetPairId, executionContext),
                midPriceHolder.getReferenceMidPrice(context.assetPair, executionContext),
                assetPair.assetPairId,
                executionContext)

        return if (!midPriceValid) {
            val freshExecutionContext = createExecutionContext(context, messageWrapper, context.assetPair, now, context.baseAsset, context.quotingAsset, order)
            val inputOrder = context.limitOrder
            inputOrder.updateStatus(OrderStatus.TooHighMidPriceDeviation, now)
            OrderProcessingResult(processInvalidLimitOrder(order, freshExecutionContext, context), ProcessedOrder(inputOrder, false))
        } else {
            OrderProcessingResult(executionContext, processedOrder)
        }
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

    private fun processInvalidLimitOrder(order: LimitOrder, executionContext: ExecutionContext, context: SingleLimitOrderContext): ExecutionContext{
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

    private class OrderProcessingResult (val executionContext: ExecutionContext, val processedOrder: ProcessedOrder)
}

