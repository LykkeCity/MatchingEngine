package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.context.SingleLimitOrderContext
import com.lykke.matching.engine.order.transaction.ExecutionContextFactory
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.order.process.GenericLimitOrdersProcessor
import com.lykke.matching.engine.order.process.PreviousLimitOrdersProcessor
import com.lykke.matching.engine.order.process.StopOrderBookProcessor
import com.lykke.matching.engine.order.ExecutionDataApplyService
import com.lykke.matching.engine.utils.PrintUtils
import com.lykke.matching.engine.utils.order.MessageStatusUtils
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.util.*

@Service
class SingleLimitOrderService(private val executionContextFactory: ExecutionContextFactory,
                              private val genericLimitOrdersProcessor: GenericLimitOrdersProcessor,
                              private val stopOrderBookProcessor: StopOrderBookProcessor,
                              private val executionDataApplyService: ExecutionDataApplyService,
                              private val previousLimitOrdersProcessor: PreviousLimitOrdersProcessor) : AbstractService {
    companion object {
        private val LOGGER = LoggerFactory.getLogger(SingleLimitOrderService::class.java.name)
        private val STATS_LOGGER = LoggerFactory.getLogger("${SingleLimitOrderService::class.java.name}.stats")
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
        val executionContext = executionContextFactory.create(context.messageId,
                messageWrapper.id!!,
                MessageType.LIMIT_ORDER,
                mapOf(order.clientId to context.clientAllWallets),
                messageWrapper.processedMessage,
                mapOf(Pair(context.assetPair!!.assetPairId, context.assetPair)),
                now,
                LOGGER,
                mapOf(Pair(context.baseAsset!!.assetId, context.baseAsset),
                        Pair(context.quotingAsset!!.assetId, context.quotingAsset)),
                context.validationResult?.let { mapOf(Pair(order.id, it)) } ?: emptyMap())

        previousLimitOrdersProcessor.cancelAndReplaceOrders(order.clientId,
                order.assetPairId,
                context.isCancelOrders,
                order.isBuySide(),
                !order.isBuySide(),
                emptyMap(),
                emptyMap(),
                executionContext)
        val processedOrder = genericLimitOrdersProcessor.processOrders(listOf(order), executionContext).single()
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

