package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.context.SingleLimitOrderContext
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.order.GenericLimitOrderProcessorFactory
import com.lykke.matching.engine.utils.PrintUtils
import org.apache.log4j.Logger
import java.util.*

class SingleLimitOrderService(genericLimitOrderProcessorFactory: GenericLimitOrderProcessorFactory): AbstractService {
    companion object {
        private val LOGGER = Logger.getLogger(SingleLimitOrderService::class.java.name)
        private val STATS_LOGGER = Logger.getLogger("${SingleLimitOrderService::class.java.name}.stats")
    }

    private var messagesCount: Long = 0

    private var logCount = 100
    private var totalTime: Double = 0.0
    private val genericLimitOrderProcessor = genericLimitOrderProcessorFactory.create(LOGGER)

    override fun processMessage(messageWrapper: MessageWrapper) {
        val context = messageWrapper.context as SingleLimitOrderContext

        val now = Date()
        LOGGER.info("Got limit order: $context")

        context.limitOrder = LimitOrder(context.limitOrder, now, now)

        val startTime = System.nanoTime()
        genericLimitOrderProcessor.processOrder(messageWrapper, context, now)
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
        if (messageWrapper.type == MessageType.OLD_LIMIT_ORDER.type) {
            messageWrapper.writeResponse(ProtocolMessages.Response.newBuilder())
        } else {
            messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder()
                    .setStatus(status.type))
        }
    }
}

