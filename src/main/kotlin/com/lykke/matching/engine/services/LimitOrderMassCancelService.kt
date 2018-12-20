package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.context.LimitOrderMassCancelOperationContext
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.order.process.common.CancelRequest
import com.lykke.matching.engine.performance.PerformanceStatsHolder
import org.apache.log4j.Logger
import org.springframework.stereotype.Service
import java.util.*

@Service
class LimitOrderMassCancelService(private val genericLimitOrderService: GenericLimitOrderService,
                                  private val genericStopLimitOrderService: GenericStopLimitOrderService,
                                  private val limitOrdersCancelServiceHelper: LimitOrdersCancelServiceHelper,
                                  private val performanceStatsHolder: PerformanceStatsHolder) : AbstractService {
    companion object {
        private val LOGGER = Logger.getLogger(LimitOrderMassCancelService::class.java.name)
    }

    override fun processMessage(messageWrapper: MessageWrapper) {
        val now = Date()
        val context = messageWrapper.context as LimitOrderMassCancelOperationContext
        LOGGER.debug("Got mass limit order cancel request id: ${context.uid}, clientId: ${context.clientId}, assetPairId: ${context.assetPairId}, isBuy: ${context.isBuy}")

        limitOrdersCancelServiceHelper.cancelOrdersAndWriteResponse(CancelRequest(genericLimitOrderService.searchOrders(context.clientId, context.assetPairId, context.isBuy),
                genericStopLimitOrderService.searchOrders(context.clientId, context.assetPairId, context.isBuy),
                context.assetPairId,
                context.messageId,
                context.uid,
                context.messageType,
                now,
                context.processedMessage,
                messageWrapper,
                LOGGER))
    }

    override fun parseMessage(messageWrapper: MessageWrapper) {
        // do nothing
    }

    override fun writeResponse(messageWrapper: MessageWrapper, status: MessageStatus) {
        val start = System.nanoTime()
        messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder().setStatus(status.type))
        val end = System.nanoTime()
        performanceStatsHolder.addWriteResponseTime(MessageType.LIMIT_ORDER_MASS_CANCEL.type, end - start)
    }
}