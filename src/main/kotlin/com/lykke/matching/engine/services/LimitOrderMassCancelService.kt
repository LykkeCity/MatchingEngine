package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.context.LimitOrderMassCancelOperationContext
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.order.process.common.CancelRequest
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.util.*

@Service
class LimitOrderMassCancelService(private val genericLimitOrderService: GenericLimitOrderService,
                                  private val genericStopLimitOrderService: GenericStopLimitOrderService,
                                  private val limitOrdersCancelServiceHelper: LimitOrdersCancelServiceHelper) : AbstractService {
    companion object {
        private val LOGGER = LoggerFactory.getLogger(LimitOrderMassCancelService::class.java.name)
    }

    override fun processMessage(messageWrapper: MessageWrapper) {
        val now = Date()
        val context = messageWrapper.context as LimitOrderMassCancelOperationContext
        LOGGER.debug("Got mass limit order cancel request id: ${context.uid}, clientId: ${context.clientId}, assetPairId: ${context.assetPairId}, isBuy: ${context.isBuy}")

        limitOrdersCancelServiceHelper.cancelOrdersAndWriteResponse(CancelRequest(genericLimitOrderService.searchOrders(context.clientId, context.assetPairId, context.isBuy),
                genericStopLimitOrderService.searchOrders(context.clientId, context.assetPairId, context.isBuy),
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
        messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder().setStatus(status.type))
    }
}