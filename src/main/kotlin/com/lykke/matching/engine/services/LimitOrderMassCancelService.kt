package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.context.LimitOrderMassCancelOperationContext
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.order.cancel.GenericLimitOrdersCancellerFactory
import org.apache.log4j.Logger
import org.springframework.stereotype.Service
import java.util.Date

@Service
class LimitOrderMassCancelService(private val genericLimitOrderService: GenericLimitOrderService,
                                  private val cancellerFactory: GenericLimitOrdersCancellerFactory) : AbstractService {
    companion object {
        private val LOGGER = Logger.getLogger(LimitOrderMassCancelService::class.java.name)
    }

    override fun processMessage(messageWrapper: MessageWrapper) {
        val now = Date()
        val context = messageWrapper.context as LimitOrderMassCancelOperationContext

        val orders = getOrders(context)

        if (orders.isNotEmpty()) {
            cancelOrders(orders, now, context)
        }

        writeResponse(messageWrapper, MessageStatus.OK)
    }

    private fun getOrders(context: LimitOrderMassCancelOperationContext): List<LimitOrder> {
        val clientId = context.clientId
        val assetPairId = context.assetPairId

        LOGGER.debug("Got mass limit order cancel request id: ${context.uid}, clientId: $clientId, assetPairId: $assetPairId, isBuy: ${context.isBuy}")

        return genericLimitOrderService.searchOrders(clientId, context.assetPairId, context.isBuy)
    }

    private fun cancelOrders(orders: List<LimitOrder>, now: Date,
                             context: LimitOrderMassCancelOperationContext) {
        val canceller = cancellerFactory.create(LOGGER, now)

        canceller.preProcessLimitOrders(orders)
                .applyFull(context.uid,
                        context.messageId,
                        context.processedMessage,
                        context.type,
                        false)
    }

    override fun parseMessage(messageWrapper: MessageWrapper) {
        // do nothing
    }

    override fun writeResponse(messageWrapper: MessageWrapper, status: MessageStatus) {
        messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder().setStatus(status.type))
    }

}