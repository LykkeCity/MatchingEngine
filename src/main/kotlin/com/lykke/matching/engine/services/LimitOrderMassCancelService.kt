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
                                  private val genericStopLimitOrderService: GenericStopLimitOrderService,
                                  private val cancellerFactory: GenericLimitOrdersCancellerFactory) : AbstractService {
    companion object {
        private val LOGGER = Logger.getLogger(LimitOrderMassCancelService::class.java.name)
    }

    override fun processMessage(messageWrapper: MessageWrapper) {
        val now = Date()
        val context = messageWrapper.context as LimitOrderMassCancelOperationContext
        LOGGER.debug("Got mass limit order cancel request id: ${context.uid}, clientId: ${context.clientId}, assetPairId: ${context.assetPairId}, isBuy: ${context.isBuy}")

        LOGGER.debug("Got mass limit order cancel request id: ${context.uid}, clientId: ${context.clientId}, assetPairId: ${context.assetPairId}, isBuy: ${context.isBuy}")
        val orders = getOrders(context)
        val stopOrders = getStopOrders(context)

        if (orders.isNotEmpty() || stopOrders.isNotEmpty()) {
            val result = cancelOrders(orders, stopOrders, now, context)
            messageWrapper.triedToPersist = true
            messageWrapper.persisted = result
        }

        writeResponse(messageWrapper, MessageStatus.OK)
    }

    private fun getOrders(context: LimitOrderMassCancelOperationContext): List<LimitOrder> {
        val clientId = context.clientId
        return genericLimitOrderService.searchOrders(clientId, context.assetPairId, context.isBuy)
    }

    private fun getStopOrders(context: LimitOrderMassCancelOperationContext): List<LimitOrder> {
        val clientId = context.clientId
        return genericStopLimitOrderService.searchOrders(clientId, context.assetPairId, context.isBuy)
    }

    private fun cancelOrders(orders: List<LimitOrder>, stopOrders: List<LimitOrder>, now: Date,
                             context: LimitOrderMassCancelOperationContext): Boolean {
        val canceller = cancellerFactory.create(LOGGER, now)

        return canceller.preProcessLimitOrders(orders)
                .preProcessStopLimitOrders(stopOrders)
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