package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.context.LimitOrderMassCancelOperationContext
import com.lykke.matching.engine.daos.order.LimitOrderType
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.order.cancel.GenericLimitOrdersCancellerFactory
import org.apache.log4j.Logger
import org.springframework.stereotype.Service
import org.springframework.util.CollectionUtils
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
        val orders = getOrders(context)

        if (orders.isNotEmpty()) {
            val result = cancelOrders(orders, now, context)
            messageWrapper.triedToPersist = true
            messageWrapper.persisted = result
        }

        writeResponse(messageWrapper, MessageStatus.OK)
    }

    private fun getOrders(context: LimitOrderMassCancelOperationContext): Map<LimitOrderType, List<LimitOrder>> {
        val clientId = context.clientId
        val assetPairId = context.assetPairId

        return mapOf(LimitOrderType.LIMIT to genericLimitOrderService.searchOrders(clientId, context.assetPairId, context.isBuy),
                LimitOrderType.STOP_LIMIT to genericStopLimitOrderService.searchOrders(clientId, assetPairId, context.isBuy))
    }

    private fun cancelOrders(typeToOrders: Map<LimitOrderType, List<LimitOrder>>,
                             now: Date,
                             context: LimitOrderMassCancelOperationContext): Boolean {
        if (typeToOrders.isEmpty()) {
            return true
        }

        val canceller = cancellerFactory.create(LOGGER, now)

        val limitOrders = typeToOrders[LimitOrderType.LIMIT]
        val stopOrders = typeToOrders[LimitOrderType.STOP_LIMIT]

        canceller.preProcessLimitOrders(limitOrders ?: emptyList())
        canceller.preProcessStopLimitOrders(stopOrders ?: emptyList())

        return canceller.applyFull(context.uid,
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