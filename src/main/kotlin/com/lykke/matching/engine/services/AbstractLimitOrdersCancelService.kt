package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.NewLimitOrder
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.order.cancel.GenericLimitOrdersCancellerFactory
import org.apache.log4j.Logger
import java.util.Date

abstract class AbstractLimitOrdersCancelService(protected val genericLimitOrderService: GenericLimitOrderService,
                                                protected val genericStopLimitOrderService: GenericStopLimitOrderService,
                                                private val cancellerFactory: GenericLimitOrdersCancellerFactory) : AbstractService {

    protected companion object {
        private val LOGGER = Logger.getLogger(AbstractLimitOrdersCancelService::class.java.name)

        class Orders private constructor(val orders: List<NewLimitOrder>,
                                         val stopOrders: List<NewLimitOrder>,
                                         val processed: Boolean) {
            companion object Factory {
                fun processed() = Orders(emptyList(), emptyList(), true)
                fun notProcessed(orders: List<NewLimitOrder>, stopOrders: List<NewLimitOrder>) = Orders(orders, stopOrders, false)
            }
        }
    }

    final override fun processMessage(messageWrapper: MessageWrapper) {
        if (messageWrapper.parsedMessage == null) {
            parseMessage(messageWrapper)
        }

        val orders = getOrders(messageWrapper)
        if (orders.processed) {
            return
        }

        val now = Date()
        val operationType = MessageType.valueOf(messageWrapper.type)?.name
                ?: "Unknown message type ${messageWrapper.type}"

        val canceller = cancellerFactory.create(LOGGER, now)
        val operationId = messageWrapper.messageId!!
        val updated = canceller.preProcessLimitOrders(orders.orders)
                .preProcessStopLimitOrders(orders.stopOrders)
                .applyFull(operationId, operationType, false)

        if (!updated) {
            messageWrapper.writeNewResponse(
                    ProtocolMessages.NewResponse.newBuilder()
                            .setId(operationId)
                            .setStatus(MessageStatus.RUNTIME.type)
                            .setStatusReason("Unable to save result")
                            .build())
            LOGGER.info("Unable to save result for operation $operationId")
            return
        }

        writeResponse(messageWrapper, MessageStatus.OK)
    }

    protected abstract fun getOrders(messageWrapper: MessageWrapper): Orders
}