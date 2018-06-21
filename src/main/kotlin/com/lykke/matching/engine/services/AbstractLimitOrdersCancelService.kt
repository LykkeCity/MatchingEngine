package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.LimitOrder
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

        class Orders private constructor(val orders: List<LimitOrder>,
                                         val stopOrders: List<LimitOrder>,
                                         val processed: Boolean) {
            companion object Factory {
                fun processed() = Orders(emptyList(), emptyList(), true)
                fun notProcessed(orders: List<LimitOrder>, stopOrders: List<LimitOrder>) = Orders(orders, stopOrders, false)
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
        val operationId = messageWrapper.id!!
        val messageId = messageWrapper.messageId!!
        val updated = canceller.preProcessLimitOrders(orders.orders)
                .preProcessStopLimitOrders(orders.stopOrders)
                .applyFull(operationId,
                        messageId,
                        messageWrapper.processedMessage(),
                        operationType,
                        false)
        messageWrapper.processedMessagePersisted = true
        if (!updated) {
            val errorMessage = "Unable to save result"
            messageWrapper.writeNewResponse(
                    ProtocolMessages.NewResponse.newBuilder()
                            .setStatus(MessageStatus.RUNTIME.type)
                            .setStatusReason(errorMessage))
            LOGGER.info("$errorMessage for operation $operationId")
            return
        }

        writeResponse(messageWrapper, MessageStatus.OK)
    }

    protected abstract fun getOrders(messageWrapper: MessageWrapper): Orders
}