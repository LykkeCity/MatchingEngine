package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.NewLimitOrder
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.order.cancel.LimitOrdersCancellerFactory
import java.util.Date
import java.util.UUID

abstract class AbstractLimitOrdersCancelService(protected val genericLimitOrderService: GenericLimitOrderService,
                                                private val cancellerFactory: LimitOrdersCancellerFactory) : AbstractService {

    protected companion object {
        class Orders private constructor(val orders: List<NewLimitOrder>,
                                         val processed: Boolean) {
            companion object Factory{
                fun processed() = Orders(emptyList(), true)
                fun notProcessed(orders: List<NewLimitOrder>) = Orders(orders, false)
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

        cancellerFactory.create(now)
                .preProcess(orders.orders)
                .applyFull(messageWrapper.messageId ?: UUID.randomUUID().toString(), operationType, false)

        writeResponse(messageWrapper, MessageStatus.OK)
    }

    protected abstract fun getOrders(messageWrapper: MessageWrapper): Orders
}