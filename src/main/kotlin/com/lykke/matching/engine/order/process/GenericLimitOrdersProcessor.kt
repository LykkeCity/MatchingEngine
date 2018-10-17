package com.lykke.matching.engine.order.process

import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.order.LimitOrderType
import com.lykke.matching.engine.order.transaction.ExecutionContext
import org.springframework.stereotype.Component

@Component
class GenericLimitOrdersProcessor(private val limitOrderProcessor: LimitOrderProcessor,
                                  private val stopLimitOrdersProcessor: StopLimitOrderProcessor) {

    fun processOrders(orders: Collection<LimitOrder>, executionContext: ExecutionContext): List<ProcessedOrder> {
        return orders.map { order -> processOrder(order, executionContext) }
    }

    private fun processOrder(order: LimitOrder, executionContext: ExecutionContext): ProcessedOrder {
        return getOrderProcessor(order).processOrder(order, executionContext)
    }

    private fun getOrderProcessor(order: LimitOrder): OrderProcessor<LimitOrder> {
        return when {
            order.type == LimitOrderType.STOP_LIMIT -> stopLimitOrdersProcessor
            else -> limitOrderProcessor
        }
    }
}