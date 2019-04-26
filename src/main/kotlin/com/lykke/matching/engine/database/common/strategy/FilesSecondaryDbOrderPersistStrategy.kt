package com.lykke.matching.engine.database.common.strategy

import com.lykke.matching.engine.common.SimpleApplicationEventPublisher
import com.lykke.matching.engine.database.common.entity.OrderBooksPersistenceData
import com.lykke.matching.engine.database.reconciliation.events.OrderBookPersistEvent
import com.lykke.matching.engine.database.reconciliation.events.StopOrderBookPersistEvent

class FilesSecondaryDbOrderPersistStrategy(private val persistedOrdersApplicationEventPublisher: SimpleApplicationEventPublisher<OrderBookPersistEvent>,
                                           private val persistedStopApplicationEventPublisher: SimpleApplicationEventPublisher<StopOrderBookPersistEvent>): OrdersPersistInSecondaryDbStrategy {
    override fun persistOrders(orderBooksData: OrderBooksPersistenceData?,
                      stopOrderBooksData: OrderBooksPersistenceData?) {
        if (orderBooksData?.isEmpty() == false) {
            persistedOrdersApplicationEventPublisher.publishEvent(OrderBookPersistEvent(orderBooksData.orderBooks))
        }

        if (stopOrderBooksData?.isEmpty() == false) {
            persistedStopApplicationEventPublisher.publishEvent(StopOrderBookPersistEvent(stopOrderBooksData.orderBooks))
        }
    }
}