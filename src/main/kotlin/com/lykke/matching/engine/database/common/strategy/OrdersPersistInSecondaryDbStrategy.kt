package com.lykke.matching.engine.database.common.strategy

import com.lykke.matching.engine.database.common.entity.OrderBooksPersistenceData

@FunctionalInterface
interface OrdersPersistInSecondaryDbStrategy {
    fun persistOrders(orderBooksData: OrderBooksPersistenceData?,
                      stopOrderBooksData: OrderBooksPersistenceData?)
}