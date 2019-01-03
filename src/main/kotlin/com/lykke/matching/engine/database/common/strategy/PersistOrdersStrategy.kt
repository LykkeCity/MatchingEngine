package com.lykke.matching.engine.database.common.strategy

import com.lykke.matching.engine.database.common.entity.OrderBooksPersistenceData
import redis.clients.jedis.Transaction

interface PersistOrdersStrategy {
    fun persist(transaction: Transaction,
                orderBooksData: OrderBooksPersistenceData?,
                stopOrderBooksData: OrderBooksPersistenceData?)
    fun isRedisTransactionUsed(): Boolean
}