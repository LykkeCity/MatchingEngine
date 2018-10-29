package com.lykke.matching.engine.database.redis.accessor.impl

import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.database.OrderBookDatabaseAccessor
import com.lykke.matching.engine.database.redis.connection.RedisConnection

class RedisOrderBookDatabaseAccessor(redisConnection: RedisConnection, db: Int)
    : AbstractRedisOrderBookDatabaseAccessor(redisConnection, db, KEY_PREFIX_ORDER), OrderBookDatabaseAccessor {

    override fun loadLimitOrders() = loadOrders()

    override fun updateOrderBook(asset: String, isBuy: Boolean, orderBook: Collection<LimitOrder>) {
        // Nothing to do
    }

    companion object {
        private const val KEY_PREFIX_ORDER = "LimitOrder:"
    }
}
