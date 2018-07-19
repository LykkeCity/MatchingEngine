package com.lykke.matching.engine.database.redis.accessor.impl

import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.database.OrderBookDatabaseAccessor
import com.lykke.matching.engine.database.redis.InitialLoadingRedisHolder

class RedisOrderBookDatabaseAccessor(redisHolder: InitialLoadingRedisHolder, db: Int)
    : AbstractRedisOrderBookDatabaseAccessor(redisHolder, db, KEY_PREFIX_ORDER), OrderBookDatabaseAccessor {

    override fun loadLimitOrders() = loadOrders()

    override fun updateOrderBook(asset: String, isBuy: Boolean, orderBook: Collection<LimitOrder>) {
        // Nothing to do
    }

    companion object {
        private const val KEY_PREFIX_ORDER = "LimitOrder:"
    }
}
