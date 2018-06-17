package com.lykke.matching.engine.database.redis.accessor.impl

import com.lykke.matching.engine.daos.NewLimitOrder
import com.lykke.matching.engine.database.OrderBookDatabaseAccessor
import redis.clients.jedis.JedisPool

class RedisOrderBookDatabaseAccessor(jedisPool: JedisPool, db: Int)
    : AbstractRedisOrderBookDatabaseAccessor(jedisPool, db, KEY_PREFIX_ORDER), OrderBookDatabaseAccessor {

    override fun loadLimitOrders() = loadOrders()

    override fun updateOrderBook(asset: String, isBuy: Boolean, orderBook: Collection<NewLimitOrder>) {
        // Nothing to do
    }

    companion object {
        private const val KEY_PREFIX_ORDER = "LimitOrder:"
    }
}
