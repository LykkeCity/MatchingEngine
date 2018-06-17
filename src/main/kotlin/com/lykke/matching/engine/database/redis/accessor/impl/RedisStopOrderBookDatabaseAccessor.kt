package com.lykke.matching.engine.database.redis.accessor.impl

import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.database.StopOrderBookDatabaseAccessor
import redis.clients.jedis.JedisPool

class RedisStopOrderBookDatabaseAccessor(jedisPool: JedisPool, db: Int)
    : AbstractRedisOrderBookDatabaseAccessor(jedisPool, db, KEY_PREFIX_ORDER, "stop"), StopOrderBookDatabaseAccessor {

    override fun loadStopLimitOrders() = loadOrders()

    override fun updateStopOrderBook(assetPairId: String, isBuy: Boolean, orderBook: Collection<LimitOrder>) {
        // Nothing to do
    }

    companion object {
        private const val KEY_PREFIX_ORDER = "StopLimitOrder:"
    }
}