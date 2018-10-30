package com.lykke.matching.engine.database

import com.lykke.matching.engine.daos.MidPrice
import redis.clients.jedis.Transaction

interface MidPriceDatabaseAccessor {
    fun removeAll(transaction: Transaction)
    fun save(transaction: Transaction, midPrices: List<MidPrice>)
}