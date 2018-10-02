package com.lykke.matching.engine.database.redis.accessor.impl

import com.lykke.matching.engine.database.ReadOnlyMessageSequenceNumberDatabaseAccessor
import com.lykke.matching.engine.database.redis.connection.RedisConnection
import redis.clients.jedis.Transaction

class RedisMessageSequenceNumberDatabaseAccessor(private val redisConnection: RedisConnection,
                                                 private val dbIndex: Int) : ReadOnlyMessageSequenceNumberDatabaseAccessor {
    companion object {
        private const val KEY = "MessageSequenceNumber"
    }

    override fun getSequenceNumber(): Long {
        var result = 0L

        redisConnection.resource { jedis ->
            jedis.select(dbIndex)
            result = jedis[KEY]?.toLong() ?: 0
        }

        return result
    }

    fun save(transaction: Transaction, sequenceNumber: Long) {
        transaction.select(dbIndex)
        transaction.set(KEY, sequenceNumber.toString())
    }

}