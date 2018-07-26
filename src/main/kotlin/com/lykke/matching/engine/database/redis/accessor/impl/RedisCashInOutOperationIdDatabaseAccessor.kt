package com.lykke.matching.engine.database.redis.accessor.impl

import com.lykke.matching.engine.database.CashOperationIdDatabaseAccessor
import com.lykke.matching.engine.database.redis.CashInOutOperationIdRedisHolder
import com.lykke.matching.engine.deduplication.ProcessedMessage
import com.lykke.utils.logging.ThrottlingLogger
import org.nustaq.serialization.FSTConfiguration
import redis.clients.jedis.Transaction

class RedisCashInOutOperationIdDatabaseAccessor(private val redisHolder: CashInOutOperationIdRedisHolder,
                                                private val dbIndex: Int) : CashOperationIdDatabaseAccessor {
    companion object {
        val LOGGER = ThrottlingLogger.getLogger(CashInOutOperationIdRedisHolder::class.java.name)
        private const val SEPARATOR = ":"
    }

    private var conf = FSTConfiguration.createJsonConfiguration()

    override fun isAlreadyProcessed(type: String, id: String): Boolean {
        try {
            val jedis = redisHolder.cashInOutOperationIdRedis()
            jedis.select(dbIndex)
            return jedis.exists(getKey(type, id))
        } catch (e: Exception) {
            redisHolder.fail()
            throw e
        }
    }

    fun save(transaction: Transaction, message: ProcessedMessage) {
        transaction.select(dbIndex)
        transaction.set(getKey(message.type.toString(), message.messageId), conf.asJsonString(message))
    }

    private fun getKey(type: String, id: String): String {
        return type + SEPARATOR + id
    }
}
