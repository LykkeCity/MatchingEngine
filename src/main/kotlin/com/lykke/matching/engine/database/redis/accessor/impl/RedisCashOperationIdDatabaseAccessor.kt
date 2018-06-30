package com.lykke.matching.engine.database.redis.accessor.impl

import com.lykke.matching.engine.database.CashOperationIdDatabaseAccessor
import com.lykke.matching.engine.deduplication.ProcessedMessage
import com.lykke.utils.logging.ThrottlingLogger
import org.nustaq.serialization.FSTConfiguration
import redis.clients.jedis.JedisPool
import redis.clients.jedis.Transaction
import java.text.SimpleDateFormat

class RedisCashOperationIdDatabaseAccessor(private val jedisPool: JedisPool,
                                           private val dbIndex: Int): CashOperationIdDatabaseAccessor {
    companion object {
        val LOGGER = ThrottlingLogger.getLogger(RedisCashOperationIdDatabaseAccessor::class.java.name)
        private val DATE_FORMAT = SimpleDateFormat("yyyyMMddHH")
        private const val PREFIX = "message"
        private const val SEPARATOR = ":"
    }

    private var conf = FSTConfiguration.createJsonConfiguration()

    override fun isAlreadyProcessed(type: String, id: String): Boolean {
        jedisPool.resource.use { jedis ->
            jedis.select(dbIndex)
            return jedis.exists(getKey(type, id))
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