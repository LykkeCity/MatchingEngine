package com.lykke.matching.engine.database.redis.accessor.impl

import com.lykke.matching.engine.database.CashOperationIdDatabaseAccessor
import com.lykke.matching.engine.database.redis.connection.RedisConnection
import com.lykke.matching.engine.deduplication.ProcessedMessage
import com.lykke.matching.engine.messages.MessageType
import com.lykke.utils.logging.ThrottlingLogger
import org.nustaq.serialization.FSTConfiguration
import redis.clients.jedis.Transaction

class RedisCashOperationIdDatabaseAccessor(private val cashInOutOperationRedisConnection: RedisConnection,
                                           private val cashTransferOperationRedisConnection: RedisConnection,
                                           private val dbIndex: Int): CashOperationIdDatabaseAccessor {
    companion object {
        val LOGGER = ThrottlingLogger.getLogger(RedisCashOperationIdDatabaseAccessor::class.java.name)
        private const val SEPARATOR = ":"
    }

    private var conf = FSTConfiguration.createJsonConfiguration()

    override fun isAlreadyProcessed(type: String, id: String): Boolean {
        return when (type) {
            MessageType.CASH_TRANSFER_OPERATION.type.toString() -> isTransferAlreadyProcessed(id)
            MessageType.CASH_IN_OUT_OPERATION.type.toString() -> isCashInOutAlreadyProcessed(id)
            else -> false
        }
    }

    private fun isCashInOutAlreadyProcessed(id: String): Boolean {
        var result = false

        cashInOutOperationRedisConnection.resource {
            jedis -> jedis.select(dbIndex)
            result = jedis.exists(getKey(MessageType.CASH_IN_OUT_OPERATION.type.toString(), id))
        }

        return result
    }

    private fun isTransferAlreadyProcessed(id: String): Boolean {
        var result = false

        cashTransferOperationRedisConnection.resource { jedis ->
            jedis.select(dbIndex)
            result = jedis.exists(getKey(MessageType.CASH_TRANSFER_OPERATION.type.toString(), id))
        }

        return result
    }

    fun save(transaction: Transaction, message: ProcessedMessage) {
        transaction.select(dbIndex)
        transaction.set(getKey(message.type.toString(), message.messageId), conf.asJsonString(message))
    }

    private fun getKey(type: String, id: String): String {
        return type + SEPARATOR + id
    }
}
