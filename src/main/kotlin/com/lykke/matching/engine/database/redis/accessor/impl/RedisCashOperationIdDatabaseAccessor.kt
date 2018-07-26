package com.lykke.matching.engine.database.redis.accessor.impl

import com.lykke.matching.engine.database.CashOperationIdDatabaseAccessor
import com.lykke.matching.engine.database.redis.CashInOutOperationIdRedisHolder
import com.lykke.matching.engine.database.redis.CashTransferOperationIdRedisHolder
import com.lykke.matching.engine.deduplication.ProcessedMessage
import com.lykke.matching.engine.messages.MessageType
import com.lykke.utils.logging.ThrottlingLogger
import org.nustaq.serialization.FSTConfiguration
import redis.clients.jedis.Transaction

class RedisCashOperationIdDatabaseAccessor(private val cashInOutOperationIdRedisHolder: CashInOutOperationIdRedisHolder,
                                           private val cashTransferOperationIdRedisHolder: CashTransferOperationIdRedisHolder,
                                           private val dbIndex: Int): CashOperationIdDatabaseAccessor {
    companion object {
        val LOGGER = ThrottlingLogger.getLogger(RedisCashOperationIdDatabaseAccessor::class.java.name)
        private const val SEPARATOR = ":"
    }

    private var conf = FSTConfiguration.createJsonConfiguration()

    override fun isAlreadyProcessed(type: String, id: String): Boolean {
        return when (type) {
            MessageType.CASH_TRANSFER_OPERATION.name -> isTransferAlreadyProcessed(id)
            MessageType.CASH_IN_OUT_OPERATION.name -> isCashInOutAlreadyProcessed(id)
            else -> false
        }
    }

    private fun isCashInOutAlreadyProcessed(id: String): Boolean {
        try {
            val jedis = cashInOutOperationIdRedisHolder.cashInOutOperationIdRedis()
            jedis.select(dbIndex)
            return jedis.exists(getKey(MessageType.CASH_IN_OUT_OPERATION.name, id))
        } catch (e: Exception) {
            cashInOutOperationIdRedisHolder.fail()
            throw e
        }
    }

    private fun isTransferAlreadyProcessed(id: String): Boolean {
        try {
            val jedis = cashTransferOperationIdRedisHolder.cashTransferOperationIdRedis()
            jedis.select(dbIndex)
            return jedis.exists(getKey(MessageType.CASH_TRANSFER_OPERATION.name, id))
        } catch (e: Exception) {
            cashTransferOperationIdRedisHolder.fail()
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
