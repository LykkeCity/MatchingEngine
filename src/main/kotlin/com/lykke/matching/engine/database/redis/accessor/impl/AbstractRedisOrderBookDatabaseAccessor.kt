package com.lykke.matching.engine.database.redis.accessor.impl

import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.database.redis.connection.RedisConnection
import com.lykke.utils.logging.MetricsLogger
import org.apache.log4j.Logger
import org.nustaq.serialization.FSTConfiguration
import redis.clients.jedis.Transaction

abstract class AbstractRedisOrderBookDatabaseAccessor(private val redisConnection: RedisConnection,
                                                      private val db: Int,
                                                      private val keyPrefix: String,
                                                      private val logPrefix: String = "") {

    companion object {
        private val LOGGER = Logger.getLogger(AbstractRedisOrderBookDatabaseAccessor::class.java.name)
        private val METRICS_LOGGER = MetricsLogger.getLogger()
        private const val KEY_SEPARATOR = ":"
    }

    private val conf = FSTConfiguration.createDefaultConfiguration()

    protected fun loadOrders(): List<LimitOrder> {
        val result = ArrayList<LimitOrder>()
        redisConnection.resource { jedis ->


            if (jedis.db != db.toLong()) {
                jedis.select(db)
            }
            val keys = jedis.keys("$keyPrefix*").toList()

            val values = if (keys.isNotEmpty())
                jedis.mget(*keys.map { it.toByteArray() }.toTypedArray())
            else emptyList()

            values.forEachIndexed { index, value ->
                val key = keys[index]
                try {
                    if (value == null) {
                        throw Exception("${logPrefix}order doesn't exist, key: $key")
                    }
                    result.add(deserializeOrder(value))
                } catch (e: Exception) {
                    val message = "Unable to load ${logPrefix}order, key: $key"
                    LOGGER.error(message, e)
                    METRICS_LOGGER.logError(message, e)
                }
            }
            LOGGER.info("Loaded ${result.size} ${logPrefix}limit orders from redis db $db")
        }
        return result
    }

    fun updateOrders(transaction: Transaction, ordersToSave: Collection<LimitOrder>, ordersToRemove: Collection<LimitOrder>) {
        if (ordersToRemove.isNotEmpty()) {
            transaction.del(*ordersToRemove.map { orderKey(it).toByteArray() }.toTypedArray())
        }
        ordersToSave.forEach { order ->
            val key = orderKey(order).toByteArray()
            val bytes = serializeOrder(order)
            transaction.set(key, bytes)
        }
    }

    private fun orderBookKeyPrefix(assetPairId: String, isBuy: Boolean) = keyPrefix + assetPairId + KEY_SEPARATOR + isBuy + KEY_SEPARATOR

    private fun orderKey(order: LimitOrder): String {
        return orderBookKeyPrefix(order.assetPairId, order.isBuySide()) + order.id
    }

    private fun deserializeOrder(value: ByteArray): LimitOrder {
        return conf.asObject(value) as LimitOrder
    }

    private fun serializeOrder(order: LimitOrder) = conf.asByteArray(order)

}