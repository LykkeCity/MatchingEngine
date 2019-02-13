package com.lykke.matching.engine.database.redis.accessor.impl

import com.lykke.matching.engine.daos.MidPrice
import com.lykke.matching.engine.database.MidPriceDatabaseAccessor
import com.lykke.matching.engine.database.ReadOnlyMidPriceDatabaseAccessor
import com.lykke.matching.engine.database.redis.connection.RedisConnection
import com.lykke.matching.engine.database.redis.utils.KeyUtils
import org.slf4j.LoggerFactory
import org.springframework.util.CollectionUtils
import redis.clients.jedis.Jedis
import redis.clients.jedis.Transaction
import redis.clients.jedis.exceptions.JedisException
import java.lang.StringBuilder
import java.math.BigDecimal
import java.util.*
import java.util.concurrent.TimeUnit
import kotlin.Comparator
import kotlin.math.sign

class RedisMidPriceDatabaseAccessor(private val dbIndex: Int,
                                    private val midPriceTTL: Long,
                                    private val redisConnection: RedisConnection) : MidPriceDatabaseAccessor,
        ReadOnlyMidPriceDatabaseAccessor {
    private companion object {
        val KEY_PREFIX = "MID_PRICE"
        val DELIMITER = ":"
        val LOGGER = LoggerFactory.getLogger(RedisMidPriceDatabaseAccessor::class.java)
    }

    override fun save(transaction: Transaction, midPrices: Collection<MidPrice>) {
        transaction.select(dbIndex)

        midPrices.forEach { midPrice ->
            transaction.setex(getKey(midPrice.assetPairId, midPrice.timestamp),
                    TimeUnit.MILLISECONDS.toSeconds(midPriceTTL).toInt(), midPrice.midPrice.toString())
        }
    }

    override fun getMidPricesByAssetPairMap(): Map<String, List<MidPrice>> {
        val assetPairIdToMidPrices = HashMap<String, MutableList<MidPrice>>()
        redisConnection.resource { jedis ->
            jedis.select(dbIndex)
            val midPriceKeys = jedis.keys("$KEY_PREFIX*").toList()

            if (CollectionUtils.isEmpty(midPriceKeys)) {
                return@resource
            }

            midPriceKeys.forEach { key ->
                getAssetPairIdAndTimeStamp(key)?.let { assetPairIdToTimestamp ->
                    val midPrice = getMidPrice(jedis, key)
                    midPrice?.let {
                        val assetPair = assetPairIdToTimestamp.first
                        val timestamp = assetPairIdToTimestamp.second
                        val midPrices = assetPairIdToMidPrices.getOrPut(assetPair) { ArrayList() }
                        midPrices.add(MidPrice(assetPair, midPrice, timestamp))
                    }
                }
            }

            assetPairIdToMidPrices.forEach { key, value -> value.sortWith(Comparator { midPrice1, midPrice2 -> (midPrice1.timestamp - midPrice2.timestamp).sign }) }
        }

        return assetPairIdToMidPrices
    }

    override fun removeAll(transaction: Transaction) {
        transaction.select(dbIndex)
        KeyUtils.removeAllKeysByPattern(transaction, "$KEY_PREFIX*")
    }

    private fun getMidPrice(jedis: Jedis, key: String): BigDecimal? {
        try {
            val midPrice = jedis.get(key)
            return midPrice?.let { midPrice.toBigDecimal() }
        } catch (e: JedisException) {
            LOGGER.info("Failed to fetch mid price with key $key")
            return null
        }
    }

    private fun getAssetPairIdAndTimeStamp(key: String): Pair<String, Long>? {
        val keyParts = key.split(DELIMITER)

        if (keyParts.size != 3) {
            LOGGER.warn("Can not parse mid price key: $key")
            return null
        }

        return keyParts[1] to keyParts[2].toLong()
    }

    private fun getKey(assetPairId: String, timestamp: Long): String {
        val result = StringBuilder(KEY_PREFIX)

        return result.append(DELIMITER)
                .append(assetPairId)
                .append(DELIMITER)
                .append(timestamp.toString()).toString()
    }
}