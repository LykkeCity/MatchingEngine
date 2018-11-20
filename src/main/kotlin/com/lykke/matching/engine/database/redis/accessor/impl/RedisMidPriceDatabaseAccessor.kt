package com.lykke.matching.engine.database.redis.accessor.impl

import com.lykke.matching.engine.daos.MidPrice
import com.lykke.matching.engine.database.MidPriceDatabaseAccessor
import com.lykke.matching.engine.database.ReadOnlyMidPriceDatabaseAccessor
import com.lykke.matching.engine.database.redis.connection.RedisConnection
import com.lykke.matching.engine.database.redis.utils.BulkUtils
import com.lykke.matching.engine.database.redis.utils.KeyUtils
import org.apache.log4j.Logger
import org.nustaq.serialization.FSTConfiguration
import org.springframework.util.CollectionUtils
import redis.clients.jedis.Transaction
import java.lang.StringBuilder
import java.util.HashMap
import java.util.concurrent.TimeUnit
import java.util.stream.Collectors
import kotlin.Comparator
import kotlin.math.sign

class RedisMidPriceDatabaseAccessor(private val dbIndex: Int,
                                    private val midPriceTTL: Long,
                                    private val redisConnection: RedisConnection) : MidPriceDatabaseAccessor,
        ReadOnlyMidPriceDatabaseAccessor {
    private companion object {
        val KEY_PREFIX = "MID_PRICE"
        val DELIMITER = ":"
        val LOGGER = Logger.getLogger(RedisMidPriceDatabaseAccessor::class.java)
    }

    private var conf = FSTConfiguration.createDefaultConfiguration()

    override fun save(transaction: Transaction, midPrices: Collection<MidPrice>) {
        transaction.select(dbIndex)

        midPrices.forEach { midPrice ->
            transaction.setex(getKey(midPrice.assetPairId, midPrice.timestamp).toByteArray(),
                    TimeUnit.MILLISECONDS.toSeconds(midPriceTTL).toInt(),
                    conf.asByteArray(midPrice))
        }
    }

    override fun getMidPricesByAssetPairMap(): Map<String, List<MidPrice>> {
        val assetPairIdToMidPrices = HashMap<String, List<MidPrice>>()
        redisConnection.resource { jedis ->
            jedis.select(dbIndex)
            val midPriceKeys = jedis.keys("$KEY_PREFIX*").toList()

            if (CollectionUtils.isEmpty(midPriceKeys)) {
                return@resource
            }

            val assetPairToKeys: Map<String, List<String>> = midPriceKeys
                    .stream()
                    .collect(Collectors.groupingBy { getAssetPairId(it) })

            assetPairToKeys.forEach { assetPairId, keys ->
                val result = jedis.mget(*keys.map { it.toByteArray() }.toTypedArray())
                        .map { midPrice -> conf.asObject(midPrice) as MidPrice }
                        .sortedWith(Comparator { midPrice1, midPrice2 -> (midPrice1.timestamp - midPrice2.timestamp).sign })

                assetPairIdToMidPrices[assetPairId] = result
            }

        }

        return assetPairIdToMidPrices
    }

    override fun removeAll(transaction: Transaction) {
        transaction.select(dbIndex)
        KeyUtils.removeAllKeysByPattern(transaction, "$KEY_PREFIX*")
    }

    private fun getAssetPairId(key: String): String? {
        val keyParts = key.split(DELIMITER)

        if (keyParts.size != 3) {
            LOGGER.warn("Can not parse mid price key: $key")
            return null
        }

        return keyParts[1]
    }

    private fun getKey(assetPairId: String, timestamp: Long): String {
        val result = StringBuilder(KEY_PREFIX)

        return result.append(DELIMITER)
                .append(assetPairId)
                .append(DELIMITER)
                .append(timestamp.toString()).toString()
    }
}