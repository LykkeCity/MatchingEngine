package com.lykke.matching.engine.database.redis.accessor.impl

import com.lykke.matching.engine.daos.MidPrice
import com.lykke.matching.engine.database.ReadOnlyMidPriceDatabaseAccessor
import com.lykke.matching.engine.database.redis.connection.RedisConnection
import org.apache.juli.logging.LogFactory
import org.nustaq.serialization.FSTConfiguration
import org.springframework.util.CollectionUtils
import java.util.*
import java.util.stream.Collectors
import kotlin.math.sign

class RedisReadOnlyMidPriceDatabaseAccessor(private val redisConnection: RedisConnection,
                                            private val dbIndex: Int) : ReadOnlyMidPriceDatabaseAccessor {

    private companion object {
        val PREFIX = "MID_PRICE"
        val DELIMITER = ":"
        val LOGGER = LogFactory.getLog(RedisReadOnlyMidPriceDatabaseAccessor::class.java)
    }

    private val conf = FSTConfiguration.createDefaultConfiguration()

    override fun all(): Map<String, List<MidPrice>> {
        val assetPairIdToMidPrices = HashMap<String, List<MidPrice>>()
        redisConnection.resource { jedis ->
            jedis.select(dbIndex)
            val midPriceKeys = jedis.keys(PREFIX).toList()

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

    private fun getAssetPairId(key: String): String? {
        val keyParts = key.split(DELIMITER)

        if (keyParts.size != 3) {
            LOGGER.warn("Can not parse mid price key: $key")
            return null
        }

        return keyParts[1]
    }
}