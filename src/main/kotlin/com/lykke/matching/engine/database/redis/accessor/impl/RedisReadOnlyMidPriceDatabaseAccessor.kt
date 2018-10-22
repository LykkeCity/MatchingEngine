package com.lykke.matching.engine.database.redis.accessor.impl

import com.lykke.matching.engine.daos.MidPrice
import com.lykke.matching.engine.database.ReadOnlyMidPriceDatabaseAccessor
import com.lykke.matching.engine.database.redis.connection.RedisConnection
import org.nustaq.serialization.FSTConfiguration
import org.springframework.util.CollectionUtils
import java.util.*
import kotlin.math.sign

class RedisReadOnlyMidPriceDatabaseAccessor(private val redisConnection: RedisConnection,
                                            private val dbIndex: Int) : ReadOnlyMidPriceDatabaseAccessor {

    private companion object {
        val PREFIX = "midPrice_"
    }

    private val conf = FSTConfiguration.createDefaultConfiguration()

    override fun all(): List<MidPrice> {
        val result = ArrayList<MidPrice>()
        redisConnection.resource { jedis ->
            jedis.select(dbIndex)
            val midPriceKeys = jedis.keys(PREFIX).toList()

            if (CollectionUtils.isEmpty(midPriceKeys)) {
                return@resource
            }

            jedis.mget(*midPriceKeys.map { it.toByteArray() }.toTypedArray())
                    .forEach{midPrice -> result.add(conf.asObject(midPrice) as MidPrice)}
        }

        result.sortWith(Comparator { midPrice1, midPrice2 -> (midPrice1.timestamp - midPrice2.timestamp).sign })

        return result
    }
}