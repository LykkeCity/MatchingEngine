package com.lykke.matching.engine.database.redis.accessor.impl

import com.lykke.matching.engine.database.ReadOnlyProcessedMessagesDatabaseAccessor
import com.lykke.matching.engine.database.redis.utils.SetUtils
import com.lykke.matching.engine.deduplication.ProcessedMessage
import com.lykke.matching.engine.deduplication.ProcessedMessageUtils
import org.nustaq.serialization.FSTConfiguration
import redis.clients.jedis.JedisPool
import redis.clients.jedis.Transaction
import java.text.SimpleDateFormat
import java.util.stream.Collectors

class RedisProcessedMessagesDatabaseAccessor(private val jedisPool: JedisPool,
                                             private val dbIndex: Int,
                                             private val timeToLive: Int): ReadOnlyProcessedMessagesDatabaseAccessor {
    companion object {
        private val DATE_FORMAT = SimpleDateFormat("yyyyMMddHH")
        private const val PREFIX = "message"
        private const val SEPARATOR = ":"
    }

    private var conf = FSTConfiguration.createJsonConfiguration()

    override fun get(): Set<ProcessedMessage> {
         jedisPool.resource.use { jedis ->
            jedis.select(dbIndex)

            val keys = jedis.keys("$PREFIX$SEPARATOR*")

            return keys
                    .stream()
                    .flatMap { jedis.smembers(it).stream() }
                    .map {conf.asObject(it.toByteArray()) as ProcessedMessage}
                    .collect(Collectors.toSet())
        }
     }

    fun save(transaction: Transaction, message: ProcessedMessage) {
        if (ProcessedMessageUtils.isDeduplicationNotNeeded(message.type)) {
            return
        }

        transaction.select(dbIndex)
        val key = getKey(message)
        SetUtils.performAtomicSaveSetExpire(transaction, key, conf.asJsonString(message), timeToLive)
     }

    private fun getKey(message: ProcessedMessage): String {
        return PREFIX + SEPARATOR + DATE_FORMAT.format(message.timestamp)
    }
}