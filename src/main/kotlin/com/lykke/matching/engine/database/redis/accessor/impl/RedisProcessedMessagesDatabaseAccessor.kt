package com.lykke.matching.engine.database.redis.accessor.impl

import com.lykke.matching.engine.database.file.FileProcessedMessagesDatabaseAccessor
import com.lykke.matching.engine.database.ReadOnlyProcessedMessagesDatabaseAccessor
import com.lykke.matching.engine.deduplication.ProcessedMessage
import com.lykke.utils.logging.MetricsLogger
import com.lykke.utils.logging.ThrottlingLogger
import org.nustaq.serialization.FSTConfiguration
import redis.clients.jedis.JedisPool
import redis.clients.jedis.Transaction
import java.text.SimpleDateFormat
import java.util.stream.Collectors

class RedisProcessedMessagesDatabaseAccessor(private val jedisPool: JedisPool,
                                             private val dbIndex: Int,
                                             private val timeToLive: Int): ReadOnlyProcessedMessagesDatabaseAccessor {
    companion object {
        val LOGGER = ThrottlingLogger.getLogger(FileProcessedMessagesDatabaseAccessor::class.java.name)
        val METRICS_LOGGER = MetricsLogger.getLogger()

        val DATE_FORMAT = SimpleDateFormat("yyyyMMdd")
        val PREFIX = "message"
        val SEPARATOR = ":"
    }

    private var conf = FSTConfiguration.createJsonConfiguration()

    override fun get(): Set<ProcessedMessage> {
        LOGGER.info("Start loading processed messages from redis")
        jedisPool.resource.use { jedis ->
            jedis.select(dbIndex)

            val keys = jedis.keys(PREFIX + SEPARATOR)
            LOGGER.info("Processed messages keys count: $keys.size")

            return keys
                    .stream()
                    .flatMap { jedis.smembers(it).stream() }
                    .map {conf.asObject(it.toByteArray()) as ProcessedMessage}
                    .collect(Collectors.toSet())
        }
     }

    fun save(transaction: Transaction, message: ProcessedMessage) {
        transaction.select(dbIndex)
        val key = getKey(message)

        val firstSave = !transaction.exists(key).get()
        transaction.sadd(key, conf.asJsonString(message))

        if (firstSave) {
            LOGGER.debug("First save of message for key $key, setting expiration time")
            transaction.expire(key, timeToLive)
        }
    }

    fun getKey(message: ProcessedMessage): String {
        return PREFIX + SEPARATOR + DATE_FORMAT.format(message.timestamp)
    }
}