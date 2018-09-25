package com.lykke.matching.engine.database.common

import com.lykke.matching.engine.database.PersistenceManager
import com.lykke.matching.engine.database.redis.connection.RedisConnection
import java.util.*

@FunctionalInterface
interface PersistenceManagerFactory {
    fun get(redisConnection: Optional<RedisConnection>): PersistenceManager
}