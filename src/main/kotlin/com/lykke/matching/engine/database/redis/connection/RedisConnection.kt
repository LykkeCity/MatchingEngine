package com.lykke.matching.engine.database.redis.connection

import redis.clients.jedis.Jedis
import redis.clients.jedis.Transaction

interface RedisConnection {
    fun resource(unitOfWork: (Jedis) -> Unit)
    fun transactionalResource(unitOfWork: (transaction: Transaction) -> Unit)
    fun reconnect()
}