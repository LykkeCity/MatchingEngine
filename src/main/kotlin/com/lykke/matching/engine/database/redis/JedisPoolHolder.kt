package com.lykke.matching.engine.database.redis

import redis.clients.jedis.Jedis
import redis.clients.jedis.JedisPool
import java.io.Closeable

class JedisPoolHolder(private val pool: JedisPool,
                      private val index: Int) : Closeable {

    override fun close() = pool.close()

    fun resource(): Jedis {
        val result = pool.resource
        result.select(index)
        return result
    }
}