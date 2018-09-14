package com.lykke.matching.engine.database.redis.connection.impl

import com.lykke.matching.engine.database.Storage
import com.lykke.matching.engine.database.redis.connection.RedisConnection
import com.lykke.matching.engine.database.redis.connection.RedisConnectionFactory
import com.lykke.matching.engine.utils.config.Config
import org.springframework.context.ApplicationEventPublisher
import org.springframework.stereotype.Component

@Component
class RedisConnectionFactoryImpl (private val applicationEventPublisher: ApplicationEventPublisher,
                                  private val config: Config): RedisConnectionFactory {
    override fun getConnection(name: String): RedisConnection? {
        if (config.me.storage != Storage.Redis) {
            return null
        }
        return RedisConnectionImpl(name, config.me.redis, applicationEventPublisher)
    }
}