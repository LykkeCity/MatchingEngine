package com.lykke.matching.engine.outgoing.rabbit.impl.services

import com.google.gson.Gson
import org.slf4j.LoggerFactory
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Service

@Service("rabbitMqOldService")
@Profile("local")
@Deprecated("consider to use new message format")
class RabbitMqOldServiceToLogService(gson: Gson) : AbstractRabbitMQToLogService<Any>(gson, LOGGER) {
    companion object {
        private val LOGGER = LoggerFactory.getLogger(RabbitMqOldServiceToLogService::class.java)
    }
}