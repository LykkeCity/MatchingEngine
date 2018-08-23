package com.lykke.matching.engine.outgoing.rabbit.impl

import com.lykke.matching.engine.logging.LogMessageTransformer
import org.apache.log4j.Logger
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Service

@Service("rabbitMqOldService")
@Profile("local")
@Deprecated("consider to use new message format")
class RabbitMqOldServiceToLogService(jsonMessageTransformer: LogMessageTransformer) : AbstractRabbitMQToLogService<Any>(jsonMessageTransformer, LOGGER) {
    companion object {
        private val LOGGER = Logger.getLogger(RabbitMqOldServiceToLogService::class.java)
    }
}