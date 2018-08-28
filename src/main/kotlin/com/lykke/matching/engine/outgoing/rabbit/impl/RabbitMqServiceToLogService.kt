package com.lykke.matching.engine.outgoing.rabbit.impl

import com.lykke.matching.engine.logging.LogMessageTransformer
import com.lykke.matching.engine.outgoing.messages.v2.events.Event
import org.apache.log4j.Logger
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Service

@Service("rabbitMqService")
@Profile("local")
class RabbitMqServiceToLogService(jsonMessageTransformer: LogMessageTransformer) : AbstractRabbitMQToLogService<Event<*>>(jsonMessageTransformer, LOGGER) {
    companion object {
        private val LOGGER = Logger.getLogger(RabbitMqServiceToLogService::class.java)
    }
}