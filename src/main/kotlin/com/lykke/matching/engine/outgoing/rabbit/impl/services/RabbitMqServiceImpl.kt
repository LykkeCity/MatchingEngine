package com.lykke.matching.engine.outgoing.rabbit.impl.services

import com.google.gson.Gson
import com.lykke.matching.engine.logging.DatabaseLogger
import com.lykke.matching.engine.outgoing.messages.v2.events.Event
import com.lykke.matching.engine.outgoing.rabbit.RabbitMqService
import com.lykke.matching.engine.outgoing.rabbit.impl.publishers.RabbitMqPublisher
import com.lykke.matching.engine.utils.config.RabbitConfig
import com.rabbitmq.client.BuiltinExchangeType
import org.springframework.context.ApplicationEventPublisher
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Service
import java.util.concurrent.BlockingQueue

@Service("rabbitMqService")
@Profile("default")
class RabbitMqServiceImpl(private val gson: Gson,
                          private val applicationEventPublisher: ApplicationEventPublisher) : RabbitMqService<Event<*>> {
    override fun startPublisher(config: RabbitConfig, publisherName: String,
                                queue: BlockingQueue<out Event<*>>, appName: String,
                                appVersion: String, exchangeType: BuiltinExchangeType,
                                messageDatabaseLogger: DatabaseLogger<Event<*>>?) {
        RabbitMqPublisher(config.uri, config.exchange, publisherName, queue, appName, appVersion, exchangeType,
                gson, applicationEventPublisher, messageDatabaseLogger).start()
    }
}