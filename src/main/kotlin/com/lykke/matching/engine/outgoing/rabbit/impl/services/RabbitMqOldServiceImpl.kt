package com.lykke.matching.engine.outgoing.rabbit.impl.services

import com.google.gson.Gson
import com.lykke.matching.engine.logging.DatabaseLogger
import com.lykke.matching.engine.outgoing.rabbit.RabbitMqService
import com.lykke.matching.engine.outgoing.rabbit.impl.publishers.RabbitMqOldFormatPublisher
import com.lykke.matching.engine.utils.config.RabbitConfig
import com.rabbitmq.client.BuiltinExchangeType
import org.springframework.context.ApplicationEventPublisher
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Service
import java.util.concurrent.BlockingQueue

@Service("rabbitMqOldService")
@Profile("default")
@Deprecated("consider to use new message format")
class RabbitMqOldServiceImpl(private val gson: Gson,
                             private val applicationEventPublisher: ApplicationEventPublisher) : RabbitMqService<Any> {
    override fun startPublisher(config: RabbitConfig,
                                publisherName: String,
                                queue: BlockingQueue<out Any>,
                                appName: String,
                                appVersion: String,
                                exchangeType: BuiltinExchangeType,
                                messageDatabaseLogger: DatabaseLogger<Any>?) {
        RabbitMqOldFormatPublisher(config.uri, config.exchange, publisherName, queue, appName, appVersion, exchangeType,
                gson, applicationEventPublisher, messageDatabaseLogger).start()
    }
}