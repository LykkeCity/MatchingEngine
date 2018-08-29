package com.lykke.matching.engine.outgoing.rabbit.impl

import com.google.gson.Gson
import com.lykke.matching.engine.logging.DatabaseLogger
import com.lykke.matching.engine.outgoing.rabbit.RabbitMqService
import com.lykke.matching.engine.utils.config.RabbitConfig
import com.rabbitmq.client.BuiltinExchangeType
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Service
import java.util.concurrent.BlockingQueue

@Service("rabbitMqOldService")
@Profile("default")
@Deprecated("consider to use new message format")
class RabbitMqOldServiceImpl(private val gson: Gson) : RabbitMqService<Any> {
    override fun startPublisher(config: RabbitConfig,
                                queue: BlockingQueue<out Any>,
                                appName: String,
                                appVersion: String,
                                exchangeType: BuiltinExchangeType,
                                messageDatabaseLogger: DatabaseLogger<Any>?) {
        RabbitMqOldFormatPublisher(config.uri, config.exchange, queue, appName, appVersion, exchangeType,
                gson, messageDatabaseLogger).start()
    }
}