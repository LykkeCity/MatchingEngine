package com.lykke.matching.engine.outgoing.rabbit.impl.services

import com.google.gson.Gson
import com.lykke.matching.engine.logging.DatabaseLogger
import com.lykke.matching.engine.outgoing.rabbit.RabbitMqService
import com.lykke.matching.engine.utils.config.RabbitConfig
import com.rabbitmq.client.BuiltinExchangeType
import org.apache.log4j.Logger
import java.util.concurrent.BlockingQueue
import java.util.concurrent.Executors

abstract class AbstractRabbitMQToLogService<T>(private val gson: Gson, private val LOGGER: Logger): RabbitMqService<T> {
    override fun startPublisher(config: RabbitConfig,
                                publisherName: String,
                                queue: BlockingQueue<out T>,
                                appName: String,
                                appVersion: String,
                                exchangeType: BuiltinExchangeType,
                                messageDatabaseLogger: DatabaseLogger<T>?) {
        val executor = Executors.newSingleThreadExecutor()
        executor.submit {
            while (true) {
                logMessage(config.exchange, publisherName, queue.take())
            }
        }
    }

    private fun logMessage(exchange: String, publisherName: String, item: T) {
        LOGGER.info("New rmq message (exchange: $exchange, publisher: $publisherName): ${gson.toJson(item)}")
    }
}