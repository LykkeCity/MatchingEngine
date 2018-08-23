package com.lykke.matching.engine.outgoing.rabbit.impl

import com.lykke.matching.engine.logging.DatabaseLogger
import com.lykke.matching.engine.logging.LogMessageTransformer
import com.lykke.matching.engine.outgoing.rabbit.RabbitMqService
import com.lykke.matching.engine.utils.config.RabbitConfig
import com.rabbitmq.client.BuiltinExchangeType
import org.apache.log4j.Logger
import java.util.concurrent.BlockingQueue
import java.util.concurrent.Executors

abstract class AbstractRabbitMQToLogService<in T>(private val jsonMessageTransformer: LogMessageTransformer, private val LOGGER: Logger): RabbitMqService<T> {
    override fun startPublisher(config: RabbitConfig,
                                queue: BlockingQueue<out T>,
                                appName: String,
                                appVersion: String,
                                exchangeType: BuiltinExchangeType,
                                messageDatabaseLogger: DatabaseLogger?) {
        val executor = Executors.newSingleThreadExecutor()
        executor.submit({
            while (true) {
                logMessage(config.exchange, queue.take())
            }
        })
    }

    private fun logMessage(exchange: String, item: T) {
        LOGGER.info("New rmq message (exchange: $exchange): ${jsonMessageTransformer.transform(item as Any).message}")
    }
}