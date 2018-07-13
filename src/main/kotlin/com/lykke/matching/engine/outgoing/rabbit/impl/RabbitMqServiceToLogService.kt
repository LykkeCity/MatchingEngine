package com.lykke.matching.engine.outgoing.rabbit.impl

import com.lykke.matching.engine.logging.MessageDatabaseLogger
import com.lykke.matching.engine.outgoing.messages.JsonSerializable
import com.lykke.matching.engine.outgoing.messages.v2.OutgoingMessage
import com.lykke.matching.engine.outgoing.rabbit.RabbitMqService
import com.lykke.matching.engine.utils.config.RabbitConfig
import com.rabbitmq.client.BuiltinExchangeType
import org.apache.log4j.Logger
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Service
import java.util.concurrent.BlockingQueue
import java.util.concurrent.Executors

@Service
@Profile("local")
class RabbitMqServiceToLogService : RabbitMqService {
    companion object {
        private val LOGGER = Logger.getLogger(RabbitMqServiceToLogService::class.java)
    }

    override fun startPublisher(config: RabbitConfig,
                                queue: BlockingQueue<out OutgoingMessage>,
                                appName: String,
                                appVersion: String,
                                exchangeType: BuiltinExchangeType,
                                messageDatabaseLogger: MessageDatabaseLogger?) {
        val executor = Executors.newSingleThreadExecutor()
        executor.submit({
            while (true) {
                logMessage(config.exchange, queue.take())
            }
        })
    }

    private fun logMessage(exchange: String, item: OutgoingMessage) {
        val value = if (item is JsonSerializable) item.toJson() else item.toString()
        LOGGER.info("New rmq message (exchange: $exchange): $value")
    }
}