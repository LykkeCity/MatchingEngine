package com.lykke.matching.engine.outgoing.rabbit

import com.lykke.matching.engine.logging.DatabaseLogger
import com.lykke.matching.engine.utils.config.RabbitConfig
import com.rabbitmq.client.BuiltinExchangeType
import java.util.concurrent.BlockingQueue

interface RabbitMqService<T> {
    fun startPublisher(config: RabbitConfig,
                       queue: BlockingQueue<out T>,
                       appName: String,
                       appVersion: String,
                       exchangeType: BuiltinExchangeType,
                       messageDatabaseLogger: DatabaseLogger<T>? = null)
}