package com.lykke.matching.engine.outgoing.rabbit

import com.lykke.matching.engine.logging.MessageDatabaseLogger
import com.lykke.matching.engine.outgoing.messages.JsonSerializable
import com.lykke.matching.engine.utils.config.RabbitConfig
import java.util.concurrent.BlockingQueue

interface RabbitMqService {
    fun startPublisher(config: RabbitConfig,
                       queue: BlockingQueue<out JsonSerializable>,
                       appName: String,
                       appVersion: String,
                       messageDatabaseLogger: MessageDatabaseLogger?)
}