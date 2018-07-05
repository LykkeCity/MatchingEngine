package com.lykke.matching.engine.outgoing.rabbit

import com.lykke.matching.engine.logging.MessageDatabaseLogger
import com.lykke.matching.engine.outgoing.messages.v2.OutgoingMessage
import com.lykke.matching.engine.utils.config.RabbitConfig
import com.rabbitmq.client.BuiltinExchangeType
import java.util.concurrent.BlockingQueue

interface RabbitMqService {
    fun startPublisher(config: RabbitConfig,
                       queue: BlockingQueue<out OutgoingMessage>,
                       appName: String,
                       appVersion: String,
                       exchangeType: BuiltinExchangeType,
                       messageDatabaseLogger: MessageDatabaseLogger?)
}