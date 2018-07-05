package com.lykke.matching.engine.outgoing.rabbit.impl

import com.lykke.matching.engine.logging.MessageDatabaseLogger
import com.lykke.matching.engine.outgoing.messages.v2.OutgoingMessage
import com.lykke.matching.engine.outgoing.rabbit.RabbitMqPublisher
import com.lykke.matching.engine.outgoing.rabbit.RabbitMqService
import com.lykke.matching.engine.utils.config.RabbitConfig
import com.rabbitmq.client.BuiltinExchangeType
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Service
import java.util.concurrent.BlockingQueue

@Service
@Profile("default")
class RabbitMqServiceImpl : RabbitMqService {
    override fun startPublisher(config: RabbitConfig,
                                queue: BlockingQueue<out OutgoingMessage>,
                                appName: String,
                                appVersion: String,
                                exchangeType: BuiltinExchangeType,
                                messageDatabaseLogger: MessageDatabaseLogger?) {
        RabbitMqPublisher(config.uri, config.exchange, queue, appName, appVersion, exchangeType, messageDatabaseLogger).start()
    }
}