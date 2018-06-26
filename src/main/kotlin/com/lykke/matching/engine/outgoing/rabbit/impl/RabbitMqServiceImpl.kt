package com.lykke.matching.engine.outgoing.rabbit.impl

import com.lykke.matching.engine.logging.MessageDatabaseLogger
import com.lykke.matching.engine.outgoing.messages.JsonSerializable
import com.lykke.matching.engine.outgoing.rabbit.RabbitMqPublisher
import com.lykke.matching.engine.outgoing.rabbit.RabbitMqService
import com.lykke.matching.engine.utils.config.RabbitConfig
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Service
import java.util.concurrent.BlockingQueue

@Service
@Profile("default")
class RabbitMqServiceImpl : RabbitMqService {
    override fun startPublisher(config: RabbitConfig,
                                queue: BlockingQueue<JsonSerializable>,
                                appName: String,
                                appVersion: String,
                                messageDatabaseLogger: MessageDatabaseLogger?) {
        RabbitMqPublisher(config.uri, config.exchange, queue, appName, appVersion,  messageDatabaseLogger).start()
    }
}