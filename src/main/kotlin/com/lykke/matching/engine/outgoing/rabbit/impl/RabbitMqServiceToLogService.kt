package com.lykke.matching.engine.outgoing.rabbit.impl

import com.lykke.matching.engine.logging.MessageDatabaseLogger
import com.lykke.matching.engine.outgoing.messages.JsonSerializable
import com.lykke.matching.engine.outgoing.rabbit.RabbitMqService
import com.lykke.matching.engine.utils.config.RabbitConfig
import org.apache.log4j.Logger
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Service
import java.util.concurrent.BlockingQueue
import java.util.concurrent.Executors

@Service
@Profile("local")
class RabbitMqServiceToLogService : RabbitMqService {
    companion object{
        private val LOGGER = Logger.getLogger(RabbitMqServiceToLogService::class.java)
    }

    override fun startPublisher(config: RabbitConfig,
                                queue: BlockingQueue<JsonSerializable>,
                                appName: String,
                                appVersion: String,
                                messageDatabaseLogger: MessageDatabaseLogger?) {
        val executor = Executors.newSingleThreadExecutor()
        executor.submit({
            while(true) {
                logMessage(config.uri, queue.take())
            }})
    }

    private fun logMessage(uri: String, item: JsonSerializable) {
        LOGGER.info("new item $item in queue $uri")
    }
}