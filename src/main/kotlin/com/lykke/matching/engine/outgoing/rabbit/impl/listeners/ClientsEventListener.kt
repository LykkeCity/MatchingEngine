package com.lykke.matching.engine.outgoing.rabbit.impl.listeners

import com.lykke.matching.engine.database.azure.AzureMessageLogDatabaseAccessor
import com.lykke.matching.engine.logging.MessageDatabaseLogger
import com.lykke.matching.engine.outgoing.messages.v2.events.Event
import com.lykke.matching.engine.outgoing.rabbit.RabbitMqService
import com.lykke.matching.engine.utils.config.Config
import com.lykke.matching.engine.utils.queue.QueueSplitter
import com.lykke.utils.AppVersion
import com.rabbitmq.client.BuiltinExchangeType
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import java.util.concurrent.BlockingQueue
import java.util.concurrent.LinkedBlockingQueue
import javax.annotation.PostConstruct

@Component
class ClientsEventListener {

    @Autowired
    private lateinit var clientsEventsQueue: BlockingQueue<Event<*>>

    @Autowired
    private lateinit var rabbitMqService: RabbitMqService

    @Autowired
    private lateinit var config: Config

    @Value("\${azure.logs.blob.container}")
    private lateinit var logBlobName: String

    @Value("\${azure.logs.clients.events.table}")
    private lateinit var logTable: String

    @PostConstruct
    fun initRabbitMqPublisher() {
        val rabbitMqQueues = HashSet<BlockingQueue<Event<*>>>()
        config.me.rabbitMqConfigs.events.forEachIndexed { index, rabbitConfig ->
            val queue = LinkedBlockingQueue<Event<*>>()
            rabbitMqQueues.add(queue)
            rabbitMqService.startPublisher(rabbitConfig, queue,
                    config.me.name,
                    AppVersion.VERSION,
                    BuiltinExchangeType.DIRECT,
                    MessageDatabaseLogger(
                            AzureMessageLogDatabaseAccessor(config.me.db.messageLogConnString,
                                    "$logTable$index", "$logBlobName$index")))
        }
        QueueSplitter("ClientEventsSplitter", clientsEventsQueue, rabbitMqQueues).start()
    }
}