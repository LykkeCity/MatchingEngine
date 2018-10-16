package com.lykke.matching.engine.outgoing.rabbit.impl.listeners

import com.lykke.matching.engine.outgoing.messages.v2.events.Event
import com.lykke.matching.engine.outgoing.messages.v2.events.ExecutionEvent
import com.lykke.matching.engine.outgoing.rabbit.RabbitMqService
import com.lykke.matching.engine.utils.config.Config
import com.lykke.matching.engine.utils.queue.QueueSplitter
import com.lykke.utils.AppVersion
import com.rabbitmq.client.BuiltinExchangeType
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import java.util.concurrent.BlockingQueue
import java.util.concurrent.LinkedBlockingQueue
import javax.annotation.PostConstruct

@Component
class TrustedClientsEventsListener {

    @Autowired
    private lateinit var trustedClientsEventsQueue: BlockingQueue<ExecutionEvent>

    @Autowired
    private lateinit var rabbitMqService: RabbitMqService<Event<*>>

    @Autowired
    private lateinit var config: Config

    @PostConstruct
    fun initRabbitMqPublisher() {
        val rabbitMqQueues = HashSet<BlockingQueue<ExecutionEvent>>()
        config.me.rabbitMqConfigs.trustedClientsEvents.forEach { rabbitConfig ->
            val queue = LinkedBlockingQueue<ExecutionEvent>()
            rabbitMqQueues.add(queue)
            rabbitMqService.startPublisher(rabbitConfig, queue,
                    config.me.name,
                    AppVersion.VERSION,
                    BuiltinExchangeType.DIRECT,
                    null)
        }
        QueueSplitter("TrustedClientEventsSplitter", trustedClientsEventsQueue, rabbitMqQueues).start()
    }
}