package com.lykke.matching.engine.config.spring.rabbit

import com.lykke.matching.engine.outgoing.messages.v2.events.Event
import com.lykke.matching.engine.outgoing.rabbit.impl.dispatchers.RabbitEventDispatcher
import com.lykke.matching.engine.outgoing.rabbit.utils.RabbitEventUtils
import com.lykke.matching.engine.utils.config.Config
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.ApplicationContext
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.DependsOn
import java.util.concurrent.BlockingDeque
import java.util.concurrent.BlockingQueue

@Configuration
@DependsOn("dynamicRabbitMqQueueConfig")
open class RabbitConfig {

    @Autowired
    private lateinit var config: Config

    @Autowired
    private lateinit var applicationContext: ApplicationContext

    @Bean
    open fun trustedClientsDispatcher(trustedClientsEventsQueue: BlockingDeque<Event<*>>): RabbitEventDispatcher<Event<*>> {
        return RabbitEventDispatcher("TrustedClientEventsDispatcher", trustedClientsEventsQueue, trustedConsumerNameToQueue())
    }

    @Bean
    open fun clientEventsDispatcher(clientsEventsQueue: BlockingDeque<Event<*>>): RabbitEventDispatcher<Event<*>> {
        return RabbitEventDispatcher("ClientEventsDispatcher", clientsEventsQueue, clientNameToQueue())
    }

    @Bean
    open fun trustedConsumerNameToQueue(): Map<String, BlockingQueue<Event<*>>> {
        val consumerNameToQueue = HashMap<String, BlockingQueue<Event<*>>>()
        config.me.rabbitMqConfigs.trustedClientsEvents.forEachIndexed { index, rabbitConfig ->
            val trustedClientsEventConsumerQueue = RabbitEventUtils.getTrustedClientsEventConsumerQueue(rabbitConfig.exchange, index)
            val queue = applicationContext.getBean(trustedClientsEventConsumerQueue) as BlockingQueue<Event<*>>

            consumerNameToQueue.put(trustedClientsEventConsumerQueue, queue)
        }

        return consumerNameToQueue
    }

    @Bean
    open fun clientNameToQueue(): Map<String, BlockingQueue<Event<*>>> {
        val consumerNameToQueue = HashMap<String, BlockingQueue<Event<*>>>()
        config.me.rabbitMqConfigs.events.forEachIndexed { index, rabbitConfig ->
            val clientsEventConsumerQueue = RabbitEventUtils.getClientEventConsumerQueueName(rabbitConfig.exchange, index)

            val queue = applicationContext.getBean(clientsEventConsumerQueue) as BlockingQueue<Event<*>>
            consumerNameToQueue.put(clientsEventConsumerQueue, queue)
        }

        return consumerNameToQueue
    }
}

