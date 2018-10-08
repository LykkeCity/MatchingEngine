package com.lykke.matching.engine.config.spring

import com.lykke.matching.engine.outgoing.messages.v2.events.Event
import com.lykke.matching.engine.outgoing.rabbit.utils.RabbitEventUtils
import com.lykke.matching.engine.utils.config.Config
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.ApplicationContext
import org.springframework.context.ConfigurableApplicationContext
import org.springframework.context.annotation.Configuration
import java.util.concurrent.LinkedBlockingQueue
import javax.annotation.PostConstruct

@Configuration("dynamicRabbitMqQueueConfig")
open class DynamicRabbitMqQueueConfig {

    @Autowired
    private lateinit var config: Config

    @Autowired
    private lateinit var applicationContext: ApplicationContext

    @PostConstruct
    private fun init() {
        registerClientEventsQueue()
        registerTrustedClientsEventsQueue()
    }

    private fun registerClientEventsQueue() {
        config.me.rabbitMqConfigs.events.forEachIndexed { index, eventConfig ->
            (applicationContext as ConfigurableApplicationContext)
                    .beanFactory
                    .registerSingleton(RabbitEventUtils.getClientEventConsumerQueueName(eventConfig.exchange, index), LinkedBlockingQueue<Event<*>>())
        }
    }

    private fun registerTrustedClientsEventsQueue() {
        config.me.rabbitMqConfigs.trustedClientsEvents.forEachIndexed { index, eventConfig ->
            (applicationContext as ConfigurableApplicationContext)
                    .beanFactory
                    .registerSingleton(RabbitEventUtils.getTrustedClientsEventConsumerQueue(eventConfig.exchange, index), LinkedBlockingQueue<Event<*>>())
        }
    }
}