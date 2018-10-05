package com.lykke.matching.engine.config.spring

import com.lykke.matching.engine.outgoing.messages.v2.events.Event
import com.lykke.matching.engine.outgoing.rabbit.utils.RabbitEventUtils
import com.lykke.matching.engine.utils.config.Config
import org.springframework.context.annotation.AnnotationConfigApplicationContext
import org.springframework.context.annotation.Configuration
import java.util.concurrent.LinkedBlockingQueue
import javax.annotation.PostConstruct

@Configuration("dynamicRabbitMqQueueConfig")
open class DynamicRabbitMqQueueConfig {
    private lateinit var config: Config

    private lateinit var applicationContext: AnnotationConfigApplicationContext

    @PostConstruct
    private fun init() {
        registerClientEventsQueue()
        registerTrustedClientsEventsQueue()
    }

    private fun registerClientEventsQueue() {
        config.me.rabbitMqConfigs.events.forEachIndexed { index, eventConfig ->
            applicationContext.beanFactory.registerSingleton(RabbitEventUtils.getClientEventConsumerQueueName(eventConfig.exchange, index), LinkedBlockingQueue<Event<*>>())
        }
    }

    private fun registerTrustedClientsEventsQueue() {
        config.me.rabbitMqConfigs.trustedClientsEvents.forEachIndexed { index, eventConfig ->
            applicationContext.beanFactory.registerSingleton(RabbitEventUtils.getTrustedClientsEventConsumerQueue(eventConfig.exchange, index), LinkedBlockingQueue<Event<*>>())
        }
    }
}