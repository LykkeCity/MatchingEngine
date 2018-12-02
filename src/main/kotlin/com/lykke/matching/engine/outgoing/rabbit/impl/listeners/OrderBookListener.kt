package com.lykke.matching.engine.outgoing.rabbit.impl.listeners

import com.lykke.matching.engine.outgoing.messages.OrderBook
import com.lykke.matching.engine.outgoing.rabbit.RabbitMqService
import com.lykke.matching.engine.outgoing.rabbit.events.RabbitFailureEvent
import com.lykke.matching.engine.outgoing.rabbit.events.RabbitRecoverEvent
import com.lykke.matching.engine.utils.config.Config
import com.lykke.matching.engine.utils.monitoring.HealthMonitorEvent
import com.lykke.matching.engine.utils.monitoring.MonitoredComponent
import com.lykke.utils.AppVersion
import com.rabbitmq.client.BuiltinExchangeType
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.ApplicationEventPublisher
import org.springframework.context.event.EventListener
import org.springframework.stereotype.Component
import java.util.concurrent.BlockingDeque
import javax.annotation.PostConstruct

@Component
class OrderBookListener {
    @Autowired
    private lateinit var rabbitOrderBookQueue: BlockingDeque<OrderBook>

    @Autowired
    private lateinit var rabbitMqOldService: RabbitMqService<Any>

    @Autowired
    private lateinit var config: Config

    @Autowired
    private lateinit var applicationEventPublisher: ApplicationEventPublisher

    @PostConstruct
    fun initRabbitMqPublisher() {
        rabbitMqOldService.startPublisher(config.me.rabbitMqConfigs.orderBooks,
                OrderBookListener::class.java.simpleName,
                rabbitOrderBookQueue,
                config.me.name,
                AppVersion.VERSION,
                BuiltinExchangeType.FANOUT)
    }

    @EventListener
    fun onFailure(rabbitFailureEvent: RabbitFailureEvent<*>) {
        if(rabbitFailureEvent.publisherName == OrderBookListener::class.java.simpleName) {
            rabbitFailureEvent.failedEvent?.let {
                rabbitOrderBookQueue.putFirst(it as OrderBook)
            }
            applicationEventPublisher.publishEvent(HealthMonitorEvent(false, MonitoredComponent.RABBIT, rabbitFailureEvent.publisherName))
        }
    }

    @EventListener
    fun onRecover(rabbitRecoverEvent: RabbitRecoverEvent) {
        if (rabbitRecoverEvent.publisherName == OrderBookListener::class.java.simpleName) {
            applicationEventPublisher.publishEvent(HealthMonitorEvent(true, MonitoredComponent.RABBIT, rabbitRecoverEvent.publisherName))
        }
    }
}