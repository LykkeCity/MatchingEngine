package com.lykke.matching.engine.outgoing.rabbit.impl.listeners

import com.lykke.matching.engine.logging.DatabaseLogger
import com.lykke.matching.engine.outgoing.messages.MarketOrderWithTrades
import com.lykke.matching.engine.outgoing.rabbit.RabbitMqService
import com.lykke.matching.engine.outgoing.rabbit.events.RabbitFailureEvent
import com.lykke.matching.engine.outgoing.rabbit.events.RabbitReadyEvent
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
class MarketOrderWithTradesEventListener {
    @Volatile
    private var failed = false

    @Autowired
    private lateinit var marketOrderWithTrades: BlockingDeque<MarketOrderWithTrades>

    @Autowired
    private lateinit var rabbitMqOldService: RabbitMqService<Any>

    @Autowired
    private lateinit var config: Config

    @Autowired
    private lateinit var applicationEventPublisher: ApplicationEventPublisher

    private lateinit var marketOrderWithTradesDatabaseLogger: DatabaseLogger<Any>

    @PostConstruct
    fun initRabbitMqPublisher() {
        rabbitMqOldService.startPublisher(config.me.rabbitMqConfigs.marketOrders,
                MarketOrderWithTradesEventListener::class.java.simpleName,
                marketOrderWithTrades,
                config.me.name,
                AppVersion.VERSION,
                BuiltinExchangeType.FANOUT,
                marketOrderWithTradesDatabaseLogger)
    }

    @EventListener
    fun onFailure(rabbitFailureEvent: RabbitFailureEvent<*>) {
        if(rabbitFailureEvent.publisherName == MarketOrderWithTradesEventListener::class.java.simpleName) {
            failed = true
            logRmqFail(rabbitFailureEvent.publisherName)
            rabbitFailureEvent.failedEvent?.let {
                marketOrderWithTrades.putFirst(it as MarketOrderWithTrades)
            }
            applicationEventPublisher.publishEvent(HealthMonitorEvent(false, MonitoredComponent.RABBIT, rabbitFailureEvent.publisherName))
        }
    }

    @EventListener
    fun onReady(rabbitReadyEvent: RabbitReadyEvent) {
        if (rabbitReadyEvent.publisherName == MarketOrderWithTradesEventListener::class.java.simpleName && failed) {
            failed = false
            logRmqRecover(rabbitReadyEvent.publisherName)
            applicationEventPublisher.publishEvent(HealthMonitorEvent(true, MonitoredComponent.RABBIT, rabbitReadyEvent.publisherName))
        }
    }
}