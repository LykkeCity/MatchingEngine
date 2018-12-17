package com.lykke.matching.engine.outgoing.rabbit.impl.listeners

import com.lykke.matching.engine.database.azure.AzureMessageLogDatabaseAccessor
import com.lykke.matching.engine.logging.DatabaseLogger
import com.lykke.matching.engine.outgoing.messages.CashTransferOperation
import com.lykke.matching.engine.outgoing.rabbit.RabbitMqService
import com.lykke.matching.engine.outgoing.rabbit.events.RabbitFailureEvent
import com.lykke.matching.engine.outgoing.rabbit.events.RabbitReadyEvent
import com.lykke.matching.engine.utils.config.Config
import com.lykke.matching.engine.utils.monitoring.HealthMonitorEvent
import com.lykke.matching.engine.utils.monitoring.MonitoredComponent
import com.lykke.utils.AppVersion
import com.rabbitmq.client.BuiltinExchangeType
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.ApplicationEventPublisher
import org.springframework.context.event.EventListener
import org.springframework.stereotype.Component
import java.util.concurrent.BlockingDeque
import javax.annotation.PostConstruct

@Component
class CashTransferEventListener {

    @Volatile
    private var failed = false

    @Autowired
    private lateinit var rabbitTransferQueue: BlockingDeque<CashTransferOperation>

    @Autowired
    private lateinit var rabbitMqOldService: RabbitMqService<Any>

    @Autowired
    private lateinit var config: Config

    @Autowired
    private lateinit var applicationEventPublisher: ApplicationEventPublisher

    @Value("\${azure.logs.blob.container}")
    private lateinit var logBlobName: String

    @Value("\${azure.logs.transfers.events.table}")
    private lateinit var logTable: String

    @PostConstruct
    fun initRabbitMqPublisher() {
        rabbitMqOldService.startPublisher(config.me.rabbitMqConfigs.transfers,
                CashTransferEventListener::class.java.simpleName,
                rabbitTransferQueue,
                config.me.name,
                AppVersion.VERSION,
                BuiltinExchangeType.FANOUT,
                DatabaseLogger(
                        AzureMessageLogDatabaseAccessor(config.me.db.messageLogConnString,
                                logTable, logBlobName)))
    }

    @EventListener
    fun onFailure(rabbitFailureEvent: RabbitFailureEvent<*>) {
        if(rabbitFailureEvent.publisherName == CashTransferEventListener::class.java.simpleName) {
            failed = true
            logRmqFail(rabbitFailureEvent.publisherName)
            rabbitFailureEvent.failedEvent?.let {
                rabbitTransferQueue.putFirst(it as CashTransferOperation)
            }
            applicationEventPublisher.publishEvent(HealthMonitorEvent(false, MonitoredComponent.RABBIT, rabbitFailureEvent.publisherName))
        }
    }

    @EventListener
    fun onReady(rabbitReadyEvent: RabbitReadyEvent) {
        if (rabbitReadyEvent.publisherName == CashTransferEventListener::class.java.simpleName && failed) {
            failed = false
            logRmqRecover(rabbitReadyEvent.publisherName)
            applicationEventPublisher.publishEvent(HealthMonitorEvent(true, MonitoredComponent.RABBIT, rabbitReadyEvent.publisherName))
        }
    }
}