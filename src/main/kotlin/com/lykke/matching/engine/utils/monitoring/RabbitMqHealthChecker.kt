package com.lykke.matching.engine.utils.monitoring

import com.lykke.matching.engine.outgoing.rabbit.events.RabbitFailureEvent
import com.lykke.matching.engine.outgoing.rabbit.events.RabbitRecoverEvent
import com.lykke.matching.engine.utils.config.Config
import com.lykke.utils.logging.MetricsLogger
import org.apache.log4j.Logger
import org.springframework.context.ApplicationEventPublisher
import org.springframework.context.event.EventListener
import org.springframework.stereotype.Component
import java.util.*

@Component
class RabbitMqHealthChecker(private val applicationEventPublisher: ApplicationEventPublisher,
                            private val config: Config) {

    private companion object {
        val METRICS_LOGGER = MetricsLogger.getLogger()
        val LOGGER = Logger.getLogger(RabbitMqHealthChecker::class.java)
        const val MIN_LIMIT_RABBIT_MQ_PUBLISHERS_COUNT = 0
        const val RECOVER_RABBIT_MQ_PUBLISHERS_COUNT = 1
    }

    private var exchangeNameToCountOfCrashedRmqPublishers = HashMap<String, Int>()

    @EventListener
    @Synchronized
    private fun onRabbitFailureEvent(rabbitFailureEvent: RabbitFailureEvent) {
        val countOfFailedRabbitPublishers = exchangeNameToCountOfCrashedRmqPublishers.compute(rabbitFailureEvent.exchangeName) { _, value -> if (value != null) value + 1 else 1 }

        val logMessage = "Rabbit MQ publisher for exchange ${rabbitFailureEvent.exchangeName} crashed, " +
                "number of functional MQ publishers for exchange is: ${config.me.rabbitMqConfigs.events.size - countOfFailedRabbitPublishers!!}, " +
                "of ${config.me.rabbitMqConfigs.events.size} possible"

        METRICS_LOGGER.logError(logMessage)
        LOGGER.error(logMessage)

        if (countOfFailedRabbitPublishers <= MIN_LIMIT_RABBIT_MQ_PUBLISHERS_COUNT) {
            applicationEventPublisher.publishEvent(HealthMonitorEvent(false, MonitoredComponent.RABBIT))
        }
    }

    @EventListener
    @Synchronized
    private fun onRabbitRecoverEvent(rabbitRecoverEvent: RabbitRecoverEvent) {
        val countOfFailedRabbitPublishers = exchangeNameToCountOfCrashedRmqPublishers.compute(rabbitRecoverEvent.exchangeName) { _, value -> if (value != null) value - 1 else null }

        METRICS_LOGGER.logError("Rabbit MQ publisher for exchange ${rabbitRecoverEvent.exchangeName} recovered, " +
                "number of functional MQ publishers for exchange is: ${config.me.rabbitMqConfigs.events.size - countOfFailedRabbitPublishers!!}, " +
                "of ${config.me.rabbitMqConfigs.events.size} possible")

        if (countOfFailedRabbitPublishers <= RECOVER_RABBIT_MQ_PUBLISHERS_COUNT) {
            applicationEventPublisher.publishEvent(HealthMonitorEvent(true, MonitoredComponent.RABBIT))
        }
    }
}