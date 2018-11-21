package com.lykke.matching.engine.outgoing.rabbit.impl.listeners

import com.lykke.utils.logging.MetricsLogger
import org.apache.log4j.Logger

private val LOGGER = Logger.getLogger("RabbitMqPublisher")
private val METRICS_LOGGER = MetricsLogger.getLogger()

fun logRecover(name: String) {
    val message = "Rabbig mq publisher: $name, recovered"
    LOGGER.warn(message)
    METRICS_LOGGER.logWarning(message)
}

fun logFail(name: String) {
    val message = "Rabbit Mq publisher: $name failed"
    LOGGER.error(message)
    METRICS_LOGGER.logError(message)
}