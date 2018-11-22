package com.lykke.matching.engine.outgoing.rabbit.impl.listeners

import com.lykke.utils.logging.MetricsLogger
import org.apache.log4j.Logger

private val LOGGER = Logger.getLogger("RabbitMqPublisher")
private val METRICS_LOGGER = MetricsLogger.getLogger()

fun logRmqRecover(name: String) {
    val message = "Rabbig mq publisher: $name, recovered"
    LOGGER.warn(message)
    METRICS_LOGGER.logWarning(message)
}

fun logRmqFail(name: String) {
    val message = "Rabbit mq publisher: $name failed"
    LOGGER.error(message)
    METRICS_LOGGER.logError(message)
}