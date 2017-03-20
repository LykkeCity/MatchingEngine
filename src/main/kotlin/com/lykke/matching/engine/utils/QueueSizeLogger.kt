package com.lykke.matching.engine.utils

import com.lykke.matching.engine.logging.MetricsLogger
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.outgoing.messages.JsonSerializable
import com.lykke.matching.engine.outgoing.messages.OrderBook
import org.apache.log4j.Logger
import java.util.Queue

class QueueSizeLogger(
        val queue: Queue<MessageWrapper>,
        val orderBookQueue: Queue<OrderBook>,
        val rabbitOrderBookQueue: Queue<JsonSerializable>,
        val queueSizeLimit: Int) {
    companion object {
        val LOGGER = Logger.getLogger(QueueSizeLogger::class.java.name)
        val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    fun log() {
        LOGGER.info("Incoming queue: ${queue.size}. " +
                "Remote logger: ${MetricsLogger.LINE_QUEUE.size + MetricsLogger.KEY_VALUE_QUEUE.size}. " +
                "Order Book queue: ${orderBookQueue.size}. " +
                "Rabbit Order Book queue ${rabbitOrderBookQueue.size}")

        if (queue.size > queueSizeLimit) {
            METRICS_LOGGER.logError(this.javaClass.name, "Internal queue size ${queue.size} is higher than limit $queueSizeLimit")
        }

        if (MetricsLogger.LINE_QUEUE.size + MetricsLogger.KEY_VALUE_QUEUE.size > queueSizeLimit) {
            METRICS_LOGGER.logError(this.javaClass.name, "Metrics logger queue size ${MetricsLogger.LINE_QUEUE.size + MetricsLogger.KEY_VALUE_QUEUE.size} is higher than limit $queueSizeLimit")
        }

        if (orderBookQueue.size > queueSizeLimit) {
            METRICS_LOGGER.logError(this.javaClass.name, "Order book queue size ${orderBookQueue.size} is higher than limit $queueSizeLimit")
        }

        if (rabbitOrderBookQueue.size > queueSizeLimit) {
            METRICS_LOGGER.logError(this.javaClass.name, "Rabbit order book size queue size ${rabbitOrderBookQueue.size} is higher than limit $queueSizeLimit")
        }
    }
}