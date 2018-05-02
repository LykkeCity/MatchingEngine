package com.lykke.matching.engine.utils

import com.lykke.matching.engine.database.PersistenceManager
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.outgoing.messages.JsonSerializable
import com.lykke.matching.engine.outgoing.messages.OrderBook
import com.lykke.utils.logging.MetricsLogger
import org.apache.log4j.Logger
import java.util.Queue

class QueueSizeLogger(
        val queue: Queue<MessageWrapper>,
        val orderBookQueue: Queue<OrderBook>,
        val rabbitOrderBookQueue: Queue<JsonSerializable>,
        val persistenceManager: PersistenceManager,
        val queueSizeLimit: Int) {
    companion object {
        val LOGGER = Logger.getLogger(QueueSizeLogger::class.java.name)
        val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    fun log() {
        val balancesQueueSize = persistenceManager.balancesQueueSize()
        LOGGER.info("Incoming queue: ${queue.size}. " +
                "Order Book queue: ${orderBookQueue.size}. " +
                "Rabbit Order Book queue ${rabbitOrderBookQueue.size}. " +
                "Balances queue $balancesQueueSize.")

        if (queue.size > queueSizeLimit) {
            METRICS_LOGGER.logError( "Internal queue size is higher than limit")
        }

        if (orderBookQueue.size > queueSizeLimit) {
            METRICS_LOGGER.logError( "Order book queue size is higher than limit")
        }

        if (rabbitOrderBookQueue.size > queueSizeLimit) {
            METRICS_LOGGER.logError( "Rabbit order book size queue size is higher than limit")
        }

        if (balancesQueueSize > queueSizeLimit) {
            METRICS_LOGGER.logError( "Balances queue size is higher than limit")
        }
    }
}