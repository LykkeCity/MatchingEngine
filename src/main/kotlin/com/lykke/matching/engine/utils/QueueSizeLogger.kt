package com.lykke.matching.engine.utils

import com.lykke.matching.engine.database.PersistenceManager
import com.lykke.matching.engine.incoming.MessageRouter
import com.lykke.matching.engine.outgoing.rabbit.impl.listeners.OrderBookListener
import com.lykke.matching.engine.outgoing.socket.ConnectionsHolder
import com.lykke.matching.engine.utils.config.Config
import com.lykke.utils.logging.MetricsLogger
import org.apache.log4j.Logger
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Component
import javax.annotation.PostConstruct
import kotlin.concurrent.fixedRateTimer

@Component
@Profile("default", "!local_config")
class QueueSizeLogger @Autowired constructor(
        val messageRouter: MessageRouter,
        val connectionHandler: ConnectionsHolder,
        val rabbitOrderBookListener: OrderBookListener,
        val persistenceManager: PersistenceManager,
        val config: Config) {

    companion object {
        val LOGGER = Logger.getLogger(QueueSizeLogger::class.java.name)
        val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    @PostConstruct
    fun start() {
        fixedRateTimer(name = "QueueSizeLogger", initialDelay = config.me.queueSizeLoggerInterval, period = config.me.queueSizeLoggerInterval) {
            log()
        }
    }

    private fun log() {
        val balancesQueueSize = persistenceManager.balancesQueueSize()
        LOGGER.info("Incoming queue: ${messageRouter.preProcessedMessageQueue.size}. " +
                "Order Book queue: ${connectionHandler.getOrderBookQueueSize()}. " +
                "Rabbit Order Book queue ${rabbitOrderBookListener.getOrderBookQueueSize()}. " +
                "Balances queue $balancesQueueSize.")

        if (messageRouter.preProcessedMessageQueue.size > config.me.queueSizeLimit) {
            METRICS_LOGGER.logError("Internal queue size is higher than limit")
        }

        if (connectionHandler.getOrderBookQueueSize() > config.me.queueSizeLimit) {
            METRICS_LOGGER.logError("Order book queue size is higher than limit")
        }

        if (rabbitOrderBookListener.getOrderBookQueueSize() > config.me.queueSizeLimit) {
            METRICS_LOGGER.logError("Rabbit order book size queue size is higher than limit")
        }

        if (balancesQueueSize > config.me.queueSizeLimit) {
            METRICS_LOGGER.logError("Balances queue size is higher than limit")
        }
    }
}