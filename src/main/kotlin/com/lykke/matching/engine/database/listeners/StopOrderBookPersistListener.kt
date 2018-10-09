package com.lykke.matching.engine.database.listeners

import com.lykke.matching.engine.common.QueueConsumer
import com.lykke.matching.engine.database.StopOrderBookDatabaseAccessor
import com.lykke.matching.engine.database.common.entity.OrderBookPersistenceData
import org.apache.log4j.Logger
import java.util.concurrent.BlockingQueue
import javax.annotation.PostConstruct
import kotlin.concurrent.thread

class StopOrderBookPersistListener(private val updatedStopOrderBooksQueue: BlockingQueue<Collection<OrderBookPersistenceData>>,
                                   private val secondaryStopOrdersAccessor: StopOrderBookDatabaseAccessor): QueueConsumer<Collection<OrderBookPersistenceData>> {

    companion object {
        private val LOGGER = Logger.getLogger(StopOrderBookPersistListener::class.java.name)
    }

    @PostConstruct
    private fun init() {
        thread(name = "${StopOrderBookPersistListener::class.java.name}.stopOrdersAsyncWriter") {
            while (true) {
                try {
                    val orderBooks = updatedStopOrderBooksQueue.take()
                    orderBooks.forEach {
                        secondaryStopOrdersAccessor.updateStopOrderBook(it.assetPairId, it.isBuy, it.orders)
                    }
                } catch (e: Exception) {
                    LOGGER.error("Unable to save stop orders async", e)
                }
            }
        }
    }
}