package com.lykke.matching.engine.database.listeners

import com.lykke.matching.engine.common.QueueConsumer
import com.lykke.matching.engine.database.OrderBookDatabaseAccessor
import com.lykke.matching.engine.database.common.entity.OrderBookPersistenceData
import org.apache.log4j.Logger
import java.util.concurrent.BlockingQueue
import javax.annotation.PostConstruct
import kotlin.concurrent.thread

class OrderBookPersistListener(private val updatedOrderBooksQueue: BlockingQueue<Collection<OrderBookPersistenceData>>,
                               private val secondaryOrdersAccessor: OrderBookDatabaseAccessor) : QueueConsumer<Collection<OrderBookPersistenceData>> {
    companion object {
        private val LOGGER = Logger.getLogger(OrderBookPersistListener::class.java.name)
    }

    @PostConstruct
    private fun init() {
        thread(name = "${OrderBookPersistListener::class.java.name}.ordersAsyncWriter") {
            while (true) {
                try {
                    val orderBooks = updatedOrderBooksQueue.take()
                    orderBooks.forEach {
                        secondaryOrdersAccessor.updateOrderBook(it.assetPairId, it.isBuy, it.orders)
                    }
                } catch (e: Exception) {
                    LOGGER.error("Unable to save orders async", e)
                }
            }
        }
    }
}