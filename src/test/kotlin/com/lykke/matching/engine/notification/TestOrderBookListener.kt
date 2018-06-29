package com.lykke.matching.engine.notification

import com.lykke.matching.engine.outgoing.messages.OrderBook
import org.springframework.beans.factory.annotation.Autowired
import java.util.concurrent.BlockingQueue

class TestOrderBookListener : AbstractQueueWrapper<OrderBook>() {
    @Autowired
    private lateinit var orderBookQueue: BlockingQueue<OrderBook>

    override fun getProcessingQueue(): BlockingQueue<*> {
        return orderBookQueue
    }
}