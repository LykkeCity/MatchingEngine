package com.lykke.matching.engine.notification

import com.lykke.matching.engine.outgoing.messages.JsonSerializable
import com.lykke.matching.engine.outgoing.messages.OrderBook
import org.springframework.beans.factory.annotation.Autowired
import java.util.concurrent.BlockingQueue

class TestRabbitOrderBookListener : AbstractQueueWrapper<OrderBook>() {
    @Autowired
    private lateinit var rabbitOrderBookQueue: BlockingQueue<JsonSerializable>

    override fun getProcessingQueue(): BlockingQueue<*> {
        return rabbitOrderBookQueue
    }

}