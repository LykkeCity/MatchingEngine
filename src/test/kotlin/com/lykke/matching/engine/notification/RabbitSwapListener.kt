package com.lykke.matching.engine.notification

import com.lykke.matching.engine.outgoing.messages.MarketOrderWithTrades
import org.springframework.beans.factory.annotation.Autowired
import java.util.concurrent.BlockingQueue

class RabbitSwapListener: AbstractQueueWrapper<MarketOrderWithTrades>() {
    @Autowired
    private lateinit var rabbitSwapQueue: BlockingQueue<MarketOrderWithTrades>

    override fun getProcessingQueue(): BlockingQueue<MarketOrderWithTrades> {
        return rabbitSwapQueue
    }
}