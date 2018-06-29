package com.lykke.matching.engine.notification

import com.lykke.matching.engine.outgoing.messages.JsonSerializable
import com.lykke.matching.engine.outgoing.messages.MarketOrderWithTrades
import org.springframework.beans.factory.annotation.Autowired
import java.util.concurrent.BlockingQueue

class RabbitSwapListener: AbstractEventListener<MarketOrderWithTrades>() {
    @Autowired
    private lateinit var rabbitSwapQueue: BlockingQueue<JsonSerializable>

    override fun getProcessingQueue(): BlockingQueue<JsonSerializable> {
        return rabbitSwapQueue
    }
}