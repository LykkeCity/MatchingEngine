package com.lykke.matching.engine.notification

import com.lykke.matching.engine.daos.TradeInfo
import org.springframework.beans.factory.annotation.Autowired
import java.util.concurrent.BlockingQueue

class TradeInfoListener: AbstractQueueWrapper<TradeInfo>() {

    @Autowired
    private lateinit var tradeInfoQueue: BlockingQueue<TradeInfo>

    override fun getProcessingQueue(): BlockingQueue<*> {
        return tradeInfoQueue
    }
}