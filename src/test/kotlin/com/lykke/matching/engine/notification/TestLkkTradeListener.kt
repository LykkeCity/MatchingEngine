package com.lykke.matching.engine.notification

import com.lykke.matching.engine.daos.LkkTrade
import org.springframework.beans.factory.annotation.Autowired
import java.util.concurrent.BlockingQueue

class TestLkkTradeListener : AbstractQueueWrapper<List<LkkTrade>>() {
    @Autowired
    private lateinit var lkkTradesQueue: BlockingQueue<List<LkkTrade>>

    override fun getProcessingQueue(): BlockingQueue<*> {
        return lkkTradesQueue
    }
}