package com.lykke.matching.engine.notification

import com.lykke.matching.engine.outgoing.messages.LimitOrdersReport
import org.springframework.beans.factory.annotation.Autowired
import java.util.concurrent.BlockingQueue

class TestClientLimitOrderListener: AbstractQueueWrapper<LimitOrdersReport>() {
    @Autowired
    lateinit var clientLimitOrdersQueue: BlockingQueue<LimitOrdersReport>

    override fun getProcessingQueue(): BlockingQueue<LimitOrdersReport> {
        return clientLimitOrdersQueue
    }

}