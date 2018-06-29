package com.lykke.matching.engine.notification

import com.lykke.matching.engine.outgoing.messages.LimitOrdersReport
import org.springframework.beans.factory.annotation.Autowired
import java.util.concurrent.BlockingQueue

class TestTrustedClientsLimitOrderListener : AbstractQueueWrapper<LimitOrdersReport>() {
    @Autowired
    private lateinit var trustedClientsLimitOrderQueue: BlockingQueue<LimitOrdersReport>

    override fun getProcessingQueue(): BlockingQueue<*> {
        return trustedClientsLimitOrderQueue
    }
}