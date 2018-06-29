package com.lykke.matching.engine.notification

import com.lykke.matching.engine.outgoing.messages.JsonSerializable
import com.lykke.matching.engine.outgoing.messages.LimitOrdersReport
import org.springframework.beans.factory.annotation.Autowired
import java.util.concurrent.BlockingQueue

class TestClientLimitOrderListener: AbstractEventListener<LimitOrdersReport>() {
    @Autowired
    lateinit var clientLimitOrdersQueue: BlockingQueue<JsonSerializable>

    override fun getProcessingQueue(): BlockingQueue<JsonSerializable> {
        return clientLimitOrdersQueue
    }

}