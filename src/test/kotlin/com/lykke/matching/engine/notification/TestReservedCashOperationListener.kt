package com.lykke.matching.engine.notification

import com.lykke.matching.engine.outgoing.messages.JsonSerializable
import com.lykke.matching.engine.outgoing.messages.ReservedCashOperation
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import java.util.concurrent.BlockingQueue

@Component
class TestReservedCashOperationListener: AbstractEventListener<ReservedCashOperation>() {
    @Autowired
    private lateinit var reservedCashOperationQueue: BlockingQueue<JsonSerializable>

    override fun getProcessingQueue(): BlockingQueue<*> {
        return reservedCashOperationQueue
    }
}