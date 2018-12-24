package com.lykke.matching.engine.notification

import com.lykke.matching.engine.outgoing.messages.BalanceUpdate
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import java.util.concurrent.BlockingQueue

@Component
class BalanceUpdateHandlerTest @Autowired constructor(val balanceUpdateQueue: BlockingQueue<BalanceUpdate>) {

    fun getCountOfBalanceUpdate(): Int {
        return balanceUpdateQueue.size
    }

    fun clear() {
        balanceUpdateQueue.clear()
    }
}