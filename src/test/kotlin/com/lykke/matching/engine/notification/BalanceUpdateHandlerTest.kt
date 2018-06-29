package com.lykke.matching.engine.notification

import com.lykke.matching.engine.outgoing.messages.JsonSerializable
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import java.util.concurrent.BlockingQueue

@Component
class BalanceUpdateHandlerTest {

    @Autowired
    lateinit var balanceUpdateQueue: BlockingQueue<JsonSerializable>

    @Autowired
    lateinit var  balanceUpdateNotificationQueue: BlockingQueue<BalanceUpdateNotification>

    fun getCountOfBalanceUpdate(): Int {
        return balanceUpdateQueue.size
    }

    fun getCountOfBalanceUpdateNotifications(): Int {
        return balanceUpdateNotificationQueue.size
    }

    fun clear() {
        balanceUpdateQueue.clear()
        balanceUpdateNotificationQueue.clear()
    }
}