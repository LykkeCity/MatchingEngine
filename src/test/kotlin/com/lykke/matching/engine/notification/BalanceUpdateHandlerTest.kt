package com.lykke.matching.engine.notification

import com.lykke.matching.engine.outgoing.messages.BalanceUpdate
import org.springframework.context.event.EventListener
import org.springframework.stereotype.Component
import java.util.*
import java.util.concurrent.LinkedBlockingQueue

@Component
class BalanceUpdateHandlerTest {
    val balanceUpdateQueue: Queue<BalanceUpdate> = LinkedBlockingQueue()
    val balanceUpdateQueueNotification: Queue<BalanceUpdateNotification> = LinkedBlockingQueue()

    @EventListener
    fun processBalanceUpdate(notification: BalanceUpdate) {
        balanceUpdateQueue.add(notification)
    }

    @EventListener
    fun balanceUpdateNotification(balanceUpdateNotification: BalanceUpdateNotification) {
        balanceUpdateQueueNotification.add(balanceUpdateNotification)
    }

    fun getCountOfBalanceUpdate(): Int {
        return balanceUpdateQueue.size
    }

    fun getCountOfBalanceUpdateNotifications(): Int {
        return balanceUpdateQueueNotification.size
    }

    fun clear() {
        balanceUpdateQueue.clear()
        balanceUpdateQueueNotification.clear()
    }
}