package com.lykke.matching.engine.notification

import com.lykke.matching.engine.outgoing.messages.BalanceUpdate
import com.lykke.matching.engine.outgoing.rabbit.events.BalanceUpdateEvent
import org.springframework.context.event.EventListener
import org.springframework.stereotype.Component
import java.util.*
import java.util.concurrent.LinkedBlockingQueue

@Component
class BalanceUpdateHandlerTest {
    val balanceUpdateQueue: Queue<BalanceUpdate> = LinkedBlockingQueue()
    val balanceUpdateQueueNotification: Queue<BalanceUpdateNotification> = LinkedBlockingQueue()

    @EventListener
    fun processBalanceUpdate(event: BalanceUpdateEvent ) {
        balanceUpdateQueue.add(event.balanceUpdate)
    }

    @EventListener
    fun balanceUpdateNotification(event: BalanceUpdateNotificationEvent) {
        balanceUpdateQueueNotification.add(event.balanceUpdateNotification)
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