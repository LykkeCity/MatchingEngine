package com.lykke.matching.engine.notification

import org.springframework.context.event.EventListener
import java.util.concurrent.atomic.AtomicInteger

class BalanceUpdateHandlerTest {
    private var counter: AtomicInteger = AtomicInteger(0)

    @EventListener
    fun processBalanceUpdate(notification: BalanceUpdateNotification) {
        counter.incrementAndGet()
    }

    fun getNumberOfNotifications(): Int {
        return counter.get()
    }

    fun clear() {
        counter.set(0)
    }
}