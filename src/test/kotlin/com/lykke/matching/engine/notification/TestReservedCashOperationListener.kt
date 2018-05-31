package com.lykke.matching.engine.notification

import com.lykke.matching.engine.outgoing.rabbit.events.ReservedCashOperationEvent
import org.springframework.context.event.EventListener
import org.springframework.stereotype.Component

@Component
class TestReservedCashOperationListener: AbstractEventListener<ReservedCashOperationEvent>() {

    @EventListener
    override fun process(event: ReservedCashOperationEvent) {
        super.process(event)
    }
}