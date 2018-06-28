package com.lykke.matching.engine.notification

import com.lykke.matching.engine.outgoing.messages.ReservedCashOperation
import com.lykke.matching.engine.outgoing.rabbit.events.ReservedCashOperationEvent
import org.springframework.context.event.EventListener
import org.springframework.stereotype.Component

@Component
class TestReservedCashOperationListener: AbstractEventListener<ReservedCashOperationEvent, ReservedCashOperation>() {
    override fun extract(t: ReservedCashOperationEvent): ReservedCashOperation {
        return t.reservedCashOperation
    }

    @EventListener
    override fun process(event: ReservedCashOperationEvent) {
        super.process(event)
    }
}