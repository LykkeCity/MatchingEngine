package com.lykke.matching.engine.notification

import com.lykke.matching.engine.outgoing.messages.LimitOrdersReport
import com.lykke.matching.engine.outgoing.rabbit.events.LimitOrdersReportEvent
import org.springframework.context.event.EventListener

class TestClientLimitOrderListener: AbstractEventListener<LimitOrdersReportEvent, LimitOrdersReport>() {
    override fun extract(t: LimitOrdersReportEvent): LimitOrdersReport {
        return t.limitOrdersReport
    }

    @EventListener
    override fun process(event: LimitOrdersReportEvent) {
        super.process(event)
    }
}