package com.lykke.matching.engine.notification

import com.lykke.matching.engine.outgoing.messages.LimitOrdersReport
import com.lykke.matching.engine.outgoing.rabbit.events.TrustedLimitOrdersReportEvent
import org.springframework.context.event.EventListener

class TestTrustedClientsLimitOrderListener : AbstractEventListener<TrustedLimitOrdersReportEvent, LimitOrdersReport>() {
    override fun extract(t: TrustedLimitOrdersReportEvent): LimitOrdersReport {
        return t.limitOrdersReport
    }

    @EventListener
    override fun process(event: TrustedLimitOrdersReportEvent) {
        super.process(event)
    }
}