package com.lykke.matching.engine.notification

import com.lykke.matching.engine.daos.TradeInfo
import com.lykke.matching.engine.outgoing.rabbit.events.TradeInfoEvent
import org.springframework.context.event.EventListener

class TradeInfoListener: AbstractEventListener<TradeInfoEvent, TradeInfo>() {
    override fun extract(t: TradeInfoEvent): TradeInfo {
        return t.tradeInfo
    }

    @EventListener
    override fun process(event: TradeInfoEvent) {
        super.process(event)
    }
}